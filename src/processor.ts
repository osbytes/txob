import { throttle } from "throttle-debounce";
import {
  ErrorUnprocessableEventHandler,
  TxOBError,
  TxOBTimeoutError,
  TxOBValidationError,
} from "./error.js";
import {
  compileHandlerMap,
  type CompiledHandler,
  type TxOBHandlerDefaults,
  type TxOBHandlerMap,
  type TxOBHandlerTuning,
  type TxOBHandlerTuningMap,
} from "./handlers.js";
import {
  createOutbox,
  validateEventData,
  type TxOBBoundPublish,
  type TxOBPublish,
} from "./outbox.js";
import { sleep } from "./sleep.js";
import {
  deliveryKey,
  type TxOBHandlerSlot,
  type TxOBNewDelivery,
  type TxOBStore,
} from "./store.js";
import {
  createTelemetryInstruments,
  endTelemetrySpan,
  recordTelemetryCounter,
  recordTelemetryDuration,
  setTelemetrySpanAttributes,
  startTelemetrySpan,
  TxOBTelemetryAttributeKey,
  TxOBTelemetryDeliveryOutcome,
  TxOBTelemetryPollOutcome,
  TxOBTelemetrySpanName,
  type TxOBTelemetry,
  type TxOBTelemetryInstruments,
} from "./telemetry.js";
import type {
  Logger,
  TxOBDelivery,
  TxOBEventFromSchemas,
  TxOBEventSchemaMap,
  TxOBSerializedError,
  WakeupEmitter,
} from "./types.js";

export type TxOBDeliveryDeadContext<
  TEventSchemas extends TxOBEventSchemaMap,
  TTx,
> = {
  event: TxOBEventFromSchemas<TEventSchemas>;
  handler: string;
  attempts: number;
  error: unknown;
  /** Publish follow-up events in the same transaction that marks the delivery dead. */
  publish: TxOBBoundPublish<TEventSchemas>;
  /** The store transaction the delivery is being marked dead in. */
  tx: TTx;
};

export type CreateEventProcessorOpts<
  TEventSchemas extends TxOBEventSchemaMap,
  TTx,
  THandlerMap extends TxOBHandlerMap<TEventSchemas> =
    TxOBHandlerMap<TEventSchemas>,
> = {
  store: TxOBStore<TTx>;
  eventSchemas: TEventSchemas;
  handlerMap: THandlerMap;
  /** Defaults applied to every handler that doesn't override them. */
  handlerDefaults?: TxOBHandlerDefaults;
  /** Maximum deliveries running in this processor at once. Default: 20. */
  concurrency?: number;
  /** Lease duration for claimed events and deliveries. Leases are renewed while work is in progress. Default: 30s. */
  leaseMs?: number;
  /** Maximum events fanned out per poll. Default: 100. */
  fanoutBatchSize?: number;
  /** Poll interval. Wakeup signals trigger polls in between. Default: 5s. */
  pollingIntervalMs?: number;
  wakeupEmitter?: WakeupEmitter;
  /** Minimum time between wakeup-triggered polls. Default: 100ms. */
  wakeupThrottleMs?: number;
  logger?: Logger;
  telemetry?: TxOBTelemetry;
  /**
   * Called in the same transaction that marks a delivery dead (max attempts reached,
   * unprocessable error, or invalid event data). If it throws, the delivery is left as is and
   * retried once its lease expires.
   */
  onDeliveryDead?: (
    ctx: TxOBDeliveryDeadContext<TEventSchemas, TTx>,
  ) => Promise<void>;
};

export interface TxOBProcessor {
  start(): void;
  stop(opts?: { timeoutMs?: number }): Promise<void>;
}

/** Runtime settings for `EventProcessor.configure`. */
export type TxOBProcessorTuning<THandlerMap> = {
  /** Max deliveries running in this processor at once. Omit to keep the current value. */
  concurrency?: number;
  /**
   * Overrides on top of the handler definitions. Replaces any previously configured overrides;
   * handlers not listed revert to their definitions. Omit to keep the current overrides.
   */
  handlers?: TxOBHandlerTuningMap<THandlerMap>;
};

export type TxOBEffectiveHandlerConfig = {
  concurrency: number | null;
  perKeyConcurrency: number | null;
  rateLimit: { max: number; durationMs: number } | null;
  maxAttempts: number;
  timeoutMs: number | null;
};

class LeaseLostError extends Error {
  constructor() {
    super("delivery lease lost");
    this.name = "LeaseLostError";
  }
}

class ProcessorStoppingError extends Error {
  constructor() {
    super("processor stopping");
    this.name = "ProcessorStoppingError";
  }
}

type ActiveDelivery = {
  delivery: TxOBDelivery;
  handler: CompiledHandler;
  controller: AbortController;
  leaseLost: boolean;
  /** When the lease runs out unless renewed (ms, local clock). */
  leaseExpiresAt: number;
};

/** One attempt: a delivery under a specific lease. */
const activeKey = (delivery: TxOBDelivery) =>
  `${deliveryKey(delivery)}|${delivery.lease_token}`;

const maxPriority = 2 ** 31 - 1;

const serializeError = (error: unknown): TxOBSerializedError => ({
  message: error instanceof Error ? error.message : String(error),
  name: error instanceof Error ? error.name : undefined,
  timestamp: new Date().toISOString(),
});

export class EventProcessor<
  TEventSchemas extends TxOBEventSchemaMap,
  TTx = unknown,
  THandlerMap extends TxOBHandlerMap<TEventSchemas> =
    TxOBHandlerMap<TEventSchemas>,
> implements TxOBProcessor {
  private readonly store: TxOBStore<TTx>;
  private readonly eventSchemas: TEventSchemas;
  private readonly handlerMap: THandlerMap;
  private readonly handlerDefaults?: TxOBHandlerDefaults;
  private tuning: TxOBHandlerTuningMap<THandlerMap> = {};
  private handlers!: Map<string, CompiledHandler[]>;
  private handlersByKey!: Map<string, CompiledHandler>;
  private readonly publish: TxOBPublish<TEventSchemas, TTx>;
  private readonly telemetry: TxOBTelemetryInstruments;
  private readonly opts: Required<
    Pick<
      CreateEventProcessorOpts<TEventSchemas, TTx>,
      | "concurrency"
      | "leaseMs"
      | "fanoutBatchSize"
      | "pollingIntervalMs"
      | "wakeupThrottleMs"
    >
  > &
    Pick<
      CreateEventProcessorOpts<TEventSchemas, TTx>,
      "logger" | "onDeliveryDead" | "wakeupEmitter"
    >;

  private state: "stopped" | "started" | "stopping" = "stopped";
  /** Aborted on stop: ends polling and signals handlers. */
  private abortController = new AbortController();
  private readonly active = new Map<string, ActiveDelivery>();
  private readonly timers = new Set<NodeJS.Timeout>();
  private heartbeat?: NodeJS.Timeout;
  private throttledTick?: throttle<() => void>;
  private wakeupListener?: () => void;
  private ticking?: Promise<void>;
  private tickAgain = false;
  private backlog = false;
  private drained?: () => void;

  constructor(opts: CreateEventProcessorOpts<TEventSchemas, TTx, THandlerMap>) {
    const {
      store,
      eventSchemas,
      handlerMap,
      handlerDefaults,
      telemetry,
      ...rest
    } = opts;
    this.store = store;
    this.eventSchemas = eventSchemas;
    this.handlerMap = handlerMap;
    this.handlerDefaults = handlerDefaults;
    this.applyHandlers(compileHandlerMap(handlerMap, handlerDefaults));
    this.publish = createOutbox({ eventSchemas, store }).publish;
    this.telemetry = createTelemetryInstruments(telemetry);
    this.opts = {
      concurrency: 20,
      leaseMs: 30_000,
      fanoutBatchSize: 100,
      pollingIntervalMs: 5_000,
      wakeupThrottleMs: 100,
      ...rest,
    };
    if (!Number.isInteger(this.opts.concurrency) || this.opts.concurrency < 1) {
      throw new Error("concurrency must be a positive integer");
    }
    if (!(this.opts.leaseMs > 0)) {
      throw new Error("leaseMs must be a positive number");
    }
  }

  /**
   * Changes how this processor runs without restarting it. Takes effect from the next claim:
   * queued deliveries are not tied to the settings they were created under. Attempts already
   * running keep the settings they started with.
   *
   * Declarative and idempotent: applying the same config again changes nothing. Invalid
   * config throws and leaves the current settings untouched.
   *
   * Applies to this processor only. Limits are enforced across processors by the store using
   * the claiming processor's settings, so apply the same config to every processor (e.g. from
   * shared config or a feature flag) to keep them consistent.
   *
   * @example
   * ```typescript
   * processor.configure({
   *   concurrency: 50,
   *   handlers: {
   *     UserCreated: { createStripeCustomer: { concurrency: 20, rateLimit: { max: 100, durationMs: 1_000 } } },
   *   },
   * });
   * ```
   */
  configure(config: TxOBProcessorTuning<THandlerMap>): void {
    const concurrency = config.concurrency ?? this.opts.concurrency;
    if (!Number.isInteger(concurrency) || concurrency < 1) {
      throw new Error("concurrency must be a positive integer");
    }
    const tuning = config.handlers ?? this.tuning;
    const handlers = compileHandlerMap(
      this.handlerMap,
      this.handlerDefaults,
      tuning as Record<string, Record<string, TxOBHandlerTuning>>,
    );

    this.applyHandlers(handlers);
    this.tuning = tuning;
    this.opts.concurrency = concurrency;
    this.opts.logger?.info(
      { concurrency, handlers: tuning },
      "processor reconfigured",
    );
    // Raised limits should pick up queued work now rather than on the next poll
    this.requestTick();
  }

  /** The settings currently in effect, per handler, after overrides. */
  getConfig(): {
    concurrency: number;
    handlers: Record<string, Record<string, TxOBEffectiveHandlerConfig>>;
  } {
    const handlers: Record<
      string,
      Record<string, TxOBEffectiveHandlerConfig>
    > = {};
    for (const handler of this.handlersByKey.values()) {
      handlers[handler.eventType] ??= {};
      handlers[handler.eventType][handler.name] = {
        concurrency: handler.concurrency ?? null,
        perKeyConcurrency: handler.perKey?.limit ?? null,
        rateLimit: handler.rateLimit ?? null,
        maxAttempts: handler.maxAttempts,
        timeoutMs: handler.timeoutMs ?? null,
      };
    }
    return { concurrency: this.opts.concurrency, handlers };
  }

  private applyHandlers(handlers: Map<string, CompiledHandler[]>): void {
    const byKey = new Map<string, CompiledHandler>();
    for (const list of handlers.values()) {
      for (const handler of list) {
        byKey.set(`${handler.eventType}:${handler.name}`, handler);
      }
    }
    this.handlers = handlers;
    this.handlersByKey = byKey;
  }

  start(): void {
    if (this.state !== "stopped") {
      this.opts.logger?.warn(`cannot start processor from '${this.state}'`);
      return;
    }
    this.state = "started";
    this.abortController = new AbortController();
    const signal = this.abortController.signal;

    this.heartbeat = setInterval(
      () => void this.extendLeases(),
      Math.max(this.opts.leaseMs / 3, 10),
    );

    this.throttledTick = throttle(this.opts.wakeupThrottleMs, () =>
      this.requestTick(),
    );
    if (this.opts.wakeupEmitter) {
      this.wakeupListener = () => this.throttledTick?.();
      this.opts.wakeupEmitter.on("wakeup", this.wakeupListener);
    }

    void (async () => {
      while (!signal.aborted) {
        this.requestTick();
        await sleep(this.opts.pollingIntervalMs, signal).catch(() => {});
      }
    })();

    this.opts.logger?.debug("processor started");
  }

  async stop(opts?: { timeoutMs?: number }): Promise<void> {
    if (this.state !== "started") {
      this.opts.logger?.warn(`cannot stop processor from '${this.state}'`);
      return;
    }
    this.state = "stopping";
    this.opts.logger?.debug("processor stopping");
    const timeoutMs = opts?.timeoutMs ?? 10_000;

    if (this.wakeupListener) {
      this.opts.wakeupEmitter?.off("wakeup", this.wakeupListener);
      this.wakeupListener = undefined;
    }
    this.throttledTick?.cancel();
    this.throttledTick = undefined;
    for (const timer of this.timers) clearTimeout(timer);
    this.timers.clear();

    this.abortController.abort(new ProcessorStoppingError());

    let caughtErr: unknown;
    const drained = (async () => {
      // A poll in flight may still claim deliveries; let it finish (it releases them) first
      await this.ticking;
      if (this.active.size > 0) {
        await new Promise<void>((resolve) => {
          this.drained = resolve;
        });
      }
    })();
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(
        () => reject(new Error(`shutdown timeout ${timeoutMs}ms elapsed`)),
        timeoutMs,
      );
    });
    try {
      await Promise.race([drained, timeout]);
    } catch (error) {
      caughtErr = error;
    } finally {
      clearTimeout(timer);
    }

    clearInterval(this.heartbeat);
    this.heartbeat = undefined;
    this.drained = undefined;
    this.state = "stopped";
    this.opts.logger?.debug("processor stopped");

    if (caughtErr) {
      this.opts.logger?.error({ error: caughtErr }, "shutdown error");
      throw caughtErr;
    }
  }

  /** Runs a tick now, or once the current tick finishes. */
  private requestTick(): void {
    if (this.state !== "started") return;
    if (this.ticking) {
      this.tickAgain = true;
      return;
    }
    this.ticking = this.tick().finally(() => {
      this.ticking = undefined;
      if (this.tickAgain) {
        this.tickAgain = false;
        this.requestTick();
      }
    });
  }

  private async tick(): Promise<void> {
    const signal = this.abortController.signal;
    const startedAt = Date.now();
    const span = startTelemetrySpan(
      this.telemetry,
      TxOBTelemetrySpanName.Poll,
      {
        [TxOBTelemetryAttributeKey.ActiveDeliveries]: this.active.size,
      },
    );
    let outcome: (typeof TxOBTelemetryPollOutcome)[keyof typeof TxOBTelemetryPollOutcome] =
      TxOBTelemetryPollOutcome.Success;
    let error: unknown;
    try {
      let fanoutFull = false;
      try {
        fanoutFull = await this.fanout(signal, span);
      } catch (err) {
        if (signal.aborted) return;
        // Deliveries already fanned out can still run
        outcome = TxOBTelemetryPollOutcome.Error;
        error = err;
        this.opts.logger?.error({ error: err }, "error fanning out events");
      }
      if (signal.aborted) return;
      await this.claimDeliveries(signal, span);
      if (fanoutFull) this.tickAgain = true;
    } catch (err) {
      if (signal.aborted) return;
      outcome = TxOBTelemetryPollOutcome.Error;
      error = err;
      this.opts.logger?.error({ error: err }, "error polling, will retry");
    } finally {
      const attributes = { [TxOBTelemetryAttributeKey.PollOutcome]: outcome };
      recordTelemetryCounter(
        this.telemetry.pollCounter,
        this.telemetry,
        attributes,
      );
      recordTelemetryDuration(
        this.telemetry.pollDuration,
        this.telemetry,
        startedAt,
        attributes,
      );
      endTelemetrySpan(span, error);
    }
  }

  /** Returns true if the batch was full and more events may be waiting. */
  private async fanout(
    signal: AbortSignal,
    span: ReturnType<typeof startTelemetrySpan>,
  ): Promise<boolean> {
    const events = await this.store.claimEventsForFanout({
      types: [...this.handlers.keys()],
      limit: this.opts.fanoutBatchSize,
      leaseMs: this.opts.leaseMs,
      signal,
    });
    if (events.length === 0) return false;

    const eventIds: string[] = [];
    const deliveries: TxOBNewDelivery[] = [];
    for (const event of events) {
      try {
        const eventDeliveries = (this.handlers.get(event.type) ?? []).map(
          (handler): TxOBNewDelivery => {
            const priority = handler.priority(event);
            if (
              !Number.isInteger(priority) ||
              Math.abs(priority) > maxPriority
            ) {
              throw new Error(
                `${event.type}.${handler.name}: priority must be a 32-bit integer, got ${priority}`,
              );
            }
            const key = handler.perKey ? handler.perKey.key(event) : null;
            if (key !== null && typeof key !== "string") {
              throw new Error(
                `${event.type}.${handler.name}: concurrency key must be a string or null, got ${typeof key}`,
              );
            }
            return {
              event_id: event.id,
              event_type: event.type,
              handler: handler.name,
              priority,
              concurrency_key: key,
            };
          },
        );
        eventIds.push(event.id);
        deliveries.push(...eventDeliveries);
      } catch (error) {
        // Left unfanned; it is retried when its lease expires. Other events in the batch
        // are unaffected.
        this.opts.logger?.error(
          {
            eventId: event.id,
            type: event.type,
            fanoutAttempts: event.fanout_attempts,
            error,
          },
          "error computing deliveries for event (invalid priority or concurrency key)",
        );
      }
    }

    if (eventIds.length > 0) {
      await this.store.completeFanout({
        event_ids: eventIds,
        deliveries,
        signal,
      });
      recordTelemetryCounter(
        this.telemetry.fanoutCounter,
        this.telemetry,
        {},
        eventIds.length,
      );
    }
    setTelemetrySpanAttributes(span, {
      [TxOBTelemetryAttributeKey.EventsFannedOut]: eventIds.length,
    });
    this.opts.logger?.debug(
      { events: eventIds.length, deliveries: deliveries.length },
      "fanned out events",
    );
    return events.length >= this.opts.fanoutBatchSize;
  }

  private async claimDeliveries(
    signal: AbortSignal,
    span: ReturnType<typeof startTelemetrySpan>,
  ): Promise<void> {
    const free = this.opts.concurrency - this.active.size;
    if (free <= 0) {
      this.backlog = true;
      return;
    }

    const slots = [...this.handlersByKey.values()].map(
      (handler): TxOBHandlerSlot => ({
        event_type: handler.eventType,
        handler: handler.name,
        limit: free,
        concurrency: handler.concurrency,
        concurrencyPerKey: handler.perKey?.limit,
        rateLimit: handler.rateLimit,
      }),
    );

    const claimedAt = Date.now();
    const deliveries = await this.store.claimDeliveries({
      slots,
      limit: free,
      leaseMs: this.opts.leaseMs,
      signal,
    });
    setTelemetrySpanAttributes(span, {
      [TxOBTelemetryAttributeKey.DeliveriesClaimed]: deliveries.length,
    });
    if (signal.aborted) {
      // Stopped while claiming: hand the deliveries straight back
      await this.store.releaseDeliveries(deliveries);
      return;
    }
    // A full claim means more may be waiting; claim again as capacity frees up
    this.backlog = deliveries.length >= free;

    for (const delivery of deliveries) {
      const handlerKey = `${delivery.event_type}:${delivery.handler}`;
      const handler = this.handlersByKey.get(handlerKey);
      if (!handler) {
        // Store returned a delivery outside of the requested slots
        this.opts.logger?.warn(
          { delivery: deliveryKey(delivery) },
          "unexpected delivery claimed",
        );
        continue;
      }
      // Reclaiming a delivery this processor still runs means the old lease lapsed
      for (const previous of this.active.values()) {
        if (
          deliveryKey(previous.delivery) === deliveryKey(delivery) &&
          !previous.leaseLost
        ) {
          this.loseLease(previous);
        }
      }
      const active: ActiveDelivery = {
        delivery,
        handler,
        controller: new AbortController(),
        leaseLost: false,
        leaseExpiresAt: claimedAt + this.opts.leaseMs,
      };
      this.active.set(activeKey(delivery), active);
      void this.runDelivery(active).finally(() => {
        this.active.delete(activeKey(delivery));
        // Finishing frees capacity, and for a limited handler it can unblock queued work
        const limited =
          handler.concurrency !== undefined || handler.perKey !== undefined;
        if (this.state === "started" && (this.backlog || limited)) {
          this.requestTick();
        }
        if (this.active.size === 0) {
          this.drained?.();
        }
      });
    }
  }

  private async runDelivery(active: ActiveDelivery): Promise<void> {
    const { delivery } = active;
    await this.executeDelivery(active, {
      eventId: delivery.event_id,
      type: delivery.event_type,
      handler: delivery.handler,
      attempt: delivery.attempts,
      correlationId: delivery.event.correlation_id,
    });
  }

  private async executeDelivery(
    active: ActiveDelivery,
    logContext: Record<string, unknown>,
  ): Promise<void> {
    const { delivery, handler } = active;
    const startedAt = Date.now();
    const span = startTelemetrySpan(
      this.telemetry,
      TxOBTelemetrySpanName.DeliveryProcess,
      {
        [TxOBTelemetryAttributeKey.EventId]: delivery.event_id,
        [TxOBTelemetryAttributeKey.EventType]: delivery.event_type,
        [TxOBTelemetryAttributeKey.EventCorrelationId]:
          delivery.event.correlation_id ?? "",
        [TxOBTelemetryAttributeKey.HandlerName]: delivery.handler,
        [TxOBTelemetryAttributeKey.DeliveryAttempt]: delivery.attempts,
      },
    );

    let handlerError: unknown;
    let event: TxOBEventFromSchemas<TEventSchemas> | undefined;
    let timer: NodeJS.Timeout | undefined;
    try {
      if (delivery.attempts > handler.maxAttempts) {
        // Previous attempts lost their lease (e.g. the processor crashed mid-handler)
        throw new ErrorUnprocessableEventHandler(
          new Error(`max attempts (${handler.maxAttempts}) exceeded`),
        );
      }

      // Re-validate so handlers receive the schema's output type (e.g. coerced dates)
      const schema = this.eventSchemas[delivery.event_type];
      try {
        event = {
          ...delivery.event,
          data: await validateEventData(
            delivery.event_type,
            schema,
            delivery.event.data,
          ),
        } as TxOBEventFromSchemas<TEventSchemas>;
      } catch (error) {
        throw error instanceof TxOBValidationError
          ? new ErrorUnprocessableEventHandler(error)
          : error;
      }

      const signals = [this.abortController.signal, active.controller.signal];
      if (handler.timeoutMs !== undefined) {
        // Cancels the handler's context; the handler decides how to wind down and its own
        // result (resolve or throw) determines the outcome
        const timeoutController = new AbortController();
        signals.push(timeoutController.signal);
        const timeoutMs = handler.timeoutMs;
        timer = setTimeout(
          () => timeoutController.abort(new TxOBTimeoutError(timeoutMs)),
          timeoutMs,
        );
      }

      await handler.fn(event, {
        signal: AbortSignal.any(signals),
        attempt: delivery.attempts,
        handler: delivery.handler,
      });
    } catch (error) {
      handlerError = error;
    } finally {
      clearTimeout(timer);
    }

    let outcome: (typeof TxOBTelemetryDeliveryOutcome)[keyof typeof TxOBTelemetryDeliveryOutcome];
    try {
      outcome = await this.finalizeDelivery(
        active,
        event,
        handlerError,
        logContext,
      );
    } catch (error) {
      // The lease will expire and the delivery will be retried
      outcome = TxOBTelemetryDeliveryOutcome.Retry;
      this.opts.logger?.error(
        { ...logContext, error },
        "error recording delivery result",
      );
    }

    this.recordDelivery(delivery, outcome, startedAt);
    setTelemetrySpanAttributes(span, {
      [TxOBTelemetryAttributeKey.DeliveryOutcome]: outcome,
    });
    endTelemetrySpan(span, handlerError);
  }

  private async finalizeDelivery(
    active: ActiveDelivery,
    event: TxOBEventFromSchemas<TEventSchemas> | undefined,
    handlerError: unknown,
    logContext: Record<string, unknown>,
  ): Promise<
    (typeof TxOBTelemetryDeliveryOutcome)[keyof typeof TxOBTelemetryDeliveryOutcome]
  > {
    const { delivery, handler } = active;

    if (active.leaseLost) {
      this.opts.logger?.warn(
        logContext,
        "delivery lease lost; result discarded",
      );
      return TxOBTelemetryDeliveryOutcome.LeaseLost;
    }

    if (handlerError !== undefined && this.abortController.signal.aborted) {
      // Failed while shutting down, most likely because the handler honored the abort signal.
      // Give the delivery back without counting the attempt.
      await this.store.releaseDeliveries([delivery]);
      this.opts.logger?.debug(logContext, "delivery released on shutdown");
      return TxOBTelemetryDeliveryOutcome.Released;
    }

    if (handlerError === undefined) {
      const ok = await this.store.completeDelivery(delivery);
      if (!ok) {
        this.opts.logger?.warn(
          logContext,
          "delivery lease lost before completion",
        );
        return TxOBTelemetryDeliveryOutcome.LeaseLost;
      }
      this.opts.logger?.debug(logContext, "handler succeeded");
      return TxOBTelemetryDeliveryOutcome.Success;
    }

    const dead =
      handlerError instanceof ErrorUnprocessableEventHandler ||
      delivery.attempts >= handler.maxAttempts;
    this.opts.logger?.error(
      { ...logContext, error: handlerError },
      dead ? "handler failed; delivery dead" : "handler failed; will retry",
    );

    if (dead) {
      const onDeliveryDead = this.opts.onDeliveryDead;
      const ok = await this.store.failDelivery(
        delivery,
        { error: serializeError(handlerError), run_after: null },
        {
          within: onDeliveryDead
            ? (tx) =>
                onDeliveryDead({
                  event:
                    event ??
                    (delivery.event as TxOBEventFromSchemas<TEventSchemas>),
                  handler: delivery.handler,
                  attempts: delivery.attempts,
                  error: handlerError,
                  tx,
                  publish: (type, data, opts) =>
                    this.publish(tx, type, data, opts),
                })
            : undefined,
        },
      );
      return ok
        ? TxOBTelemetryDeliveryOutcome.Dead
        : TxOBTelemetryDeliveryOutcome.LeaseLost;
    }

    const backoff = handler.backoff({
      attempt: delivery.attempts,
      error: handlerError,
      event: event ?? delivery.event,
      handler: delivery.handler,
    });
    let runAfter =
      backoff instanceof Date ? backoff : new Date(Date.now() + backoff);
    if (
      handlerError instanceof TxOBError &&
      handlerError.backoffUntil &&
      handlerError.backoffUntil > runAfter
    ) {
      runAfter = handlerError.backoffUntil;
    }

    const ok = await this.store.failDelivery(delivery, {
      error: serializeError(handlerError),
      run_after: runAfter,
    });
    if (ok) this.scheduleTick(runAfter);
    return ok
      ? TxOBTelemetryDeliveryOutcome.Retry
      : TxOBTelemetryDeliveryOutcome.LeaseLost;
  }

  /** Poll when a backed-off delivery becomes due instead of waiting for the next interval. */
  private scheduleTick(at: Date): void {
    if (this.state !== "started") return;
    const timer = setTimeout(
      () => {
        this.timers.delete(timer);
        this.requestTick();
      },
      Math.max(at.getTime() - Date.now(), 0),
    );
    timer.unref?.();
    this.timers.add(timer);
  }

  private loseLease(active: ActiveDelivery): void {
    active.leaseLost = true;
    active.controller.abort(new LeaseLostError());
    this.opts.logger?.warn(
      { eventId: active.delivery.event_id, handler: active.delivery.handler },
      "delivery lease lost",
    );
  }

  private async extendLeases(): Promise<void> {
    // A lease that couldn't be renewed in time may already belong to another processor
    for (const active of this.active.values()) {
      if (!active.leaseLost && Date.now() >= active.leaseExpiresAt) {
        this.loseLease(active);
      }
    }
    const actives = [...this.active.values()].filter((a) => !a.leaseLost);
    if (actives.length === 0) return;
    const renewedAt = Date.now();
    try {
      const lost = await this.store.extendLeases(
        actives.map((a) => a.delivery),
        { leaseMs: this.opts.leaseMs },
      );
      for (const delivery of lost) {
        const active = this.active.get(activeKey(delivery));
        if (active && !active.leaseLost) this.loseLease(active);
      }
      for (const active of actives) {
        active.leaseExpiresAt = renewedAt + this.opts.leaseMs;
      }
    } catch (error) {
      this.opts.logger?.error({ error }, "error extending delivery leases");
    }
  }

  private recordDelivery(
    delivery: TxOBDelivery,
    outcome: (typeof TxOBTelemetryDeliveryOutcome)[keyof typeof TxOBTelemetryDeliveryOutcome],
    startedAt?: number,
  ): void {
    const attributes = {
      [TxOBTelemetryAttributeKey.EventType]: delivery.event_type,
      [TxOBTelemetryAttributeKey.HandlerName]: delivery.handler,
      [TxOBTelemetryAttributeKey.DeliveryOutcome]: outcome,
    };
    recordTelemetryCounter(
      this.telemetry.deliveryCounter,
      this.telemetry,
      attributes,
    );
    if (startedAt !== undefined) {
      recordTelemetryDuration(
        this.telemetry.deliveryDuration,
        this.telemetry,
        startedAt,
        attributes,
      );
    }
  }
}

export const createEventProcessor = <
  const TEventSchemas extends TxOBEventSchemaMap,
  TTx,
  const THandlerMap extends TxOBHandlerMap<TEventSchemas> =
    TxOBHandlerMap<TEventSchemas>,
>(
  opts: CreateEventProcessorOpts<TEventSchemas, TTx, THandlerMap>,
): EventProcessor<TEventSchemas, TTx, THandlerMap> => new EventProcessor(opts);

/**
 * Identity helper that types a handler map against event schemas, for defining handlers
 * separately from the processor.
 */
export const createEventHandlerMap = <
  const TEventSchemas extends TxOBEventSchemaMap,
  const THandlerMap extends TxOBHandlerMap<TEventSchemas>,
>(opts: {
  eventSchemas: TEventSchemas;
  handlerMap: THandlerMap;
}): THandlerMap => opts.handlerMap;
