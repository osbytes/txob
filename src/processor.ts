import { sleep } from "./sleep.js";
import PQueue from "p-queue";
import { throttle } from "throttle-debounce";
import type { StandardSchemaV1 } from "@standard-schema/spec";
import {
  createTelemetryInstruments,
  endTelemetrySpan,
  recordTelemetryCounter,
  recordTelemetryDuration,
  setTelemetrySpanAttributes,
  startTelemetrySpan,
  TxOBTelemetryAttributeKey,
  TxOBTelemetryPollOutcome,
  TxOBTelemetrySpanName,
  type TxOBTelemetry,
  type TxOBTelemetryInstruments,
} from "./telemetry.js";
import {
  defaultBackoff,
  defaultMaxErrors,
  defaultMaxEventConcurrency,
  defaultMaxHandlerConcurrency,
  defaultMaxQueuedEvents,
  defaultPollingIntervalMs,
  defaultWakeupThrottleMs,
  defaultWakeupTimeoutMs,
} from "./processor-defaults.js";
import { processEvent } from "./process-event.js";

export { defaultBackoff };

type TxOBEventHandlerResult = {
  processed_at?: Date;
  unprocessable_at?: Date;
  errors?: { error: unknown; timestamp: Date }[];
};

export type TxOBEventData = Record<string, unknown>;

export type TxOBEvent<TxOBEventType extends string, TData = TxOBEventData> = {
  id: string;
  timestamp: Date;
  type: TxOBEventType;
  data: TData;
  correlation_id: string;
  handler_results: Record<string, TxOBEventHandlerResult>;
  errors: number;
  backoff_until?: Date | null;
  processed_at?: Date;
};

export type TxOBEventDataMap<TxOBEventType extends string> = Record<
  TxOBEventType,
  TxOBEventData
>;

type TxOBEventByType<
  TxOBEventType extends string,
  TEventDataMap extends TxOBEventDataMap<TxOBEventType>,
> = {
  [TType in TxOBEventType]: TxOBEvent<TType, TEventDataMap[TType]>;
}[TxOBEventType];

export type TxOBStandardSchema = StandardSchemaV1<unknown, TxOBEventData>;

export type TxOBEventSchemaMap<TxOBEventType extends string> = Record<
  TxOBEventType,
  TxOBStandardSchema
>;

export type TxOBSchemaOutput<TSchema extends TxOBStandardSchema> =
  TSchema extends StandardSchemaV1<unknown, infer TOutput> ? TOutput : never;

type TxOBEventDataMapFromSchemas<
  TEventSchemas extends TxOBEventSchemaMap<string>,
> = {
  [TType in keyof TEventSchemas & string]: TxOBSchemaOutput<
    TEventSchemas[TType]
  >;
};

type TxOBEventHandlerMapFromSchemas<
  TEventSchemas extends TxOBEventSchemaMap<string>,
> = TxOBEventHandlerMap<
  keyof TEventSchemas & string,
  TxOBEventDataMapFromSchemas<TEventSchemas>
>;

type TxOBEventTypeFromSchemas<
  TEventSchemas extends TxOBEventSchemaMap<string>,
> = keyof TEventSchemas & string;

type TxOBEventHandlerOpts = {
  signal?: AbortSignal;
};

export type TxOBEventHandler<
  TxOBEventType extends string = string,
  TData = TxOBEventData,
> = (
  event: TxOBEvent<TxOBEventType, TData>,
  opts: TxOBEventHandlerOpts,
) => Promise<void>;

export type TxOBEventHandlerMap<
  TxOBEventType extends string,
  TEventDataMap extends TxOBEventDataMap<TxOBEventType> =
    TxOBEventDataMap<TxOBEventType>,
> = {
  [TType in TxOBEventType]: {
    [key: string]: TxOBEventHandler<TType, TEventDataMap[TType]>;
  };
};

export type CreateEventProcessorOptsFromSchemas<
  TEventSchemas extends TxOBEventSchemaMap<string>,
> = Omit<
  Partial<
    TxOBProcessEventsOpts<
      TxOBEventTypeFromSchemas<TEventSchemas>,
      TxOBEventDataMapFromSchemas<TEventSchemas>
    >
  >,
  "signal" | "telemetry"
> & {
  pollingIntervalMs?: number;
  wakeupTimeoutMs?: number;
  wakeupThrottleMs?: number;
  wakeupEmitter?: WakeupEmitter;
  telemetry?: TxOBTelemetry;
  eventSchemas: TEventSchemas;
  client: TxOBProcessorClient<
    TxOBEventTypeFromSchemas<TEventSchemas>,
    TxOBEventDataMapFromSchemas<TEventSchemas>
  >;
  handlerMap: TxOBEventHandlerMapFromSchemas<TEventSchemas>;
};

export type TxOBProcessorClientOpts = {
  signal?: AbortSignal;
  maxErrors: number;
};

export interface WakeupEmitter {
  on(event: "wakeup", listener: () => void): void;
  off(event: "wakeup", listener: () => void): void;
  close(): Promise<void>;
}

export interface TxOBProcessorClient<
  TxOBEventType extends string,
  TEventDataMap extends TxOBEventDataMap<TxOBEventType> =
    TxOBEventDataMap<TxOBEventType>,
> {
  getEventsToProcess(
    opts: TxOBProcessorClientOpts,
  ): Promise<
    Pick<TxOBEventByType<TxOBEventType, TEventDataMap>, "id" | "errors">[]
  >;
  transaction(
    fn: (
      txProcessorClient: TxOBTransactionProcessorClient<
        TxOBEventType,
        TEventDataMap
      >,
    ) => Promise<void>,
  ): Promise<void>;
}

export interface TxOBTransactionProcessorClient<
  TxOBEventType extends string,
  TEventDataMap extends TxOBEventDataMap<TxOBEventType> =
    TxOBEventDataMap<TxOBEventType>,
> {
  getEventByIdForUpdateSkipLocked(
    eventId: TxOBEventByType<TxOBEventType, TEventDataMap>["id"],
    opts: TxOBProcessorClientOpts,
  ): Promise<TxOBEventByType<TxOBEventType, TEventDataMap> | null>;
  updateEvent(
    event: TxOBEventByType<TxOBEventType, TEventDataMap>,
  ): Promise<void>;
  createEvent(
    event: Omit<
      TxOBEventByType<TxOBEventType, TEventDataMap>,
      "processed_at" | "backoff_until"
    >,
  ): Promise<void>;
}

export type TxOBBackoffContext<
  TxOBEventType extends string,
  TEventDataMap extends TxOBEventDataMap<TxOBEventType> =
    TxOBEventDataMap<TxOBEventType>,
> = {
  attempt: number;
  error?: unknown;
  errors: readonly unknown[];
  event: Readonly<TxOBEventByType<TxOBEventType, TEventDataMap>>;
  maxErrors: number;
};

type TxOBProcessEventsOpts<
  TxOBEventType extends string,
  TEventDataMap extends TxOBEventDataMap<TxOBEventType>,
> = {
  maxErrors: number;
  backoff: (context: TxOBBackoffContext<TxOBEventType, TEventDataMap>) => Date;
  signal?: AbortSignal;
  logger?: Logger;
  maxEventConcurrency?: number;
  maxHandlerConcurrency?: number;
  maxQueuedEvents?: number;
  onEventMaxErrorsReached?: (opts: {
    event: Readonly<TxOBEventByType<TxOBEventType, TEventDataMap>>;
    txClient: TxOBTransactionProcessorClient<TxOBEventType, TEventDataMap>;
    signal?: AbortSignal;
  }) => Promise<void>;
  telemetry?: TxOBTelemetryInstruments;
};

export interface Logger {
  debug(message?: unknown, ...optionalParams: unknown[]): void;
  info(message?: unknown, ...optionalParams: unknown[]): void;
  warn(message?: unknown, ...optionalParams: unknown[]): void;
  error(message?: unknown, ...optionalParams: unknown[]): void;
}

export interface TxOBProcessor {
  start(): void;
  stop(opts?: { timeoutMs?: number }): Promise<void>;
}

export class EventProcessor<
  TxOBEventType extends string,
  TEventDataMap extends TxOBEventDataMap<TxOBEventType> =
    TxOBEventDataMap<TxOBEventType>,
> implements TxOBProcessor {
  private client: TxOBProcessorClient<TxOBEventType, TEventDataMap>;
  private handlerMap: TxOBEventHandlerMap<TxOBEventType, TEventDataMap>;
  private opts: Omit<
    TxOBProcessEventsOpts<TxOBEventType, TEventDataMap>,
    "signal"
  > & {
    pollingIntervalMs: number;
    maxQueuedEvents: number;
    wakeupTimeoutMs: number;
    wakeupThrottleMs: number;
  };
  private abortController: AbortController;
  private queue: PQueue;
  private state: "stopped" | "started" | "stopping" = "stopped";
  private wakeupEmitter?: WakeupEmitter;
  private wakeupListener?: () => void;
  private throttledPoll?: ReturnType<typeof throttle>;
  private lastWakeupTime: number = Date.now();
  private isPolling: boolean = false;

  constructor({
    client,
    handlerMap,
    wakeupEmitter,
    telemetry,
    ...opts
  }: Omit<
    Partial<TxOBProcessEventsOpts<TxOBEventType, TEventDataMap>>,
    "signal" | "telemetry"
  > & {
    pollingIntervalMs?: number;
    wakeupTimeoutMs?: number;
    wakeupThrottleMs?: number;
    wakeupEmitter?: WakeupEmitter;
    telemetry?: TxOBTelemetry;
  } & {
    client: TxOBProcessorClient<TxOBEventType, TEventDataMap>;
    handlerMap: TxOBEventHandlerMap<TxOBEventType, TEventDataMap>;
  }) {
    const _opts = {
      pollingIntervalMs: defaultPollingIntervalMs,
      maxErrors: defaultMaxErrors,
      backoff: defaultBackoff,
      maxEventConcurrency: defaultMaxEventConcurrency,
      maxHandlerConcurrency: defaultMaxHandlerConcurrency,
      maxQueuedEvents: defaultMaxQueuedEvents,
      wakeupTimeoutMs: defaultWakeupTimeoutMs,
      wakeupThrottleMs: defaultWakeupThrottleMs,
      telemetry: createTelemetryInstruments(telemetry),
      ...opts,
    };
    this.client = client;
    this.handlerMap = handlerMap;
    this.wakeupEmitter = wakeupEmitter;
    this.opts = _opts;
    this.abortController = new AbortController();
    this.queue = new PQueue({
      concurrency: _opts.maxEventConcurrency,
    });
  }

  start() {
    if (this.state !== "stopped") {
      this.opts.logger?.warn(`cannot start processor from '${this.state}'`);
      return;
    }
    this.state = "started";
    this.opts.logger?.debug("processor started");

    const queuedEventIds: Set<string> = new Set();

    // Poll function that can be called from wakeup signals or polling loop
    const poll = async (): Promise<void> => {
      if (this.abortController.signal.aborted) {
        return;
      }

      // Prevent concurrent polls
      if (this.isPolling) {
        this.opts.logger?.debug("skipping poll - already polling");
        recordTelemetryCounter(
          this.opts.telemetry?.pollCounter,
          this.opts.telemetry,
          {
            [TxOBTelemetryAttributeKey.PollOutcome]:
              TxOBTelemetryPollOutcome.SkippedAlreadyPolling,
          },
        );
        return;
      }

      this.isPolling = true;
      const pollStartedAt = Date.now();
      const pollSpan = startTelemetrySpan(
        this.opts.telemetry,
        TxOBTelemetrySpanName.Poll,
        {
          [TxOBTelemetryAttributeKey.QueueSize]: queuedEventIds.size,
          [TxOBTelemetryAttributeKey.QueueMaxSize]: this.opts.maxQueuedEvents,
        },
      );
      let pollOutcome: TxOBTelemetryPollOutcome =
        TxOBTelemetryPollOutcome.Success;
      let pollError: unknown;
      try {
        // Skip polling if we're at capacity to prevent memory leaks
        if (queuedEventIds.size >= this.opts.maxQueuedEvents) {
          pollOutcome = TxOBTelemetryPollOutcome.SkippedQueueFull;
          this.opts.logger?.debug(
            {
              queuedCount: queuedEventIds.size,
              maxQueuedEvents: this.opts.maxQueuedEvents,
            },
            "skipping poll - queue at capacity",
          );
          return;
        }

        const events = await this.client.getEventsToProcess({
          ...this.opts,
          signal: this.abortController.signal,
        });

        const unqueuedEvents = events.filter(
          (event) => !queuedEventIds.has(event.id),
        );
        setTelemetrySpanAttributes(pollSpan, {
          [TxOBTelemetryAttributeKey.EventsFound]: events.length,
          [TxOBTelemetryAttributeKey.EventsQueued]: unqueuedEvents.length,
          [TxOBTelemetryAttributeKey.QueueSize]: queuedEventIds.size,
        });
        this.opts.logger?.debug(
          `found ${unqueuedEvents.length} events to process`,
        );

        for (const event of unqueuedEvents) {
          queuedEventIds.add(event.id);
          this.queue
            .add(
              async () => {
                try {
                  const { backoffUntil } = await processEvent<
                    TxOBEventType,
                    TEventDataMap
                  >({
                    client: this.client,
                    handlerMap: this.handlerMap,
                    unlockedEvent: event,
                    opts: {
                      ...this.opts,
                      signal: this.abortController.signal,
                    },
                  });

                  // Simulate a local wakeup signal after the backoff period
                  // to reduce latency on backed-off event reprocessing
                  if (backoffUntil) {
                    sleep(
                      backoffUntil.getTime() - Date.now(),
                      this.abortController.signal,
                    )
                      .then(() => {
                        this.throttledPoll?.();
                      })
                      .catch(() => {});
                  }
                } catch (error) {
                  this.opts.logger?.error(
                    {
                      eventId: event.id,
                      error,
                    },
                    "error processing event",
                  );
                } finally {
                  queuedEventIds.delete(event.id);
                }
              },
              { signal: this.abortController.signal },
            )
            .catch(() => {
              // Handle queue.add() rejections (e.g., when aborted)
              // The event processing error is already logged in the task's catch block
              queuedEventIds.delete(event.id);
            });
        }
      } catch (error) {
        pollOutcome = TxOBTelemetryPollOutcome.Error;
        pollError = error;
        this.opts.logger?.error(
          { error },
          "error polling for events, will retry",
        );
        // Continue polling even on error
      } finally {
        recordTelemetryCounter(
          this.opts.telemetry?.pollCounter,
          this.opts.telemetry,
          {
            [TxOBTelemetryAttributeKey.PollOutcome]: pollOutcome,
          },
        );
        recordTelemetryDuration(
          this.opts.telemetry?.pollDuration,
          this.opts.telemetry,
          pollStartedAt,
          {
            [TxOBTelemetryAttributeKey.PollOutcome]: pollOutcome,
          },
        );
        endTelemetrySpan(pollSpan, pollError);
        this.isPolling = false;
      }
    };

    if (this.wakeupEmitter) {
      // Setup wakeup signal listener with combined leading and trailing edge throttle
      // Using throttle-debounce library for robust throttling behavior
      // Leading edge: Poll immediately on the first signal
      // Trailing edge: If signals keep coming, also poll after a delay from the last signal
      // This provides both low latency (leading edge) and ensures we catch events
      // even if signals arrive in a continuous burst (trailing edge)
      const throttleMs = this.opts.wakeupThrottleMs;

      // Create throttled poll function with both leading and trailing edges
      // noLeading: false (default) = execute immediately on first signal (leading edge)
      // noTrailing: false (default) = also execute after delay from last signal (trailing edge)
      // This throttled poll is used by both wakeup signals and fallback polling to prevent conflicts
      this.throttledPoll = throttle(throttleMs, () => {
        if (this.abortController.signal.aborted) {
          return;
        }
        this.lastWakeupTime = Date.now();
        this.opts.logger?.debug("triggering throttled poll");
        void poll();
      });

      this.wakeupListener = () => {
        if (this.abortController.signal.aborted) {
          return;
        }
        this.opts.logger?.debug("received wakeup signal");
        this.throttledPoll?.();
      };

      this.wakeupEmitter.on("wakeup", this.wakeupListener);
      this.opts.logger?.debug("wakeup signal listener registered");

      // Initial poll
      this.throttledPoll?.();

      // Start fallback polling loop
      // This runs at a lower frequency and only polls if we haven't received a wakeup signal recently
      // Uses the same throttled poll function to prevent conflicts with wakeup-triggered polls
      (async () => {
        try {
          do {
            await sleep(
              this.opts.pollingIntervalMs,
              this.abortController.signal,
            ).catch(() => {});

            if (this.abortController.signal.aborted) {
              break;
            }

            // Check if we've received a wakeup signal recently
            const timeSinceLastWakeup = Date.now() - this.lastWakeupTime;
            if (timeSinceLastWakeup >= this.opts.wakeupTimeoutMs) {
              this.opts.logger?.debug(
                {
                  timeSinceLastWakeup,
                  wakeupTimeoutMs: this.opts.wakeupTimeoutMs,
                },
                "fallback poll triggered - no wakeup signal received",
              );
              this.throttledPoll?.();
            } else {
              this.opts.logger?.debug(
                {
                  timeSinceLastWakeup,
                  wakeupTimeoutMs: this.opts.wakeupTimeoutMs,
                },
                "skipping fallback poll - wakeup signal received recently",
              );
            }
          } while (!this.abortController.signal.aborted);
        } catch (error) {
          this.opts.logger?.error({ error }, "fallback polling loop error");
        }
      })();
    } else {
      // Standard polling loop
      (async () => {
        try {
          do {
            await poll();
            await sleep(
              this.opts.pollingIntervalMs,
              this.abortController.signal,
            ).catch(() => {});
          } while (!this.abortController.signal.aborted);
        } catch (error) {
          this.opts.logger?.error({ error }, "polling loop error");
        }
      })();
    }
  }

  async stop(opts?: { timeoutMs?: number }): Promise<void> {
    if (this.state !== "started") {
      this.opts.logger?.warn(`cannot stop processor from '${this.state}'`);
      return;
    }
    this.state = "stopping";
    this.opts.logger?.debug("processor stopping");

    const _stopOpts = {
      timeoutMs: 10000,
      ...opts,
    };

    // Clean up wakeup listener
    if (this.wakeupListener && this.wakeupEmitter) {
      this.wakeupEmitter.off("wakeup", this.wakeupListener);
      this.wakeupListener = undefined;
    }

    // Cancel throttled poll (cleans up any pending timers)
    if (this.throttledPoll) {
      (this.throttledPoll as { cancel?: () => void }).cancel?.();
      this.throttledPoll = undefined;
    }

    this.abortController.abort();
    this.queue.pause();

    let caughtErr: unknown;
    try {
      // Wait for both the queue to be empty and the polling loop to complete
      await Promise.race([
        this.queue.onPendingZero(),
        sleep(_stopOpts.timeoutMs).then(() => {
          throw new Error(`shutdown timeout ${_stopOpts.timeoutMs}ms elapsed`);
        }),
      ]);
    } catch (error) {
      caughtErr = error;
    }

    this.state = "stopped";
    this.opts.logger?.debug("processor stopped");

    if (caughtErr) {
      this.opts.logger?.error({ error: caughtErr }, "shutdown error");
      throw caughtErr;
    }
  }
}

export const createEventProcessor = <
  const TEventSchemas extends TxOBEventSchemaMap<string>,
>(
  opts: CreateEventProcessorOptsFromSchemas<TEventSchemas>,
): EventProcessor<
  TxOBEventTypeFromSchemas<TEventSchemas>,
  TxOBEventDataMapFromSchemas<TEventSchemas>
> => {
  const { eventSchemas: _eventSchemas, ...processorOpts } = opts;
  return new EventProcessor(processorOpts);
};

export const createEventHandlerMap = <
  const TEventSchemas extends TxOBEventSchemaMap<string>,
>(opts: {
  eventSchemas: TEventSchemas;
  handlerMap: TxOBEventHandlerMapFromSchemas<TEventSchemas>;
}): TxOBEventHandlerMapFromSchemas<TEventSchemas> => {
  const { eventSchemas: _eventSchemas, handlerMap } = opts;
  return handlerMap;
};
