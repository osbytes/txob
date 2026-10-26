import type {
  TxOBEventFromSchemas,
  TxOBEventSchemaMap,
  TxOBEventTypeOf,
} from "./types.js";

export type TxOBHandlerContext = {
  /** Aborted on processor shutdown, handler timeout, or lost lease. */
  signal: AbortSignal;
  /** 1-based attempt number. */
  attempt: number;
  handler: string;
};

export type TxOBHandlerFn<TEvent> = (
  event: TEvent,
  ctx: TxOBHandlerContext,
) => Promise<void>;

export type TxOBBackoffContext<TEvent> = {
  /** 1-based number of the attempt that just failed. */
  attempt: number;
  error: unknown;
  event: TEvent;
  handler: string;
};

export type TxOBRetryConfig<TEvent> = {
  /** Total attempts before the delivery is marked dead. Default: 5. */
  maxAttempts?: number;
  /** Delay before the next attempt, in ms or as a date. Default: exponential, 1s doubling up to 60s. */
  backoff?: (ctx: TxOBBackoffContext<TEvent>) => number | Date;
};

/**
 * Concurrency limits are enforced by the store across all processors: `limit: 5` means at
 * most 5 deliveries of this handler run at once in total, however many processors run.
 */
export type TxOBConcurrencyConfig<TEvent> = {
  /** Maximum deliveries of this handler running at once across all processors. */
  limit?: number;
  /**
   * Maximum deliveries running at once per key across all processors. The key is computed
   * when the event is fanned out, so deliveries for different keys never block each other.
   */
  perKey?: {
    key: (event: TEvent) => string | null | undefined;
    limit: number;
  };
};

export type TxOBRateLimitConfig = {
  /** At most `max` handler starts per sliding `durationMs` window, across all processors. */
  max: number;
  durationMs: number;
};

export type TxOBHandlerConfig<TEvent> = {
  handler: TxOBHandlerFn<TEvent>;
  /** A number is shorthand for `{ limit }`. */
  concurrency?: number | TxOBConcurrencyConfig<TEvent>;
  rateLimit?: TxOBRateLimitConfig;
  retry?: TxOBRetryConfig<TEvent>;
  /**
   * Abort the handler's `signal` after this long, with a `TxOBTimeoutError` reason. The handler
   * is responsible for honoring it; its own result (resolve or throw) decides the outcome.
   */
  timeoutMs?: number;
  /** Higher runs first. Computed when the event is fanned out. Default: 0. */
  priority?: number | ((event: TEvent) => number);
};

export type TxOBHandlerDefinition<TEvent> =
  | TxOBHandlerFn<TEvent>
  | TxOBHandlerConfig<TEvent>;

export type TxOBHandlerMap<TEventSchemas extends TxOBEventSchemaMap> = {
  [TType in TxOBEventTypeOf<TEventSchemas>]: Record<
    string,
    TxOBHandlerDefinition<TxOBEventFromSchemas<TEventSchemas, TType>>
  >;
};

/**
 * Handler settings that can be changed while the processor runs, via
 * `EventProcessor.configure`. `undefined` keeps the value from the handler definition; `null`
 * removes a limit set there.
 */
export type TxOBHandlerTuning<TEvent = any> = {
  /** Max deliveries of this handler running at once across all processors. */
  concurrency?: number | null;
  /** Max running at once per key. Requires `concurrency.perKey.key` in the handler definition. */
  perKeyConcurrency?: number;
  rateLimit?: TxOBRateLimitConfig | null;
  /** Merged over the handler definition's `retry`. */
  retry?: TxOBRetryConfig<TEvent>;
  timeoutMs?: number | null;
};

type TxOBEventOfDefinition<TDefinition> =
  TDefinition extends TxOBHandlerDefinition<infer TEvent> ? TEvent : never;

/** Tuning keyed by the event types and handler names of a handler map. */
export type TxOBHandlerTuningMap<THandlerMap> = {
  [TType in keyof THandlerMap]?: {
    [THandler in keyof THandlerMap[TType]]?: TxOBHandlerTuning<
      TxOBEventOfDefinition<THandlerMap[TType][THandler]>
    >;
  };
};

export type TxOBHandlerDefaults = {
  retry?: TxOBRetryConfig<any>;
  timeoutMs?: number;
};

export const defaultMaxAttempts = 5;

export const defaultBackoff = ({ attempt }: { attempt: number }): number =>
  Math.min(1000 * 2 ** (attempt - 1), 60_000);

export type CompiledHandler = {
  eventType: string;
  name: string;
  fn: TxOBHandlerFn<any>;
  concurrency?: number;
  perKey?: { key: (event: any) => string | null | undefined; limit: number };
  rateLimit?: TxOBRateLimitConfig;
  maxAttempts: number;
  backoff: (ctx: TxOBBackoffContext<any>) => number | Date;
  timeoutMs?: number;
  priority: (event: any) => number;
};

const assertPositiveInt = (value: number, path: string): void => {
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`${path} must be a positive integer, received ${value}`);
  }
};

const assertPositive = (value: number, path: string): void => {
  if (!(typeof value === "number" && value > 0)) {
    throw new Error(`${path} must be a positive number, received ${value}`);
  }
};

/**
 * Normalizes and validates a handler map. Throws on invalid configuration so mistakes
 * surface at startup rather than while processing.
 */
export const compileHandlerMap = (
  handlerMap: Record<string, Record<string, TxOBHandlerDefinition<any>>>,
  defaults: TxOBHandlerDefaults = {},
  tuning: Record<string, Record<string, TxOBHandlerTuning> | undefined> = {},
): Map<string, CompiledHandler[]> => {
  const compiled = new Map<string, CompiledHandler[]>();

  // Tuning usually comes from runtime config (env, flags, an admin API); catch typos
  for (const [eventType, handlers] of Object.entries(tuning)) {
    for (const name of Object.keys(handlers ?? {})) {
      if (!handlerMap[eventType]?.[name]) {
        throw new Error(`cannot tune unknown handler '${eventType}.${name}'`);
      }
    }
  }

  for (const [eventType, handlers] of Object.entries(handlerMap)) {
    const compiledHandlers: CompiledHandler[] = [];
    for (const [name, definition] of Object.entries(handlers)) {
      const path = `handlerMap.${eventType}.${name}`;
      if (name.includes(":")) {
        throw new Error(`${path}: handler names cannot contain ':'`);
      }
      const config: TxOBHandlerConfig<any> =
        typeof definition === "function" ? { handler: definition } : definition;
      if (typeof config.handler !== "function") {
        throw new Error(`${path}.handler must be a function`);
      }

      const concurrency: TxOBConcurrencyConfig<any> =
        typeof config.concurrency === "number"
          ? { limit: config.concurrency }
          : { ...config.concurrency };
      const tuned = tuning[eventType]?.[name];
      if (tuned?.concurrency !== undefined) {
        concurrency.limit = tuned.concurrency ?? undefined;
      }
      if (tuned?.perKeyConcurrency !== undefined) {
        if (!concurrency.perKey) {
          throw new Error(
            `${path}: perKeyConcurrency requires concurrency.perKey.key in the handler definition`,
          );
        }
        concurrency.perKey = {
          ...concurrency.perKey,
          limit: tuned.perKeyConcurrency,
        };
      }
      const rateLimit =
        tuned?.rateLimit === null
          ? undefined
          : (tuned?.rateLimit ?? config.rateLimit);
      if (concurrency.limit !== undefined) {
        assertPositiveInt(concurrency.limit, `${path}.concurrency.limit`);
      }
      if (concurrency.perKey) {
        assertPositiveInt(
          concurrency.perKey.limit,
          `${path}.concurrency.perKey.limit`,
        );
        if (typeof concurrency.perKey.key !== "function") {
          throw new Error(`${path}.concurrency.perKey.key must be a function`);
        }
      }
      if (rateLimit) {
        assertPositiveInt(rateLimit.max, `${path}.rateLimit.max`);
        assertPositive(rateLimit.durationMs, `${path}.rateLimit.durationMs`);
      }

      const retry = { ...defaults.retry, ...config.retry, ...tuned?.retry };
      const maxAttempts = retry.maxAttempts ?? defaultMaxAttempts;
      assertPositiveInt(maxAttempts, `${path}.retry.maxAttempts`);

      const timeoutMs =
        tuned?.timeoutMs === null
          ? undefined
          : (tuned?.timeoutMs ?? config.timeoutMs ?? defaults.timeoutMs);
      if (timeoutMs !== undefined) {
        assertPositive(timeoutMs, `${path}.timeoutMs`);
      }

      const priority = config.priority ?? 0;

      compiledHandlers.push({
        eventType,
        name,
        fn: config.handler,
        concurrency: concurrency.limit,
        perKey: concurrency.perKey,
        rateLimit,
        maxAttempts,
        backoff: retry.backoff ?? defaultBackoff,
        timeoutMs,
        priority: typeof priority === "function" ? priority : () => priority,
      });
    }
    compiled.set(eventType, compiledHandlers);
  }

  return compiled;
};
