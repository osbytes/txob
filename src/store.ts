import type { TxOBDelivery, TxOBEvent, TxOBSerializedError } from "./types.js";

export type TxOBFanoutEvent = TxOBEvent & {
  fanout_attempts: number;
  fanout_lease_token: string;
};

export type TxOBNewDelivery = {
  event_id: string;
  event_type: string;
  handler: string;
  priority: number;
  concurrency_key: string | null;
};

/**
 * A handler the processor wants deliveries for, with the handler's limits. The store enforces
 * the limits across all processors, counting deliveries with a live lease as running.
 */
export type TxOBHandlerSlot = {
  event_type: string;
  handler: string;
  /** Max deliveries of this handler to return from this claim (the processor's free capacity). */
  limit: number;
  /** Max deliveries of this handler leased at once. */
  concurrency?: number;
  /** Max deliveries leased at once per `concurrency_key`. Deliveries without a key are unlimited. */
  concurrencyPerKey?: number;
  /** Max claims of this handler in any sliding window of `durationMs`. */
  rateLimit?: { max: number; durationMs: number };
};

export type TxOBDeliveryFailure = {
  error: TxOBSerializedError;
  /** When to retry. `null` marks the delivery dead (no further attempts). */
  run_after: Date | null;
};

export type TxOBStoreOpts = {
  signal?: AbortSignal;
};

/**
 * Storage backend for events and deliveries.
 *
 * Claims are lease-based: a claimed record is hidden from other processors until its
 * lease expires. Writes that finalize a claim are fenced by its lease token, so a
 * processor that lost its lease cannot overwrite the work of the processor that took over.
 *
 * `TTx` is the store's transaction/connection handle used by `insertEvent` so events can
 * be written atomically with business data.
 */
export interface TxOBStore<TTx = unknown> {
  /** Insert an event using the caller's transaction. */
  insertEvent(tx: TTx, event: TxOBEvent): Promise<void>;

  /** Lease up to `limit` events of `types` that have not yet been fanned out. */
  claimEventsForFanout(
    opts: TxOBStoreOpts & { types: string[]; limit: number; leaseMs: number },
  ): Promise<TxOBFanoutEvent[]>;

  /**
   * Atomically create deliveries (idempotent per event/handler) and mark the events fanned out.
   */
  completeFanout(
    opts: TxOBStoreOpts & {
      event_ids: string[];
      deliveries: TxOBNewDelivery[];
    },
  ): Promise<void>;

  /**
   * Lease pending deliveries, highest priority first: at most `slot.limit` per handler and
   * `limit` overall, within each slot's cross-processor `concurrency`, `concurrencyPerKey` and
   * `rateLimit`. Concurrent claims from different processors must not exceed those limits.
   */
  claimDeliveries(
    opts: TxOBStoreOpts & {
      slots: TxOBHandlerSlot[];
      limit: number;
      leaseMs: number;
    },
  ): Promise<TxOBDelivery[]>;

  /** Returns `false` if the lease was lost. */
  completeDelivery(
    delivery: TxOBDelivery,
    opts?: TxOBStoreOpts,
  ): Promise<boolean>;

  /**
   * Record a failed attempt. `within` runs in the same transaction as the update, which
   * lets hooks publish follow-up events atomically. Returns `false` if the lease was lost.
   */
  failDelivery(
    delivery: TxOBDelivery,
    failure: TxOBDeliveryFailure,
    opts?: TxOBStoreOpts & { within?: (tx: TTx) => Promise<void> },
  ): Promise<boolean>;

  /** Extend leases. Returns the deliveries whose lease was lost. */
  extendLeases(
    deliveries: TxOBDelivery[],
    opts: TxOBStoreOpts & { leaseMs: number },
  ): Promise<TxOBDelivery[]>;

  /** Give back claimed deliveries that were never attempted (e.g. on shutdown). */
  releaseDeliveries(
    deliveries: TxOBDelivery[],
    opts?: TxOBStoreOpts,
  ): Promise<void>;
}

export const deliveryKey = (
  delivery: Pick<TxOBDelivery, "event_id" | "handler">,
): string => `${delivery.event_id}:${delivery.handler}`;
