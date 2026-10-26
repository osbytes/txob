import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { deliveryKey, type TxOBFanoutEvent, type TxOBStore } from "../store.js";
import type {
  TxOBDelivery,
  TxOBEvent,
  TxOBSerializedError,
  WakeupEmitter,
} from "../types.js";

/** Buffers writes until the transaction commits. */
export type MemoryTx = {
  events: TxOBEvent[];
};

type EventRecord = TxOBEvent & {
  fanned_out_at: Date | null;
  fanout_attempts: number;
  fanout_run_after: number;
  fanout_lease_token: string | null;
};

export type MemoryDeliveryRecord = {
  event_id: string;
  event_type: string;
  handler: string;
  status: "pending" | "completed" | "dead";
  attempts: number;
  priority: number;
  concurrency_key: string | null;
  run_after: number;
  lease_token: string | null;
  last_error: TxOBSerializedError | null;
  created_at: number;
  completed_at: Date | null;
};

export type MemoryStore = TxOBStore<MemoryTx> & {
  /** Runs `fn` with a transaction whose writes apply only if `fn` resolves. */
  transaction<T>(fn: (tx: MemoryTx) => Promise<T>): Promise<T>;
  /** Emits `wakeup` when events or deliveries are inserted. */
  wakeupEmitter: WakeupEmitter;
  /** Snapshot of all deliveries, for assertions in tests. */
  getDeliveries(): MemoryDeliveryRecord[];
  /** Snapshot of all events with fan-out state, for assertions in tests. */
  getEvents(): (TxOBEvent & { fanned_out_at: Date | null })[];
};

/**
 * In-process store for tests and local development. Not durable and not shared across
 * processes.
 */
export const createMemoryStore = (): MemoryStore => {
  const events = new Map<string, EventRecord>();
  const deliveries = new Map<string, MemoryDeliveryRecord>();
  const emitter = new EventEmitter();
  /** Claim times per handler, for rate limits. */
  const starts = new Map<string, number[]>();
  let sequence = 0;

  const commit = (tx: MemoryTx) => {
    for (const event of tx.events) {
      if (events.has(event.id)) {
        throw new Error(`duplicate event id '${event.id}'`);
      }
    }
    for (const event of tx.events) {
      events.set(event.id, {
        ...structuredClone(event),
        fanned_out_at: null,
        fanout_attempts: 0,
        fanout_run_after: Date.now(),
        fanout_lease_token: null,
      });
    }
    if (tx.events.length > 0) emitter.emit("wakeup");
  };

  const transaction = async <T>(
    fn: (tx: MemoryTx) => Promise<T>,
  ): Promise<T> => {
    const tx: MemoryTx = { events: [] };
    const result = await fn(tx);
    commit(tx);
    return result;
  };

  const leased = (delivery: TxOBDelivery): MemoryDeliveryRecord | undefined => {
    const record = deliveries.get(deliveryKey(delivery));
    return record && record.lease_token === delivery.lease_token
      ? record
      : undefined;
  };

  const toDelivery = (record: MemoryDeliveryRecord): TxOBDelivery => {
    const event = events.get(record.event_id)!;
    return {
      event_id: record.event_id,
      event_type: record.event_type,
      handler: record.handler,
      attempts: record.attempts,
      priority: record.priority,
      concurrency_key: record.concurrency_key,
      lease_token: record.lease_token!,
      event: structuredClone({
        id: event.id,
        type: event.type,
        data: event.data,
        correlation_id: event.correlation_id,
        created_at: event.created_at,
      }),
    };
  };

  return {
    transaction,

    wakeupEmitter: {
      on: (event, listener) => emitter.on(event, listener),
      off: (event, listener) => emitter.off(event, listener),
      close: async () => {
        emitter.removeAllListeners();
      },
    },

    getDeliveries: () =>
      [...deliveries.values()].map((d) => structuredClone(d)),

    getEvents: () =>
      [...events.values()].map(
        ({ id, type, data, correlation_id, created_at, fanned_out_at }) =>
          structuredClone({
            id,
            type,
            data,
            correlation_id,
            created_at,
            fanned_out_at,
          }),
      ),

    insertEvent: async (tx, event) => {
      tx.events.push(event);
    },

    claimEventsForFanout: async ({ types, limit, leaseMs }) => {
      const now = Date.now();
      const claimable = [...events.values()]
        .filter(
          (e) =>
            e.fanned_out_at === null &&
            e.fanout_run_after <= now &&
            types.includes(e.type),
        )
        .sort((a, b) => a.created_at.getTime() - b.created_at.getTime())
        .slice(0, limit);
      return claimable.map((e): TxOBFanoutEvent => {
        e.fanout_run_after = now + leaseMs;
        e.fanout_lease_token = randomUUID();
        e.fanout_attempts++;
        return structuredClone({
          id: e.id,
          type: e.type,
          data: e.data,
          correlation_id: e.correlation_id,
          created_at: e.created_at,
          fanout_attempts: e.fanout_attempts,
          fanout_lease_token: e.fanout_lease_token,
        });
      });
    },

    completeFanout: async ({ event_ids, deliveries: newDeliveries }) => {
      let inserted = 0;
      for (const d of newDeliveries) {
        const key = deliveryKey(d);
        if (deliveries.has(key)) continue;
        deliveries.set(key, {
          ...d,
          status: "pending",
          attempts: 0,
          run_after: Date.now(),
          lease_token: null,
          last_error: null,
          created_at: sequence++,
          completed_at: null,
        });
        inserted++;
      }
      for (const id of event_ids) {
        const event = events.get(id);
        if (event) {
          event.fanned_out_at = new Date();
          event.fanout_lease_token = null;
        }
      }
      if (inserted > 0) emitter.emit("wakeup");
    },

    claimDeliveries: async ({ slots, limit, leaseMs }) => {
      const now = Date.now();
      const byPriority = (a: MemoryDeliveryRecord, b: MemoryDeliveryRecord) =>
        b.priority - a.priority ||
        a.run_after - b.run_after ||
        a.created_at - b.created_at;
      const isLive = (d: MemoryDeliveryRecord) =>
        d.status === "pending" && d.lease_token !== null && d.run_after > now;

      const candidates = slots.flatMap((slot) => {
        const handlerKey = `${slot.event_type}:${slot.handler}`;
        const ofHandler = [...deliveries.values()].filter(
          (d) => d.event_type === slot.event_type && d.handler === slot.handler,
        );
        const live = ofHandler.filter(isLive);

        let budget = slot.limit;
        if (slot.concurrency !== undefined) {
          budget = Math.min(budget, slot.concurrency - live.length);
        }
        if (slot.rateLimit) {
          const windowStart = now - slot.rateLimit.durationMs;
          const recent = (starts.get(handlerKey) ?? []).filter(
            (t) => t > windowStart,
          );
          starts.set(handlerKey, recent);
          budget = Math.min(budget, slot.rateLimit.max - recent.length);
        }

        const perKey = new Map<string, number>();
        for (const d of live) {
          if (d.concurrency_key !== null) {
            perKey.set(
              d.concurrency_key,
              (perKey.get(d.concurrency_key) ?? 0) + 1,
            );
          }
        }

        const picked: MemoryDeliveryRecord[] = [];
        for (const d of ofHandler
          .filter((d) => d.status === "pending" && d.run_after <= now)
          .sort(byPriority)) {
          if (picked.length >= budget) break;
          if (
            slot.concurrencyPerKey !== undefined &&
            d.concurrency_key !== null
          ) {
            const running = perKey.get(d.concurrency_key) ?? 0;
            if (running >= slot.concurrencyPerKey) continue;
            perKey.set(d.concurrency_key, running + 1);
          }
          picked.push(d);
        }
        return picked;
      });

      const rateLimited = new Set(
        slots
          .filter((s) => s.rateLimit)
          .map((s) => `${s.event_type}:${s.handler}`),
      );
      return candidates
        .sort(byPriority)
        .slice(0, limit)
        .map((d) => {
          d.run_after = now + leaseMs;
          d.lease_token = randomUUID();
          d.attempts++;
          const handlerKey = `${d.event_type}:${d.handler}`;
          if (rateLimited.has(handlerKey)) {
            starts.set(handlerKey, [...(starts.get(handlerKey) ?? []), now]);
          }
          return toDelivery(d);
        });
    },

    completeDelivery: async (delivery) => {
      const record = leased(delivery);
      if (!record) return false;
      record.status = "completed";
      record.completed_at = new Date();
      record.lease_token = null;
      return true;
    },

    failDelivery: async (delivery, failure, opts) => {
      const record = leased(delivery);
      if (!record) return false;
      if (opts?.within) {
        // Apply the hook's writes and the update together, or neither
        const tx: MemoryTx = { events: [] };
        await opts.within(tx);
        commit(tx);
      }
      record.status = failure.run_after === null ? "dead" : "pending";
      record.run_after = failure.run_after?.getTime() ?? record.run_after;
      record.completed_at = failure.run_after === null ? new Date() : null;
      record.lease_token = null;
      record.last_error = failure.error;
      return true;
    },

    extendLeases: async (items, { leaseMs }) => {
      const lost: TxOBDelivery[] = [];
      for (const delivery of items) {
        const record = leased(delivery);
        if (record) {
          record.run_after = Date.now() + leaseMs;
        } else {
          lost.push(delivery);
        }
      }
      return lost;
    },

    releaseDeliveries: async (items) => {
      for (const delivery of items) {
        const record = leased(delivery);
        if (!record) continue;
        record.run_after = Date.now();
        record.lease_token = null;
        record.attempts = Math.max(record.attempts - 1, 0);
      }
    },
  };
};
