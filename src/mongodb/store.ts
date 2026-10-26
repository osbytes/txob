import { randomUUID } from "node:crypto";
import {
  MongoBulkWriteError,
  MongoServerError,
  type ClientSession,
  type Filter,
  type MongoClient,
} from "mongodb";
import type { TxOBFanoutEvent, TxOBHandlerSlot, TxOBStore } from "../store.js";
import type { TxOBDelivery, TxOBEvent, TxOBSerializedError } from "../types.js";

export type MongoCollectionOpts = {
  /** Database for txob's collections. */
  db: string;
  /** Prefix for collection names. Default: "txob_". */
  collectionPrefix?: string;
};

export type CreateMongoStoreOpts = MongoCollectionOpts & {
  client: MongoClient;
};

type EventDoc = {
  _id: string;
  type: string;
  data: TxOBEvent["data"];
  correlation_id: string | null;
  created_at: Date;
  fanned_out_at: Date | null;
  fanout_attempts: number;
  fanout_run_after: Date;
  fanout_lease_token: string | null;
};

type DeliveryDoc = {
  _id: string;
  event_id: string;
  event_type: string;
  handler: string;
  status: "pending" | "completed" | "dead";
  attempts: number;
  priority: number;
  concurrency_key: string | null;
  /** When the delivery may next be attempted; doubles as the lease expiry while claimed. */
  run_after: Date;
  lease_token: string | null;
  last_error: TxOBSerializedError | null;
  created_at: Date;
  completed_at: Date | null;
};

type LockDoc = { _id: string; token: string; expires_at: Date };

type RateLimitClaimDoc = {
  event_type: string;
  handler: string;
  claimed_at: Date;
};

export const resolveCollections = (opts: CreateMongoStoreOpts) => {
  const prefix = opts.collectionPrefix ?? "txob_";
  const db = opts.client.db(opts.db);
  return {
    names: {
      events: `${prefix}events`,
      deliveries: `${prefix}deliveries`,
      locks: `${prefix}locks`,
      rateLimitClaims: `${prefix}rate_limit_claims`,
    },
    events: db.collection<EventDoc>(`${prefix}events`),
    deliveries: db.collection<DeliveryDoc>(`${prefix}deliveries`),
    locks: db.collection<LockDoc>(`${prefix}locks`),
    rateLimitClaims: db.collection<RateLimitClaimDoc>(
      `${prefix}rate_limit_claims`,
    ),
  };
};

const toEvent = (doc: EventDoc): TxOBEvent => ({
  id: doc._id,
  type: doc.type,
  data: doc.data,
  correlation_id: doc.correlation_id,
  created_at: doc.created_at,
});

const isDuplicateKey = (error: unknown): boolean =>
  (error instanceof MongoServerError && error.code === 11000) ||
  (error instanceof MongoBulkWriteError &&
    (Array.isArray(error.writeErrors)
      ? error.writeErrors
      : [error.writeErrors]
    ).every((e) => e.code === 11000));

const deliveryId = (eventId: string, handler: string) =>
  `${eventId}:${handler}`;

/** How long a claim lock may be held before another processor can take it over. */
const lockTtlMs = 10_000;

/**
 * MongoDB store. Run `migrate` once to create indexes.
 *
 * Requires a replica set or sharded cluster: publishing atomically with business data and
 * `onDeliveryDead` use multi-document transactions, and wakeups use change streams.
 * A standalone server can be converted to a single-node replica set.
 *
 * Times are taken from the processors' clocks, so keep them synchronized (e.g. NTP); skew
 * well below `leaseMs` is harmless.
 *
 * Publish with `outbox.publish(session, ...)` inside `session.withTransaction(...)`.
 */
export const createMongoStore = (
  opts: CreateMongoStoreOpts,
): TxOBStore<ClientSession> => {
  const { client } = opts;
  const { events, deliveries, locks, rateLimitClaims } =
    resolveCollections(opts);

  const lockKey = (slot: TxOBHandlerSlot) =>
    `claim:${slot.event_type}:${slot.handler}`;

  /**
   * Compare-and-set lock: inserting the lock document fails with a duplicate key while
   * another processor holds an unexpired lock.
   */
  const tryLock = async (key: string, token: string): Promise<boolean> => {
    const now = new Date();
    try {
      await locks.findOneAndUpdate(
        { _id: key, expires_at: { $lte: now } },
        { $set: { token, expires_at: new Date(now.getTime() + lockTtlMs) } },
        { upsert: true },
      );
      return true;
    } catch (error) {
      if (isDuplicateKey(error)) return false;
      throw error;
    }
  };

  const lock = async (key: string, token: string): Promise<boolean> => {
    // Claims hold the lock for a few milliseconds; retry briefly before giving up this round
    for (let attempt = 0; attempt < 3; attempt++) {
      if (await tryLock(key, token)) return true;
      await new Promise((resolve) =>
        setTimeout(resolve, 5 + Math.random() * 10),
      );
    }
    return false;
  };

  const liveFilter = (
    slot: TxOBHandlerSlot,
    now: Date,
  ): Filter<DeliveryDoc> => ({
    event_type: slot.event_type,
    handler: slot.handler,
    status: "pending",
    lease_token: { $ne: null },
    run_after: { $gt: now },
  });

  const byPriority = (a: DeliveryDoc, b: DeliveryDoc) =>
    b.priority - a.priority ||
    a.run_after.getTime() - b.run_after.getTime() ||
    a.created_at.getTime() - b.created_at.getTime();

  /** Picks claimable deliveries for one handler within its limits. */
  const pickForSlot = async (
    slot: TxOBHandlerSlot,
    now: Date,
  ): Promise<DeliveryDoc[]> => {
    let budget = slot.limit;
    if (slot.concurrency !== undefined) {
      budget = Math.min(
        budget,
        slot.concurrency -
          (await deliveries.countDocuments(liveFilter(slot, now))),
      );
    }
    if (slot.rateLimit) {
      const recent = await rateLimitClaims.countDocuments({
        event_type: slot.event_type,
        handler: slot.handler,
        claimed_at: {
          $gt: new Date(now.getTime() - slot.rateLimit.durationMs),
        },
      });
      budget = Math.min(budget, slot.rateLimit.max - recent);
    }
    if (budget <= 0) return [];

    const filter: Filter<DeliveryDoc> = {
      event_type: slot.event_type,
      handler: slot.handler,
      status: "pending",
      run_after: { $lte: now },
    };
    const perKey = new Map<string, number>();
    if (slot.concurrencyPerKey !== undefined) {
      const live = await deliveries
        .aggregate<{
          _id: string;
          n: number;
        }>([
          {
            $match: {
              ...liveFilter(slot, now),
              concurrency_key: { $ne: null },
            },
          },
          { $group: { _id: "$concurrency_key", n: { $sum: 1 } } },
        ])
        .toArray();
      for (const { _id, n } of live) perKey.set(_id, n);
      // Skip keys already at their limit so they can't crowd out other keys
      const saturated = live
        .filter(({ n }) => n >= slot.concurrencyPerKey!)
        .map(({ _id }) => _id);
      if (saturated.length > 0) filter.concurrency_key = { $nin: saturated };
    }

    const candidates = await deliveries
      .find(filter)
      .sort({ priority: -1, run_after: 1, created_at: 1 })
      // with a per-key limit some candidates are dropped below, so look further ahead
      .limit(slot.concurrencyPerKey === undefined ? budget : budget * 10)
      .toArray();

    const picked: DeliveryDoc[] = [];
    for (const d of candidates) {
      if (picked.length >= budget) break;
      if (slot.concurrencyPerKey !== undefined && d.concurrency_key !== null) {
        const running = perKey.get(d.concurrency_key) ?? 0;
        if (running >= slot.concurrencyPerKey) continue;
        perKey.set(d.concurrency_key, running + 1);
      }
      picked.push(d);
    }
    return picked;
  };

  /** Groups deliveries by lease token (one token per claim batch). */
  const byToken = (items: TxOBDelivery[]) => {
    const groups = new Map<string, string[]>();
    for (const d of items) {
      groups.set(d.lease_token, [
        ...(groups.get(d.lease_token) ?? []),
        deliveryId(d.event_id, d.handler),
      ]);
    }
    return groups;
  };

  return {
    insertEvent: async (session, event) => {
      await events.insertOne(
        {
          _id: event.id,
          type: event.type,
          data: event.data,
          correlation_id: event.correlation_id,
          created_at: event.created_at,
          fanned_out_at: null,
          fanout_attempts: 0,
          fanout_run_after: event.created_at,
          fanout_lease_token: null,
        },
        { session },
      );
    },

    claimEventsForFanout: async ({ types, limit, leaseMs }) => {
      const now = new Date();
      const filter: Filter<EventDoc> = {
        fanned_out_at: null,
        fanout_run_after: { $lte: now },
        type: { $in: types },
      };
      const candidates = await events
        .find(filter, { projection: { _id: 1 } })
        .sort({ created_at: 1 })
        .limit(limit)
        .toArray();
      if (candidates.length === 0) return [];

      // Each document is updated atomically, and the filter re-checks availability, so
      // concurrent processors can't both lease the same event
      const token = randomUUID();
      await events.updateMany(
        { ...filter, _id: { $in: candidates.map((c) => c._id) } },
        {
          $set: {
            fanout_run_after: new Date(now.getTime() + leaseMs),
            fanout_lease_token: token,
          },
          $inc: { fanout_attempts: 1 },
        },
      );
      const claimed = await events
        .find({ fanout_lease_token: token })
        .toArray();
      return claimed.map(
        (doc): TxOBFanoutEvent => ({
          ...toEvent(doc),
          fanout_attempts: doc.fanout_attempts,
          fanout_lease_token: token,
        }),
      );
    },

    completeFanout: async ({ event_ids, deliveries: newDeliveries }) => {
      // Not one transaction, but safe to repeat: existing deliveries are kept, so a crash
      // between the two writes just re-runs fan-out for the event
      if (newDeliveries.length > 0) {
        const now = new Date();
        try {
          await deliveries.insertMany(
            newDeliveries.map(
              (d): DeliveryDoc => ({
                _id: deliveryId(d.event_id, d.handler),
                ...d,
                status: "pending",
                attempts: 0,
                run_after: now,
                lease_token: null,
                last_error: null,
                created_at: now,
                completed_at: null,
              }),
            ),
            { ordered: false },
          );
        } catch (error) {
          if (!isDuplicateKey(error)) throw error;
        }
      }
      await events.updateMany(
        { _id: { $in: event_ids } },
        { $set: { fanned_out_at: new Date(), fanout_lease_token: null } },
      );
    },

    claimDeliveries: async ({ slots, limit, leaseMs }) => {
      const token = randomUUID();
      const limited = slots.filter(
        (s) =>
          s.concurrency !== undefined ||
          s.concurrencyPerKey !== undefined ||
          s.rateLimit !== undefined,
      );

      // Limits are checked against deliveries leased by every processor, so claims for a
      // limited handler are serialized with a lock. Busy handlers are skipped this round.
      const locked: string[] = [];
      try {
        const claimable: TxOBHandlerSlot[] = slots.filter(
          (s) => !limited.includes(s),
        );
        // Each lock is taken independently and busy ones are skipped, so nobody waits on
        // one lock while holding another
        await Promise.all(
          limited.map(async (slot) => {
            if (await lock(lockKey(slot), token)) {
              locked.push(lockKey(slot));
              claimable.push(slot);
            }
          }),
        );
        const now = new Date();

        const picked = (
          await Promise.all(claimable.map((slot) => pickForSlot(slot, now)))
        )
          .flat()
          .sort(byPriority)
          .slice(0, limit);
        if (picked.length === 0) return [];

        await deliveries.updateMany(
          {
            _id: { $in: picked.map((d) => d._id) },
            status: "pending",
            run_after: { $lte: now },
          },
          {
            $set: {
              lease_token: token,
              run_after: new Date(now.getTime() + leaseMs),
            },
            $inc: { attempts: 1 },
          },
        );
        const claimed = (
          await deliveries.find({ lease_token: token }).toArray()
        ).sort(byPriority);

        const rateLimited = claimed.filter((d) =>
          slots.some(
            (s) =>
              s.rateLimit &&
              s.event_type === d.event_type &&
              s.handler === d.handler,
          ),
        );
        if (rateLimited.length > 0) {
          await rateLimitClaims.insertMany(
            rateLimited.map((d) => ({
              event_type: d.event_type,
              handler: d.handler,
              claimed_at: now,
            })),
          );
          for (const slot of slots.filter((s) => s.rateLimit)) {
            await rateLimitClaims.deleteMany({
              event_type: slot.event_type,
              handler: slot.handler,
              claimed_at: {
                $lte: new Date(now.getTime() - slot.rateLimit!.durationMs),
              },
            });
          }
        }

        const eventDocs = await events
          .find({ _id: { $in: [...new Set(claimed.map((d) => d.event_id))] } })
          .toArray();
        const eventsById = new Map(eventDocs.map((e) => [e._id, toEvent(e)]));
        return claimed.map(
          (d): TxOBDelivery => ({
            event_id: d.event_id,
            event_type: d.event_type,
            handler: d.handler,
            attempts: d.attempts,
            priority: d.priority,
            concurrency_key: d.concurrency_key,
            lease_token: token,
            event: eventsById.get(d.event_id)!,
          }),
        );
      } finally {
        if (locked.length > 0) {
          await locks.deleteMany({ _id: { $in: locked }, token });
        }
      }
    },

    completeDelivery: async (delivery) => {
      const result = await deliveries.updateOne(
        {
          _id: deliveryId(delivery.event_id, delivery.handler),
          lease_token: delivery.lease_token,
        },
        {
          $set: {
            status: "completed",
            completed_at: new Date(),
            lease_token: null,
          },
        },
      );
      return result.matchedCount === 1;
    },

    failDelivery: async (delivery, failure, failOpts) => {
      const dead = failure.run_after === null;
      const fail = (session?: ClientSession) =>
        deliveries.updateOne(
          {
            _id: deliveryId(delivery.event_id, delivery.handler),
            lease_token: delivery.lease_token,
          },
          {
            $set: {
              status: dead ? "dead" : "pending",
              ...(failure.run_after ? { run_after: failure.run_after } : {}),
              completed_at: dead ? new Date() : null,
              lease_token: null,
              last_error: failure.error,
            },
          },
          { session },
        );

      const within = failOpts?.within;
      if (!within) {
        return (await fail()).matchedCount === 1;
      }

      const session = client.startSession();
      try {
        return await session.withTransaction(async () => {
          if ((await fail(session)).matchedCount !== 1) return false;
          await within(session);
          return true;
        });
      } finally {
        await session.endSession();
      }
    },

    extendLeases: async (items, { leaseMs }) => {
      const lost: TxOBDelivery[] = [];
      for (const [token, ids] of byToken(items)) {
        await deliveries.updateMany(
          { _id: { $in: ids }, lease_token: token },
          { $set: { run_after: new Date(Date.now() + leaseMs) } },
        );
        const held = new Set(
          (
            await deliveries
              .find(
                { _id: { $in: ids }, lease_token: token },
                { projection: { _id: 1 } },
              )
              .toArray()
          ).map((d) => d._id),
        );
        lost.push(
          ...items.filter(
            (d) =>
              d.lease_token === token &&
              !held.has(deliveryId(d.event_id, d.handler)),
          ),
        );
      }
      return lost;
    },

    releaseDeliveries: async (items) => {
      for (const [token, ids] of byToken(items)) {
        await deliveries.updateMany(
          { _id: { $in: ids }, lease_token: token },
          {
            $set: { run_after: new Date(), lease_token: null },
            $inc: { attempts: -1 },
          },
        );
      }
    },
  };
};
