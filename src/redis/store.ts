import { randomUUID } from "node:crypto";
import type { TxOBStore } from "../store.js";
import type { TxOBDelivery, TxOBEvent } from "../types.js";
import {
  createRedisDriver,
  createScriptRunner,
  type RedisClientLike,
  type RedisConnection,
} from "./driver.js";
import {
  claimDeliveriesScript,
  claimFanoutScript,
  completeDeliveryScript,
  completeFanoutScript,
  extendLeasesScript,
  failDeliveryScript,
  insertEventScript,
  releaseDeliveriesScript,
} from "./scripts.js";

export type RedisKeyOpts = {
  /**
   * Prefix for every key txob writes. Default: "{txob}:". The braces are a cluster hash tag
   * that keeps all of txob's keys in one slot, which its scripts require.
   */
  prefix?: string;
};

export type CreateRedisStoreOpts<TClient extends RedisClientLike> =
  RedisKeyOpts & {
    /** An ioredis/iovalkey client, or a connected node-redis client. */
    client: TClient;
  };

/** The client's MULTI, which `outbox.publish` adds the event to. */
export type RedisMultiOf<TClient> = TClient extends {
  multi(...args: any[]): infer TMulti;
}
  ? TMulti
  : never;

export const defaultPrefix = "{txob}:";

type Hash = Record<string, string>;

const toHash = (flat: string[]): Hash => {
  const hash: Hash = {};
  for (let i = 0; i < flat.length; i += 2) hash[flat[i]] = flat[i + 1];
  return hash;
};

const toEvent = (id: string, hash: Hash): TxOBEvent => ({
  id,
  type: hash.type,
  data: JSON.parse(hash.data),
  correlation_id: hash.correlation_id ?? null,
  created_at: new Date(Number(hash.created_at)),
});

const nullable = (value: string | null) => (value === null ? "" : `=${value}`);

const deliveryId = (d: Pick<TxOBDelivery, "event_id" | "handler">) =>
  `${d.event_id}:${d.handler}`;

/**
 * Redis store (also Valkey, and other servers that run Redis Lua scripts). Every claim and
 * state change is a single Lua script, so limits hold across processors without locks.
 *
 * Publish by queueing the event on the same MULTI as your writes:
 *
 * ```typescript
 * const multi = redis.multi();
 * multi.hset(`user:${id}`, user);
 * await outbox.publish(multi, "UserCreated", { userId: id });
 * await multi.exec();
 * ```
 *
 * Durability is whatever your Redis persistence gives you (e.g. AOF with
 * `appendfsync always`). Run with `maxmemory-policy noeviction` so outbox keys are never
 * evicted.
 */
export const createRedisStore = <TClient extends RedisClientLike>(
  opts: CreateRedisStoreOpts<TClient>,
): TxOBStore<RedisMultiOf<TClient>> => {
  const prefix = opts.prefix ?? defaultPrefix;
  const driver = createRedisDriver(opts.client);
  // Declared as each script's key so cluster clients route to the slot holding txob's keys
  const routingKey = `${prefix}events`;
  const run = createScriptRunner(driver, routingKey);

  // WATCH needs a connection of its own. One is shared by all dead-delivery hooks, used one
  // at a time, and closed once idle.
  let watchConnection: Promise<RedisConnection> | undefined;
  let watchQueue: Promise<unknown> = Promise.resolve();
  let idleTimer: NodeJS.Timeout | undefined;
  const closeWatchConnection = () => {
    const closing = watchConnection;
    watchConnection = undefined;
    void closing?.then((conn) => conn.close()).catch(() => {});
  };
  const withWatchConnection = <T>(
    fn: (conn: RedisConnection) => Promise<T>,
  ): Promise<T> => {
    const run = watchQueue.then(async () => {
      clearTimeout(idleTimer);
      watchConnection ??= driver.connect();
      let conn: RedisConnection;
      try {
        conn = await watchConnection;
      } catch (error) {
        watchConnection = undefined;
        throw error;
      }
      try {
        const result = await fn(conn);
        await conn.command(["UNWATCH"]);
        return result;
      } catch (error) {
        // The connection may be mid-transaction or broken; start over with a fresh one
        closeWatchConnection();
        throw error;
      } finally {
        idleTimer = setTimeout(closeWatchConnection, 1_000);
        idleTimer.unref?.();
      }
    });
    watchQueue = run.catch(() => {});
    return run;
  };

  const failArgs = (
    delivery: TxOBDelivery,
    failure: Parameters<TxOBStore["failDelivery"]>[1],
  ) => [
    prefix,
    deliveryId(delivery),
    delivery.lease_token,
    failure.run_after ? String(failure.run_after.getTime()) : "",
    JSON.stringify(failure.error),
  ];

  return {
    insertEvent: async (multi, event) => {
      // Inside a MULTI, so the script is sent in full rather than by SHA
      driver.queue(multi as object, [
        "EVAL",
        insertEventScript,
        "1",
        routingKey,
        prefix,
        event.id,
        event.type,
        JSON.stringify(event.data),
        String(event.created_at.getTime()),
        nullable(event.correlation_id),
      ]);
    },

    claimEventsForFanout: async ({ types, limit, leaseMs }) => {
      if (types.length === 0) return [];
      const token = randomUUID();
      const rows: [string, string[]][] = await run(claimFanoutScript, [
        prefix,
        leaseMs,
        limit,
        token,
        ...types,
      ]);
      return rows.map(([id, flat]) => {
        const hash = toHash(flat);
        return {
          ...toEvent(id, hash),
          fanout_attempts: Number(hash.fanout_attempts),
          fanout_lease_token: token,
        };
      });
    },

    completeFanout: async ({ event_ids, deliveries }) => {
      if (event_ids.length === 0 && deliveries.length === 0) return;
      await run(completeFanoutScript, [
        prefix,
        event_ids.length,
        ...event_ids,
        ...deliveries.flatMap((d) => [
          d.event_id,
          d.event_type,
          d.handler,
          d.priority,
          nullable(d.concurrency_key),
        ]),
      ]);
    },

    claimDeliveries: async ({ slots, limit, leaseMs }) => {
      if (slots.length === 0 || limit <= 0) return [];
      const token = randomUUID();
      const optional = (value: number | undefined) =>
        value === undefined ? "" : value;
      const rows: [string, string[], string[]][] = await run(
        claimDeliveriesScript,
        [
          prefix,
          leaseMs,
          limit,
          token,
          slots.length,
          ...slots.flatMap((s) => [
            s.event_type,
            s.handler,
            s.limit,
            optional(s.concurrency),
            optional(s.concurrencyPerKey),
            optional(s.rateLimit?.max),
            optional(s.rateLimit?.durationMs),
          ]),
        ],
      );
      return rows.map(([, deliveryFlat, eventFlat]) => {
        const d = toHash(deliveryFlat);
        return {
          event_id: d.event_id,
          event_type: d.event_type,
          handler: d.handler,
          attempts: Number(d.attempts),
          priority: Number(d.priority),
          concurrency_key: d.concurrency_key ?? null,
          lease_token: token,
          event: toEvent(d.event_id, toHash(eventFlat)),
        };
      });
    },

    completeDelivery: async (delivery) =>
      (await run(completeDeliveryScript, [
        prefix,
        deliveryId(delivery),
        delivery.lease_token,
      ])) === 1,

    failDelivery: async (delivery, failure, failOpts) => {
      const within = failOpts?.within;
      if (!within) {
        return (
          (await run(failDeliveryScript, failArgs(delivery, failure))) === 1
        );
      }

      // The hook's writes must commit with the state change, so both go in one MULTI, guarded
      // by WATCH on the delivery. WATCH needs a connection of its own.
      const key = `${prefix}d:${deliveryId(delivery)}`;
      return withWatchConnection(async (conn) => {
        for (let attempt = 0; attempt < 5; attempt++) {
          await conn.watch(key);
          if (
            (await conn.command(["HGET", key, "lease_token"])) !==
            delivery.lease_token
          ) {
            await conn.command(["UNWATCH"]);
            return false;
          }
          const multi = conn.multi();
          driver.queue(multi, [
            "EVAL",
            failDeliveryScript,
            "1",
            routingKey,
            ...failArgs(delivery, failure),
          ]);
          await within(multi as RedisMultiOf<TClient>);
          const results = await conn.exec(multi);
          // A lease renewal touched the delivery; check the lease again and retry
          if (results === null) continue;
          return results[0] === 1;
        }
        return false;
      });
    },

    extendLeases: async (deliveries, { leaseMs }) => {
      if (deliveries.length === 0) return [];
      const lost: number[] = await run(extendLeasesScript, [
        prefix,
        leaseMs,
        ...deliveries.flatMap((d) => [deliveryId(d), d.lease_token]),
      ]);
      return lost.map((i) => deliveries[i]);
    },

    releaseDeliveries: async (deliveries) => {
      if (deliveries.length === 0) return;
      await run(releaseDeliveriesScript, [
        prefix,
        ...deliveries.flatMap((d) => [deliveryId(d), d.lease_token]),
      ]);
    },
  };
};
