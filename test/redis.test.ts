import { randomBytes } from "node:crypto";
import { Redis } from "ioredis";
import { Redis as Valkey } from "iovalkey";
import { createClient } from "redis";
import { describe, expect, it } from "vitest";
import {
  createRedisStore,
  createRedisWakeupEmitter,
  type RedisClientLike,
} from "../src/redis/index.js";
import { createOutbox } from "../src/index.js";
import { runStoreContract, type ContractDelivery } from "./contract.js";
import { schema } from "./helpers.js";

/**
 * Integration tests against a real Redis (or Valkey). Skipped unless TXOB_TEST_REDIS_URL is
 * set, e.g.
 *   docker run --rm -p 63799:6379 redis:8-alpine
 *   TXOB_TEST_REDIS_URL=redis://localhost:63799 yarn test
 */
const url = process.env.TXOB_TEST_REDIS_URL;

/** Commands txob needs to inspect and clean up its keys, on either client. */
type Inspect = {
  command(args: string[]): Promise<any>;
  close(): Promise<unknown>;
};

const harness = async <TClient extends RedisClientLike & { multi(): any }>(
  client: TClient,
  inspect: Inspect,
  exec: (multi: ReturnType<TClient["multi"]>) => Promise<unknown>,
) => {
  const prefix = `{txob_test_${randomBytes(4).toString("hex")}}:`;
  const opts = { client, prefix };
  const store = createRedisStore(opts);
  const hgetall = async (key: string) => {
    const flat: string[] | Record<string, string> = await inspect.command([
      "HGETALL",
      key,
    ]);
    // node-redis maps HGETALL replies to objects
    if (!Array.isArray(flat)) return flat;
    const hash: Record<string, string> = {};
    for (let i = 0; i < flat.length; i += 2) hash[flat[i]] = flat[i + 1];
    return hash;
  };
  return {
    store,
    transaction: async <T>(
      fn: (multi: ReturnType<TClient["multi"]>) => Promise<T>,
    ) => {
      const multi = client.multi() as ReturnType<TClient["multi"]>;
      const result = await fn(multi);
      await exec(multi);
      return result;
    },
    getDeliveries: async () => {
      const ids: string[] = await inspect.command([
        "ZRANGE",
        `${prefix}deliveries`,
        "0",
        "-1",
      ]);
      return Promise.all(
        ids.map(async (id) => {
          const d = await hgetall(`${prefix}d:${id}`);
          return {
            ...d,
            attempts: Number(d.attempts),
          } as unknown as ContractDelivery;
        }),
      );
    },
    getEvents: async () => {
      const ids: string[] = await inspect.command([
        "ZRANGE",
        `${prefix}events`,
        "0",
        "-1",
      ]);
      return Promise.all(
        ids.map(async (id) => {
          const e = await hgetall(`${prefix}e:${id}`);
          return {
            id,
            type: e.type,
            fanned_out_at: e.fanned_out_at
              ? new Date(Number(e.fanned_out_at))
              : null,
          };
        }),
      );
    },
    createWakeupEmitter: () => createRedisWakeupEmitter(opts),
    teardown: async () => {
      let cursor = "0";
      do {
        const [next, keys]: [string, string[]] = await inspect.command([
          "SCAN",
          cursor,
          "MATCH",
          `${prefix.replace(/[{}[\]*?]/g, "\\$&")}*`,
          "COUNT",
          "1000",
        ]);
        cursor = next;
        if (keys.length > 0) await inspect.command(["DEL", ...keys]);
      } while (cursor !== "0");
      await inspect.close();
    },
  };
};

describe.skipIf(!url)("redis", () => {
  runStoreContract("redis (ioredis)", async () => {
    const client = new Redis(url!);
    return harness(
      client,
      {
        command: (args) => client.call(args[0], ...args.slice(1)),
        close: () => client.quit(),
      },
      (multi) => multi.exec(),
    );
  });

  runStoreContract("redis (node-redis)", async () => {
    const client = await createClient({ url }).connect();
    return harness(
      client,
      {
        command: (args) => client.sendCommand(args),
        close: () => client.close(),
      },
      (multi) => multi.exec(),
    );
  });

  runStoreContract("valkey client (iovalkey)", async () => {
    const client = new Valkey(url!);
    return harness(
      client,
      {
        command: (args) => client.call(args[0], ...args.slice(1)),
        close: () => client.quit(),
      },
      (multi) => multi.exec(),
    );
  });

  describe("transactions", () => {
    const eventSchemas = { Created: schema<{ id: string }>() };

    it("commits the event with business writes in the same MULTI, and ignores duplicate ids", async () => {
      const client = new Redis(url!);
      const prefix = `{txob_test_${randomBytes(4).toString("hex")}}:`;
      const outbox = createOutbox({
        eventSchemas,
        store: createRedisStore({ client, prefix }),
      });
      try {
        const multi = client.multi();
        multi.set(`${prefix}user`, "u1");
        const event = await outbox.publish(multi, "Created", { id: "u1" });
        expect(await client.exists(`${prefix}e:${event.id}`)).toBe(0);
        await multi.exec();
        expect(await client.get(`${prefix}user`)).toBe("u1");
        expect(await client.hget(`${prefix}e:${event.id}`, "type")).toBe(
          "Created",
        );

        const again = client.multi();
        await outbox.publish(
          again,
          "Created",
          { id: "other" },
          { id: event.id },
        );
        await again.exec();
        expect(
          JSON.parse((await client.hget(`${prefix}e:${event.id}`, "data"))!),
        ).toEqual({ id: "u1" });
      } finally {
        await client.del(...(await client.keys(`${prefix}*`)));
        await client.quit();
      }
    });
  });
});
