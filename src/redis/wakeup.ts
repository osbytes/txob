import { EventEmitter } from "node:events";
import type { WakeupEmitter } from "../types.js";
import { createRedisDriver, type RedisClientLike } from "./driver.js";
import { defaultPrefix, type RedisKeyOpts } from "./store.js";

/**
 * Emits `wakeup` when events or deliveries are written, using Redis pub/sub on a dedicated
 * connection, so processors pick up work immediately instead of on the next poll.
 *
 * Connection problems are emitted as `error` events if you listen for them; processors keep
 * polling either way.
 */
export const createRedisWakeupEmitter = async (
  opts: RedisKeyOpts & { client: RedisClientLike },
): Promise<
  WakeupEmitter & { on(event: "error", listener: (err: Error) => void): void }
> => {
  const emitter = new EventEmitter();
  const conn = await createRedisDriver(opts.client).connect();
  conn.onError((error) => {
    if (emitter.listenerCount("error") > 0) emitter.emit("error", error);
  });
  await conn.subscribe(`${opts.prefix ?? defaultPrefix}wakeup`, () =>
    emitter.emit("wakeup"),
  );

  return {
    on: (event: "wakeup" | "error", listener: (...args: any[]) => void) => {
      emitter.on(event, listener);
    },
    off: (event: "wakeup", listener: () => void) => {
      emitter.off(event, listener);
    },
    close: () => conn.close(),
  };
};
