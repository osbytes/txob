import { EventEmitter } from "node:events";
import type { ChangeStream } from "mongodb";
import type { WakeupEmitter } from "../types.js";
import { resolveCollections, type CreateMongoStoreOpts } from "./store.js";

/**
 * Emits `wakeup` when events or deliveries are inserted, using a change stream, so processors
 * pick up work immediately instead of on the next poll. Requires a replica set or sharded
 * cluster.
 *
 * Change stream errors are emitted as `error` events if you listen for them; processors keep
 * polling either way. The stream is not re-opened automatically.
 */
export const createMongoWakeupEmitter = async (
  opts: CreateMongoStoreOpts,
): Promise<
  WakeupEmitter & { on(event: "error", listener: (err: Error) => void): void }
> => {
  const { names } = resolveCollections(opts);
  const emitter = new EventEmitter();

  const changeStream: ChangeStream = opts.client.db(opts.db).watch([
    {
      $match: {
        operationType: "insert",
        "ns.coll": { $in: [names.events, names.deliveries] },
      },
    },
  ]);

  // EventEmitter throws on an unhandled `error`; a lost change stream only costs latency
  const emitError = (err: Error) => {
    if (emitter.listenerCount("error") > 0) emitter.emit("error", err);
  };
  let closing = false;
  changeStream.on("change", () => emitter.emit("wakeup"));
  changeStream.on("error", emitError);
  changeStream.on("close", () => {
    if (!closing) emitError(new Error("MongoDB change stream closed"));
  });

  return {
    on: (event: "wakeup" | "error", listener: (...args: any[]) => void) => {
      emitter.on(event, listener);
    },
    off: (event: "wakeup", listener: () => void) => {
      emitter.off(event, listener);
    },
    close: async () => {
      closing = true;
      await changeStream.close();
    },
  };
};
