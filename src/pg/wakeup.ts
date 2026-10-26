import { EventEmitter } from "node:events";
import { Client, escapeIdentifier, type ClientConfig } from "pg";
import type { WakeupEmitter } from "../types.js";
import { defaultChannel } from "./schema.js";

export type CreatePgWakeupEmitterOpts = {
  /** Config for the dedicated LISTEN connection. */
  listenClientConfig: ClientConfig;
  /** Must match the `channel` passed to `migrate`. Default: "txob". */
  channel?: string;
};

/**
 * Emits `wakeup` when the NOTIFY triggers created by `migrate` fire, so processors pick up new
 * events and deliveries immediately instead of on the next poll. Uses a dedicated connection
 * because a connection in LISTEN mode can't be shared with a pool.
 *
 * Connection errors are emitted as `error` events if you listen for them; processors keep
 * polling either way. The connection is not re-established automatically.
 */
export const createPgWakeupEmitter = async (
  opts: CreatePgWakeupEmitterOpts,
): Promise<
  WakeupEmitter & { on(event: "error", listener: (err: Error) => void): void }
> => {
  // Postgres folds unquoted channel names to lowercase; the trigger uses the literal name
  const channel = opts.channel ?? defaultChannel;
  const emitter = new EventEmitter();

  const listenClient = new Client(opts.listenClientConfig);
  await listenClient.connect();
  await listenClient.query(`LISTEN ${escapeIdentifier(channel)}`);

  listenClient.on("notification", (msg) => {
    if (msg.channel === channel) {
      emitter.emit("wakeup");
    }
  });
  // EventEmitter throws on an unhandled `error`; a lost LISTEN connection only costs latency
  // (processors keep polling), so errors are reported only to listeners that asked for them
  const emitError = (err: Error) => {
    if (emitter.listenerCount("error") > 0) emitter.emit("error", err);
  };
  listenClient.on("error", emitError);
  listenClient.on("end", () =>
    emitError(new Error("Postgres LISTEN connection ended")),
  );

  return {
    on: (event: "wakeup" | "error", listener: (...args: any[]) => void) => {
      emitter.on(event, listener);
    },
    off: (event: "wakeup", listener: () => void) => {
      emitter.off(event, listener);
    },
    close: async () => {
      listenClient.removeAllListeners("end");
      await listenClient.query(`UNLISTEN ${escapeIdentifier(channel)}`);
      await listenClient.end();
    },
  };
};
