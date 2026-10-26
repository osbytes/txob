import { EventEmitter } from "node:events";
import type { Sql, TransactionSql } from "postgres";
import type { WakeupEmitter } from "../types.js";
import { defaultChannel } from "./schema.js";
import type { PgAdapter } from "./store.js";

/**
 * Runs txob's SQL through postgres.js. Publish with the `sql` that `sql.begin` passes to its
 * callback.
 */
export const postgresAdapter = <TTypes extends Record<string, unknown>>(
  sql: Sql<TTypes>,
): PgAdapter<TransactionSql<TTypes>> => ({
  query: async (text, values) => [...(await sql.unsafe(text, values as any[]))],
  queryIn: async (tx, text, values) => [
    ...(await tx.unsafe(text, values as any[])),
  ],
  transaction: async (fn) => (await sql.begin((tx) => fn(tx))) as any,
});

/**
 * Emits `wakeup` when the NOTIFY triggers created by `migrate` fire, using postgres.js's own
 * LISTEN support (which reconnects automatically).
 */
export const createPostgresWakeupEmitter = async (
  sql: Sql<any>,
  opts: { channel?: string } = {},
): Promise<WakeupEmitter> => {
  const emitter = new EventEmitter();
  const { unlisten } = await sql.listen(
    opts.channel ?? defaultChannel,
    () => void emitter.emit("wakeup"),
  );
  return {
    on: (event, listener) => void emitter.on(event, listener),
    off: (event, listener) => void emitter.off(event, listener),
    close: async () => {
      emitter.removeAllListeners();
      await unlisten();
    },
  };
};
