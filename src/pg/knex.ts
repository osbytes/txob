import type { Knex } from "knex";
import { splitPlaceholders, toArrayLiteral } from "./params.js";
import {
  getDropStatements,
  getTableStatements,
  getWakeupTriggerStatements,
  type PgMigrationOpts,
} from "./schema.js";
import type { PgAdapter } from "./store.js";

// Knex uses `?` placeholders and expands array bindings into lists, so arrays are sent as
// Postgres array literals (every array parameter in txob's SQL has an explicit cast)
const toRaw = (text: string, values: unknown[]) => {
  const { chunks, indexes } = splitPlaceholders(text);
  return {
    sql: chunks.map((chunk) => chunk.replace(/\?/g, "\\?")).join("?"),
    bindings: indexes.map((index) => {
      const value = values[index];
      return Array.isArray(value) ? toArrayLiteral(value) : (value ?? null);
    }),
  };
};

const run = async (knex: Knex, text: string, values: unknown[]) => {
  const { sql, bindings } = toRaw(text, values);
  const result = await knex.raw(sql, bindings as Knex.RawBinding[]);
  return result.rows as any[];
};

/**
 * Runs txob's SQL through Knex with the `pg` client. Publish with the `trx` from
 * `knex.transaction(async (trx) => ...)`.
 */
export const knexAdapter = (knex: Knex): PgAdapter<Knex.Transaction> => ({
  query: (text, values) => run(knex, text, values),
  queryIn: (trx, text, values) => run(trx, text, values),
  transaction: (fn) => knex.transaction((trx) => fn(trx)),
});

/**
 * A Knex migration that creates txob's tables, indexes and wakeup triggers:
 *
 *   // migrations/20260101000000_txob.js
 *   export const { up, down } = txobMigration();
 */
export const txobMigration = (opts: PgMigrationOpts = {}) => ({
  up: async (knex: Knex) => {
    for (const statement of [
      ...getTableStatements(opts),
      ...getWakeupTriggerStatements(opts),
    ]) {
      await knex.raw(statement);
    }
  },
  down: async (knex: Knex) => {
    for (const statement of getDropStatements(opts)) await knex.raw(statement);
  },
});
