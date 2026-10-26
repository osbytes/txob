import {
  CompiledQuery,
  sql,
  type ColumnType,
  type Generated,
  type Kysely,
  type Transaction,
} from "kysely";
import {
  getDropStatements,
  getTableStatements,
  getWakeupTriggerStatements,
  type PgMigrationOpts,
} from "./schema.js";
import type { PgAdapter } from "./store.js";

/**
 * Runs txob's SQL through Kysely with a Postgres dialect. Publish with the `trx` from
 * `db.transaction().execute(...)`.
 */
export const kyselyAdapter = <DB>(
  db: Kysely<DB>,
): PgAdapter<Transaction<DB>> => ({
  query: async (text, values) =>
    (await db.executeQuery(CompiledQuery.raw(text, values))).rows as any[],
  queryIn: async (trx, text, values) =>
    (await trx.executeQuery(CompiledQuery.raw(text, values))).rows as any[],
  transaction: (fn) => db.transaction().execute(fn),
});

/**
 * A Kysely migration that creates txob's tables, indexes and wakeup triggers:
 *
 *   // migrations/0001_txob.ts
 *   export const { up, down } = txobMigration();
 */
export const txobMigration = (
  opts: PgMigrationOpts = {},
): {
  up(db: Kysely<any>): Promise<void>;
  down(db: Kysely<any>): Promise<void>;
} => ({
  up: async (db: Kysely<any>) => {
    for (const statement of [
      ...getTableStatements(opts),
      ...getWakeupTriggerStatements(opts),
    ]) {
      await sql.raw(statement).execute(db);
    }
  },
  down: async (db: Kysely<any>) => {
    for (const statement of getDropStatements(opts)) {
      await sql.raw(statement).execute(db);
    }
  },
});

type Timestamp = ColumnType<Date, Date | string, Date | string>;

/** Row types for txob's tables, if you want to query them with Kysely (e.g. for dashboards). */
export interface TxobEventsTable {
  id: string;
  type: string;
  data: unknown;
  correlation_id: string | null;
  created_at: Generated<Date>;
  fanned_out_at: Timestamp | null;
  fanout_attempts: Generated<number>;
  fanout_run_after: Generated<Date>;
  fanout_lease_token: string | null;
}

export interface TxobDeliveriesTable {
  event_id: string;
  event_type: string;
  handler: string;
  status: Generated<"pending" | "completed" | "dead">;
  attempts: Generated<number>;
  priority: Generated<number>;
  concurrency_key: string | null;
  run_after: Generated<Date>;
  lease_token: string | null;
  last_error: unknown | null;
  created_at: Generated<Date>;
  completed_at: Timestamp | null;
}

export interface TxobRateLimitClaimsTable {
  event_type: string;
  handler: string;
  claimed_at: Generated<Date>;
}

/** txob's tables under their default names; merge into your own `Database` interface. */
export interface TxobDatabase {
  txob_events: TxobEventsTable;
  txob_deliveries: TxobDeliveriesTable;
  txob_rate_limit_claims: TxobRateLimitClaimsTable;
}
