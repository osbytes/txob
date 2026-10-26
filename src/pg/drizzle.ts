import { sql, type SQL } from "drizzle-orm";
import {
  check,
  index,
  integer,
  jsonb,
  pgSchema,
  pgTable,
  primaryKey,
  text,
  timestamp,
  uuid,
} from "drizzle-orm/pg-core";
import { splitPlaceholders } from "./params.js";
import type { PgTableOpts } from "./schema.js";
import type { PgAdapter } from "./store.js";

type DrizzleDatabase = {
  execute(query: SQL): PromiseLike<unknown>;
  transaction(fn: (tx: any) => Promise<any>, config?: any): Promise<any>;
};

/** The transaction type `db.transaction` passes to its callback. */
export type DrizzleTransactionOf<TDb extends DrizzleDatabase> = Parameters<
  Parameters<TDb["transaction"]>[0]
>[0];

const toSql = (text: string, values: unknown[]): SQL => {
  const { chunks, indexes } = splitPlaceholders(text);
  const parts: SQL[] = [sql.raw(chunks[0])];
  indexes.forEach((index, i) => {
    parts.push(sql`${sql.param(values[index])}`, sql.raw(chunks[i + 1]));
  });
  return sql.join(parts);
};

// node-postgres, Neon and PGlite return a result object; postgres.js returns the rows
const rowsOf = (result: unknown): any[] =>
  Array.isArray(result) ? result : (result as { rows: any[] }).rows;

/**
 * Runs txob's SQL through a Drizzle Postgres database (node-postgres, postgres.js, Neon,
 * PGlite, ...). Publish with the `tx` from `db.transaction`.
 */
export const drizzleAdapter = <TDb extends DrizzleDatabase>(
  db: TDb,
): PgAdapter<DrizzleTransactionOf<TDb>> => ({
  query: async (text, values) => rowsOf(await db.execute(toSql(text, values))),
  queryIn: async (tx, text, values) =>
    rowsOf(await tx.execute(toSql(text, values))),
  transaction: (fn) => db.transaction(fn),
});

/**
 * txob's tables as Drizzle definitions, so Drizzle Kit includes them in the migrations it
 * generates. Export the result from your schema file:
 *
 *   export const { txobEvents, txobDeliveries, txobRateLimitClaims } = txobTables();
 *
 * Drizzle Kit can't create triggers, so add `getWakeupTriggerStatements()` with a custom
 * migration (`drizzle-kit generate --custom`) if you use a wakeup emitter.
 */
export const txobTables = (opts: PgTableOpts = {}) => {
  const prefix = opts.tablePrefix ?? "txob_";
  const table = (
    opts.schema ? pgSchema(opts.schema).table : pgTable
  ) as typeof pgTable;
  const timestamptz = (name: string) => timestamp(name, { withTimezone: true });

  const txobEvents = table(
    `${prefix}events`,
    {
      id: text("id").primaryKey(),
      type: text("type").notNull(),
      data: jsonb("data").notNull(),
      correlation_id: text("correlation_id"),
      created_at: timestamptz("created_at").notNull().defaultNow(),
      fanned_out_at: timestamptz("fanned_out_at"),
      fanout_attempts: integer("fanout_attempts").notNull().default(0),
      fanout_run_after: timestamptz("fanout_run_after").notNull().defaultNow(),
      fanout_lease_token: uuid("fanout_lease_token"),
    },
    (t) => [
      index(`${prefix}events_fanout_idx`)
        .on(t.created_at)
        .where(sql`fanned_out_at IS NULL`),
    ],
  );

  const txobDeliveries = table(
    `${prefix}deliveries`,
    {
      event_id: text("event_id")
        .notNull()
        .references(() => txobEvents.id, { onDelete: "cascade" }),
      event_type: text("event_type").notNull(),
      handler: text("handler").notNull(),
      status: text("status").notNull().default("pending"),
      attempts: integer("attempts").notNull().default(0),
      priority: integer("priority").notNull().default(0),
      concurrency_key: text("concurrency_key"),
      run_after: timestamptz("run_after").notNull().defaultNow(),
      lease_token: uuid("lease_token"),
      last_error: jsonb("last_error"),
      created_at: timestamptz("created_at").notNull().defaultNow(),
      completed_at: timestamptz("completed_at"),
    },
    (t) => [
      primaryKey({ columns: [t.event_id, t.handler] }),
      check(
        `${prefix}deliveries_status_check`,
        sql`status IN ('pending', 'completed', 'dead')`,
      ),
      index(`${prefix}deliveries_claim_idx`)
        .on(
          t.event_type,
          t.handler,
          t.priority.desc().nullsFirst(),
          t.run_after,
        )
        .where(sql`status = 'pending'`),
      index(`${prefix}deliveries_leased_idx`)
        .on(t.event_type, t.handler, t.concurrency_key)
        .where(sql`status = 'pending' AND lease_token IS NOT NULL`),
    ],
  );

  const txobRateLimitClaims = table(
    `${prefix}rate_limit_claims`,
    {
      event_type: text("event_type").notNull(),
      handler: text("handler").notNull(),
      claimed_at: timestamptz("claimed_at").notNull().defaultNow(),
    },
    (t) => [
      index(`${prefix}rate_limit_claims_idx`).on(
        t.event_type,
        t.handler,
        t.claimed_at,
      ),
    ],
  );

  return { txobEvents, txobDeliveries, txobRateLimitClaims };
};
