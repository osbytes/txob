import { escapeIdentifier, escapeLiteral } from "pg";
import type { PgQuerier } from "./store.js";

export type PgTableOpts = {
  /** Postgres schema for txob's tables. Default: unqualified (uses `search_path`). */
  schema?: string;
  /** Prefix for table, index, function and trigger names. Default: "txob_". */
  tablePrefix?: string;
};

export type PgMigrationOpts = PgTableOpts & {
  /** NOTIFY channel used to wake processors when events or deliveries are inserted. Default: "txob". */
  channel?: string;
};

export const defaultChannel = "txob";

export const resolveNames = (opts: PgTableOpts = {}) => {
  const prefix = opts.tablePrefix ?? "txob_";
  const qualify = (name: string) =>
    opts.schema
      ? `${escapeIdentifier(opts.schema)}.${escapeIdentifier(name)}`
      : escapeIdentifier(name);
  return {
    prefix,
    qualify,
    events: qualify(`${prefix}events`),
    deliveries: qualify(`${prefix}deliveries`),
    rateLimitClaims: qualify(`${prefix}rate_limit_claims`),
  };
};

/**
 * Returns the DDL for txob's tables, indexes and wakeup triggers. Use this to add txob to
 * your own migration tooling (Prisma, Drizzle Kit, Knex, node-pg-migrate, ...), or call
 * `migrate` to run it directly. Requires Postgres 14+.
 */
export const getMigrationSql = (opts: PgMigrationOpts = {}): string => {
  const { prefix, qualify, events, deliveries, rateLimitClaims } =
    resolveNames(opts);
  const channel = opts.channel ?? defaultChannel;
  const notifyFn = qualify(`${prefix}notify`);

  return `
CREATE TABLE IF NOT EXISTS ${events} (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  data JSONB NOT NULL,
  correlation_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  fanned_out_at TIMESTAMPTZ,
  fanout_attempts INTEGER NOT NULL DEFAULT 0,
  -- when the event may next be fanned out; doubles as the fan-out lease expiry
  fanout_run_after TIMESTAMPTZ NOT NULL DEFAULT now(),
  fanout_lease_token UUID
);

CREATE INDEX IF NOT EXISTS ${escapeIdentifier(`${prefix}events_fanout_idx`)}
  ON ${events} (created_at) WHERE fanned_out_at IS NULL;

CREATE TABLE IF NOT EXISTS ${deliveries} (
  event_id TEXT NOT NULL REFERENCES ${events} (id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  handler TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'completed', 'dead')),
  attempts INTEGER NOT NULL DEFAULT 0,
  priority INTEGER NOT NULL DEFAULT 0,
  concurrency_key TEXT,
  -- when the delivery may next be attempted; doubles as the lease expiry while claimed
  run_after TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_token UUID,
  last_error JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ,
  PRIMARY KEY (event_id, handler)
);

CREATE INDEX IF NOT EXISTS ${escapeIdentifier(`${prefix}deliveries_claim_idx`)}
  ON ${deliveries} (event_type, handler, priority DESC, run_after) WHERE status = 'pending';

-- deliveries with a lease, counted for cross-processor concurrency limits
CREATE INDEX IF NOT EXISTS ${escapeIdentifier(`${prefix}deliveries_leased_idx`)}
  ON ${deliveries} (event_type, handler, concurrency_key)
  WHERE status = 'pending' AND lease_token IS NOT NULL;

-- claim times of rate-limited handlers (sliding window log)
CREATE TABLE IF NOT EXISTS ${rateLimitClaims} (
  event_type TEXT NOT NULL,
  handler TEXT NOT NULL,
  claimed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ${escapeIdentifier(`${prefix}rate_limit_claims_idx`)}
  ON ${rateLimitClaims} (event_type, handler, claimed_at);

CREATE OR REPLACE FUNCTION ${notifyFn}() RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_notify(${escapeLiteral(channel)}, TG_TABLE_NAME);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER ${escapeIdentifier(`${prefix}events_notify`)}
  AFTER INSERT ON ${events} FOR EACH STATEMENT EXECUTE FUNCTION ${notifyFn}();

CREATE OR REPLACE TRIGGER ${escapeIdentifier(`${prefix}deliveries_notify`)}
  AFTER INSERT ON ${deliveries} FOR EACH STATEMENT EXECUTE FUNCTION ${notifyFn}();
`;
};

/**
 * Creates txob's tables, indexes and wakeup triggers if they don't exist.
 * Serialized with an advisory lock so concurrent processes can call it at startup.
 */
export const migrate = async (
  querier: PgQuerier,
  opts: PgMigrationOpts = {},
): Promise<void> => {
  // A multi-statement query without parameters runs in a single implicit transaction,
  // so the transaction-scoped advisory lock is held until all DDL is applied.
  await querier.query(
    `SELECT pg_advisory_xact_lock(hashtext('txob_migrate'));\n${getMigrationSql(opts)}`,
  );
};
