import type { TxOBFanoutEvent, TxOBStore } from "../store.js";
import type { TxOBDelivery, TxOBEvent } from "../types.js";
import { resolveNames, type PgTableOpts } from "./schema.js";

/**
 * Anything that can run a query: a `pg.Client`, `pg.PoolClient`, `pg.Pool`, or a
 * pg-compatible driver. Typed structurally so any version of `pg` (or its types) fits.
 */
export interface PgQuerier {
  query(
    text: string,
    values?: any[],
  ): Promise<{ rows: any[]; rowCount: number | null }>;
}

export interface PgPoolClient extends PgQuerier {
  release(err?: Error | boolean): void;
}

export interface PgPool extends PgQuerier {
  connect(): Promise<PgPoolClient>;
}

export type CreatePgStoreOpts = PgTableOpts & {
  /**
   * Used for claims and updates. Transactions (only needed when `onDeliveryDead` is set)
   * check out a dedicated connection.
   */
  pool: PgPool;
};

type DeliveryRow = {
  event_id: string;
  event_type: string;
  handler: string;
  attempts: number;
  priority: number;
  concurrency_key: string | null;
  lease_token: string;
  type: string;
  data: TxOBEvent["data"];
  correlation_id: string | null;
  event_created_at: Date;
};

const toDelivery = (row: DeliveryRow): TxOBDelivery => ({
  event_id: row.event_id,
  event_type: row.event_type,
  handler: row.handler,
  attempts: row.attempts,
  priority: row.priority,
  concurrency_key: row.concurrency_key,
  lease_token: row.lease_token,
  event: {
    id: row.event_id,
    type: row.type,
    data: row.data,
    correlation_id: row.correlation_id,
    created_at: row.event_created_at,
  },
});

const ms = (param: string) => `(${param}::float8 * interval '1 millisecond')`;

/**
 * Postgres store. Run `migrate` (or apply `getMigrationSql`) before use.
 *
 * Publish events with `outbox.publish(tx, ...)` where `tx` is the connection your
 * business transaction runs on (e.g. a `pg.PoolClient` after `BEGIN`).
 */
export const createPgStore = (
  opts: CreatePgStoreOpts,
): TxOBStore<PgQuerier> => {
  const { pool } = opts;
  const { events, deliveries, rateLimitClaims } = resolveNames(opts);

  const leaseKeys = (items: TxOBDelivery[]) => [
    items.map((d) => d.event_id),
    items.map((d) => d.handler),
    items.map((d) => d.lease_token),
  ];

  // Matches deliveries currently leased by any processor
  const live = (alias: string) =>
    `${alias}.status = 'pending' AND ${alias}.lease_token IS NOT NULL AND ${alias}.run_after > now()`;

  const lockNamespace = `txob:${deliveries}`;

  const claimSql = `
WITH slots AS (
  SELECT * FROM unnest($1::text[], $2::text[], $3::int[], $4::int[], $5::int[], $6::int[], $7::float8[])
    AS s (event_type, handler, slot_limit, concurrency, key_limit, rate_max, rate_ms)
), budgets AS (
  SELECT s.*, GREATEST(LEAST(
    s.slot_limit,
    s.concurrency - (
      SELECT count(*) FROM ${deliveries} a
      WHERE a.event_type = s.event_type AND a.handler = s.handler AND ${live("a")}
    ),
    s.rate_max - (
      SELECT count(*) FROM ${rateLimitClaims} r
      WHERE r.event_type = s.event_type AND r.handler = s.handler
        AND r.claimed_at > now() - ${ms("s.rate_ms")}
    )
  ), 0) AS budget
  FROM slots s
), candidates AS (
  SELECT c.event_id, c.handler, c.priority, c.run_after, c.created_at FROM budgets b
  CROSS JOIN LATERAL (
    SELECT w.* FROM (
      SELECT p.*, row_number() OVER (
        PARTITION BY p.concurrency_key ORDER BY p.priority DESC, p.run_after, p.created_at
      ) AS key_rank
      FROM (
        SELECT d.event_id, d.handler, d.priority, d.run_after, d.created_at, d.concurrency_key
        FROM ${deliveries} d
        WHERE d.status = 'pending' AND d.event_type = b.event_type AND d.handler = b.handler
          AND d.run_after <= now()
          -- skip keys already at their limit so they can't crowd out other keys
          AND (b.key_limit IS NULL OR d.concurrency_key IS NULL OR d.concurrency_key NOT IN (
            SELECT a.concurrency_key FROM ${deliveries} a
            WHERE a.event_type = b.event_type AND a.handler = b.handler
              AND a.concurrency_key IS NOT NULL AND ${live("a")}
            GROUP BY a.concurrency_key HAVING count(*) >= b.key_limit
          ))
        ORDER BY d.priority DESC, d.run_after, d.created_at
        -- with a per-key limit some candidates are dropped below, so look further ahead
        LIMIT CASE WHEN b.key_limit IS NULL THEN b.budget ELSE b.budget * 10 END
        FOR UPDATE SKIP LOCKED
      ) p
    ) w
    WHERE b.key_limit IS NULL OR w.concurrency_key IS NULL OR w.key_rank + (
      SELECT count(*) FROM ${deliveries} a
      WHERE a.event_type = b.event_type AND a.handler = b.handler
        AND a.concurrency_key = w.concurrency_key AND ${live("a")}
    ) <= b.key_limit
    ORDER BY w.priority DESC, w.run_after, w.created_at
    LIMIT b.budget
  ) c
  ORDER BY c.priority DESC, c.run_after, c.created_at
  LIMIT $8
), claimed AS (
  UPDATE ${deliveries} d SET
    run_after = now() + ${ms("$9")},
    lease_token = gen_random_uuid(),
    attempts = d.attempts + 1
  FROM candidates c
  WHERE d.event_id = c.event_id AND d.handler = c.handler
  RETURNING d.event_id, d.event_type, d.handler, d.attempts, d.priority, d.concurrency_key, d.lease_token
), rate_limit_log AS (
  INSERT INTO ${rateLimitClaims} (event_type, handler)
  SELECT c.event_type, c.handler FROM claimed c
  JOIN slots s ON s.event_type = c.event_type AND s.handler = c.handler
  WHERE s.rate_max IS NOT NULL
), rate_limit_prune AS (
  DELETE FROM ${rateLimitClaims} r USING slots s
  WHERE s.rate_max IS NOT NULL AND r.event_type = s.event_type AND r.handler = s.handler
    AND r.claimed_at <= now() - ${ms("s.rate_ms")}
)
SELECT claimed.*, e.type, e.data, e.correlation_id, e.created_at AS event_created_at
FROM claimed JOIN ${events} e ON e.id = claimed.event_id
ORDER BY claimed.priority DESC`;

  const failSql = `
UPDATE ${deliveries} SET
  status = CASE WHEN $4::timestamptz IS NULL THEN 'dead' ELSE 'pending' END,
  run_after = COALESCE($4::timestamptz, run_after),
  completed_at = CASE WHEN $4::timestamptz IS NULL THEN now() END,
  lease_token = NULL,
  last_error = $5::jsonb
WHERE event_id = $1 AND handler = $2 AND lease_token = $3::uuid`;

  return {
    insertEvent: async (tx, event) => {
      await tx.query(
        `INSERT INTO ${events} (id, type, data, correlation_id, created_at) VALUES ($1, $2, $3, $4, $5)`,
        [
          event.id,
          event.type,
          JSON.stringify(event.data),
          event.correlation_id,
          event.created_at,
        ],
      );
    },

    claimEventsForFanout: async ({ types, limit, leaseMs }) => {
      const result = await pool.query(
        `
UPDATE ${events} e SET
  fanout_run_after = now() + ${ms("$3")},
  fanout_lease_token = gen_random_uuid(),
  fanout_attempts = e.fanout_attempts + 1
FROM (
  SELECT id FROM ${events}
  WHERE fanned_out_at IS NULL AND fanout_run_after <= now() AND type = ANY($1::text[])
  ORDER BY created_at
  LIMIT $2
  FOR UPDATE SKIP LOCKED
) claimable
WHERE e.id = claimable.id
RETURNING e.id, e.type, e.data, e.correlation_id, e.created_at, e.fanout_attempts, e.fanout_lease_token`,
        [types, limit, leaseMs],
      );
      return result.rows as TxOBFanoutEvent[];
    },

    completeFanout: async ({ event_ids, deliveries: newDeliveries }) => {
      // Single statement, so delivery creation and marking events fanned out are atomic.
      // Re-running fan-out for an event is harmless: existing deliveries are kept.
      await pool.query(
        `
WITH inserted AS (
  INSERT INTO ${deliveries} (event_id, event_type, handler, priority, concurrency_key)
  SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::int[], $5::text[])
  ON CONFLICT (event_id, handler) DO NOTHING
)
UPDATE ${events} SET fanned_out_at = now(), fanout_lease_token = NULL
WHERE id = ANY($6::text[])`,
        [
          newDeliveries.map((d) => d.event_id),
          newDeliveries.map((d) => d.event_type),
          newDeliveries.map((d) => d.handler),
          newDeliveries.map((d) => d.priority),
          newDeliveries.map((d) => d.concurrency_key),
          event_ids,
        ],
      );
    },

    claimDeliveries: async ({ slots, limit, leaseMs }) => {
      const params = [
        slots.map((s) => s.event_type),
        slots.map((s) => s.handler),
        slots.map((s) => s.limit),
        slots.map((s) => s.concurrency ?? null),
        slots.map((s) => s.concurrencyPerKey ?? null),
        slots.map((s) => s.rateLimit?.max ?? null),
        slots.map((s) => s.rateLimit?.durationMs ?? null),
        limit,
        leaseMs,
      ];

      const limited = slots.filter(
        (s) =>
          s.concurrency !== undefined ||
          s.concurrencyPerKey !== undefined ||
          s.rateLimit !== undefined,
      );
      if (limited.length === 0) {
        const result = await pool.query(claimSql, params);
        return (result.rows as DeliveryRow[]).map(toDelivery);
      }

      // Limits are checked against deliveries leased by every processor. Claims for a limited
      // handler are serialized with a transaction-scoped advisory lock so two processors can't
      // both see the same free capacity. Locks are taken in a fixed order to avoid deadlocks.
      const client = await pool.connect();
      let releaseErr: Error | undefined;
      try {
        await client.query("BEGIN");
        try {
          await client.query(
            `SELECT pg_advisory_xact_lock(hashtextextended(k, 0)) FROM (SELECT unnest($1::text[]) AS k ORDER BY 1) keys`,
            [
              limited.map(
                (s) => `${lockNamespace}:${s.event_type}:${s.handler}`,
              ),
            ],
          );
          const result = await client.query(claimSql, params);
          await client.query("COMMIT");
          return (result.rows as DeliveryRow[]).map(toDelivery);
        } catch (error) {
          await client.query("ROLLBACK").catch((rollbackError: Error) => {
            releaseErr = rollbackError;
          });
          throw error;
        }
      } finally {
        client.release(releaseErr);
      }
    },

    completeDelivery: async (delivery) => {
      const result = await pool.query(
        `
UPDATE ${deliveries} SET status = 'completed', completed_at = now(), lease_token = NULL
WHERE event_id = $1 AND handler = $2 AND lease_token = $3::uuid`,
        [delivery.event_id, delivery.handler, delivery.lease_token],
      );
      return result.rowCount === 1;
    },

    failDelivery: async (delivery, failure, failOpts) => {
      const params = [
        delivery.event_id,
        delivery.handler,
        delivery.lease_token,
        failure.run_after,
        JSON.stringify(failure.error),
      ];
      const within = failOpts?.within;
      if (!within) {
        const result = await pool.query(failSql, params);
        return result.rowCount === 1;
      }

      const client = await pool.connect();
      let releaseErr: Error | undefined;
      try {
        await client.query("BEGIN");
        try {
          const result = await client.query(failSql, params);
          if (result.rowCount !== 1) {
            await client.query("ROLLBACK");
            return false;
          }
          await within(client);
          await client.query("COMMIT");
          return true;
        } catch (error) {
          await client.query("ROLLBACK").catch((rollbackError: Error) => {
            // Don't return a connection in an unknown transaction state to the pool
            releaseErr = rollbackError;
          });
          throw error;
        }
      } finally {
        client.release(releaseErr);
      }
    },

    extendLeases: async (items, { leaseMs }) => {
      const result = await pool.query(
        `
UPDATE ${deliveries} d SET run_after = now() + ${ms("$4")}
FROM unnest($1::text[], $2::text[], $3::uuid[]) AS l (event_id, handler, lease_token)
WHERE d.event_id = l.event_id AND d.handler = l.handler AND d.lease_token = l.lease_token
RETURNING d.lease_token`,
        [...leaseKeys(items), leaseMs],
      );
      // Match on the token: a stale and a current lease can share the same delivery key
      const extended = new Set(
        result.rows.map((r: { lease_token: string }) => r.lease_token),
      );
      return items.filter((d) => !extended.has(d.lease_token));
    },

    releaseDeliveries: async (items) => {
      if (items.length === 0) return;
      await pool.query(
        `
UPDATE ${deliveries} d SET run_after = now(), lease_token = NULL, attempts = GREATEST(d.attempts - 1, 0)
FROM unnest($1::text[], $2::text[], $3::uuid[]) AS l (event_id, handler, lease_token)
WHERE d.event_id = l.event_id AND d.handler = l.handler AND d.lease_token = l.lease_token`,
        leaseKeys(items),
      );
    },
  };
};
