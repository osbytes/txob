import { randomBytes } from "node:crypto";
import pg from "pg";
import { describe, expect, it } from "vitest";
import {
  createPgStore,
  createPgWakeupEmitter,
  getMigrationSql,
  migrate,
} from "../src/pg/index.js";
import { runStoreContract } from "./contract.js";

/**
 * Integration tests against a real Postgres. Skipped unless TXOB_TEST_PG_URL is set, e.g.
 *   docker run --rm -e POSTGRES_PASSWORD=txob -p 54329:5432 postgres:17-alpine
 *   TXOB_TEST_PG_URL=postgres://postgres:txob@localhost:54329/postgres yarn test
 */
const connectionString = process.env.TXOB_TEST_PG_URL;

describe.skipIf(!connectionString)("postgres", () => {
  const createIsolated = async () => {
    // Unique names per test so tests don't share tables or NOTIFY channels
    const tablePrefix = `t${randomBytes(4).toString("hex")}_`;
    const channel = `${tablePrefix}wakeup`;
    const pool = new pg.Pool({ connectionString, max: 20 });
    await migrate(pool, { tablePrefix, channel });
    return { pool, tablePrefix, channel };
  };

  runStoreContract("postgres", async () => {
    const { pool, tablePrefix, channel } = await createIsolated();
    const store = createPgStore({ pool, tablePrefix });
    return {
      store,
      transaction: async (fn) => {
        const client = await pool.connect();
        try {
          await client.query("BEGIN");
          const result = await fn(client);
          await client.query("COMMIT");
          return result;
        } catch (error) {
          await client.query("ROLLBACK");
          throw error;
        } finally {
          client.release();
        }
      },
      getDeliveries: async () =>
        (await pool.query(`SELECT * FROM ${tablePrefix}deliveries`)).rows,
      getEvents: async () =>
        (
          await pool.query(
            `SELECT id, type, fanned_out_at FROM ${tablePrefix}events`,
          )
        ).rows,
      createWakeupEmitter: () =>
        createPgWakeupEmitter({
          listenClientConfig: { connectionString },
          channel,
        }),
      teardown: async () => {
        await pool.query(
          `DROP TABLE ${tablePrefix}deliveries, ${tablePrefix}events, ${tablePrefix}rate_limit_claims; DROP FUNCTION ${tablePrefix}notify`,
        );
        await pool.end();
      },
    };
  });

  describe("wakeup emitter", () => {
    it("survives losing its connection and reports it to error listeners", async () => {
      const channel = `w${randomBytes(4).toString("hex")}`;
      const pool = new pg.Pool({ connectionString });
      const quiet = await createPgWakeupEmitter({
        listenClientConfig: { connectionString },
        channel,
      });
      const watched = await createPgWakeupEmitter({
        listenClientConfig: { connectionString },
        channel,
      });
      const errors: Error[] = [];
      watched.on("error", (err) => errors.push(err));
      try {
        await pool.query(
          `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query = $1`,
          [`LISTEN "${channel}"`],
        );
        await new Promise((resolve) => setTimeout(resolve, 200));
        expect(errors.length).toBeGreaterThan(0);
      } finally {
        await pool.end();
      }
      void quiet;
    });
  });

  describe("migrate", () => {
    it("is idempotent and safe to run concurrently", async () => {
      const { pool, tablePrefix } = await createIsolated();
      try {
        await Promise.all([
          migrate(pool, { tablePrefix }),
          migrate(pool, { tablePrefix }),
          migrate(pool, { tablePrefix }),
        ]);
        const tables = await pool.query(
          `SELECT tablename FROM pg_tables WHERE tablename LIKE $1 ORDER BY tablename`,
          [`${tablePrefix}%`],
        );
        expect(tables.rows.map((r) => r.tablename)).toEqual([
          `${tablePrefix}deliveries`,
          `${tablePrefix}events`,
          `${tablePrefix}rate_limit_claims`,
        ]);
      } finally {
        await pool.query(
          `DROP TABLE ${tablePrefix}deliveries, ${tablePrefix}events, ${tablePrefix}rate_limit_claims; DROP FUNCTION ${tablePrefix}notify`,
        );
        await pool.end();
      }
    });

    it("supports a custom schema", async () => {
      const pool = new pg.Pool({ connectionString });
      const schemaName = `s${randomBytes(4).toString("hex")}`;
      try {
        await pool.query(`CREATE SCHEMA ${schemaName}`);
        await migrate(pool, { schema: schemaName });
        const tables = await pool.query(
          `SELECT tablename FROM pg_tables WHERE schemaname = $1 ORDER BY tablename`,
          [schemaName],
        );
        expect(tables.rows.map((r) => r.tablename)).toEqual([
          "txob_deliveries",
          "txob_events",
          "txob_rate_limit_claims",
        ]);
      } finally {
        await pool.query(`DROP SCHEMA ${schemaName} CASCADE`);
        await pool.end();
      }
    });

    it("escapes identifiers in generated SQL", () => {
      const sql = getMigrationSql({
        schema: 'we"ird',
        tablePrefix: "p_",
        channel: "c'h",
      });
      expect(sql).toContain('"we""ird"."p_events"');
      expect(sql).toContain("pg_notify('c''h'");
    });
  });
});
