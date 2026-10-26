import type {
  DataSource,
  EntityManager,
  MigrationInterface,
  QueryRunner,
} from "typeorm";
import {
  getDropStatements,
  getTableStatements,
  getWakeupTriggerStatements,
  type PgMigrationOpts,
} from "./schema.js";
import type { PgAdapter } from "./store.js";

const run = async (runner: QueryRunner, text: string, values: unknown[]) =>
  (await runner.query(text, values as any[], true)).records as any[];

/**
 * Runs txob's SQL through a TypeORM Postgres `DataSource`. Publish with the `EntityManager`
 * from `dataSource.transaction(async (manager) => ...)`.
 */
export const typeormAdapter = (
  dataSource: DataSource,
): PgAdapter<EntityManager> => ({
  query: async (text, values) => {
    const runner = dataSource.createQueryRunner();
    try {
      return await run(runner, text, values);
    } finally {
      await runner.release();
    }
  },
  queryIn: (manager, text, values) => {
    if (!manager.queryRunner?.isTransactionActive) {
      throw new Error(
        "txob: pass the EntityManager from dataSource.transaction(...)",
      );
    }
    return run(manager.queryRunner, text, values);
  },
  transaction: (fn) => dataSource.transaction((manager) => fn(manager)),
});

/**
 * A TypeORM migration class that creates txob's tables, indexes and wakeup triggers. Add it to
 * your DataSource's `migrations`:
 *
 *   migrations: [txobMigration(), ...]
 */
export const txobMigration = (
  opts: PgMigrationOpts = {},
): new () => MigrationInterface =>
  class TxobInit1761436800000 implements MigrationInterface {
    name = "TxobInit1761436800000";

    async up(queryRunner: QueryRunner) {
      for (const statement of [
        ...getTableStatements(opts),
        ...getWakeupTriggerStatements(opts),
      ]) {
        await queryRunner.query(statement);
      }
    }

    async down(queryRunner: QueryRunner) {
      for (const statement of getDropStatements(opts)) {
        await queryRunner.query(statement);
      }
    }
  };
