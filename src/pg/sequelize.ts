import type { QueryInterface, Sequelize, Transaction } from "sequelize";
import {
  getDropStatements,
  getTableStatements,
  getWakeupTriggerStatements,
  type PgMigrationOpts,
} from "./schema.js";
import type { PgAdapter } from "./store.js";

const run = (
  sequelize: Sequelize,
  text: string,
  values: unknown[],
  transaction?: Transaction,
) =>
  sequelize.query(text, {
    // Sequelize rewrites `$$` when binding, so only bind when there are parameters
    ...(values.length > 0 ? { bind: values as any[] } : {}),
    transaction,
    type: "SELECT" as any,
    raw: true,
  }) as Promise<any[]>;

/**
 * Runs txob's SQL through Sequelize on Postgres. Publish with the `Transaction` from a
 * managed `sequelize.transaction(async (t) => ...)`.
 */
export const sequelizeAdapter = (
  sequelize: Sequelize,
): PgAdapter<Transaction> => ({
  query: (text, values) => run(sequelize, text, values),
  queryIn: (t, text, values) => run(sequelize, text, values, t),
  transaction: (fn) => sequelize.transaction((t) => fn(t)),
});

// sequelize-cli passes the QueryInterface; Umzug passes `{ context }`
type MigrationArg = QueryInterface | { context: QueryInterface };
const queryInterfaceOf = (arg: MigrationArg) =>
  "context" in arg ? arg.context : arg;

/**
 * A Sequelize migration (sequelize-cli or Umzug) that creates txob's tables, indexes and
 * wakeup triggers:
 *
 *   // migrations/20260101000000-txob.js
 *   module.exports = txobMigration();
 */
export const txobMigration = (opts: PgMigrationOpts = {}) => ({
  up: async (arg: MigrationArg) => {
    const queryInterface = queryInterfaceOf(arg);
    for (const statement of [
      ...getTableStatements(opts),
      ...getWakeupTriggerStatements(opts),
    ]) {
      await queryInterface.sequelize.query(statement);
    }
  },
  down: async (arg: MigrationArg) => {
    const queryInterface = queryInterfaceOf(arg);
    for (const statement of getDropStatements(opts)) {
      await queryInterface.sequelize.query(statement);
    }
  },
});
