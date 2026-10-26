// Compile-time checks (run by `yarn typecheck`, never executed): each adapter infers the
// ORM's own transaction type, so publish accepts it and rejects anything else.
import { drizzle } from "drizzle-orm/node-postgres";
import knex from "knex";
import { Kysely } from "kysely";
import postgres from "postgres";
import { Sequelize } from "sequelize";
import { DataSource } from "typeorm";
import { createOutbox } from "../src/index.js";
import { createPgStore } from "../src/pg/index.js";
import { drizzleAdapter } from "../src/pg/drizzle.js";
import { knexAdapter } from "../src/pg/knex.js";
import { kyselyAdapter } from "../src/pg/kysely.js";
import { postgresAdapter } from "../src/pg/postgres.js";
import { prismaAdapter } from "../src/pg/prisma.js";
import { sequelizeAdapter } from "../src/pg/sequelize.js";
import { typeormAdapter } from "../src/pg/typeorm.js";
import { schema } from "./helpers.js";
import { PrismaClient } from "./prisma/generated/client.js";

const eventSchemas = { Ping: schema<{ n: number }>() };

export const checks = async () => {
  const db = drizzle("postgres://");
  const drizzleOutbox = createOutbox({
    eventSchemas,
    store: createPgStore({ adapter: drizzleAdapter(db) }),
  });
  await db.transaction(async (tx) => {
    await drizzleOutbox.publish(tx, "Ping", { n: 1 });
  });
  // @ts-expect-error not a Drizzle transaction
  await drizzleOutbox.publish("tx", "Ping", { n: 1 });

  const prisma = new PrismaClient({} as any);
  const prismaOutbox = createOutbox({
    eventSchemas,
    store: createPgStore({ adapter: prismaAdapter(prisma) }),
  });
  await prisma.$transaction(async (tx) => {
    await tx.note.create({ data: { body: "x" } });
    await prismaOutbox.publish(tx, "Ping", { n: 1 });
  });
  // @ts-expect-error not a Prisma transaction
  await prismaOutbox.publish({}, "Ping", { n: 1 });

  const kdb = new Kysely<{ notes: { body: string } }>({} as any);
  const kyselyOutbox = createOutbox({
    eventSchemas,
    store: createPgStore({ adapter: kyselyAdapter(kdb) }),
  });
  await kdb.transaction().execute(async (trx) => {
    await trx.insertInto("notes").values({ body: "x" }).execute();
    await kyselyOutbox.publish(trx, "Ping", { n: 1 });
  });
  // @ts-expect-error a Kysely instance is not a transaction
  await kyselyOutbox.publish(kdb, "Ping", { n: 1 });

  const sql = postgres();
  const postgresOutbox = createOutbox({
    eventSchemas,
    store: createPgStore({ adapter: postgresAdapter(sql) }),
  });
  await sql.begin(async (tx) => {
    await postgresOutbox.publish(tx, "Ping", { n: 1 });
  });

  const sequelize = new Sequelize("postgres://");
  const sequelizeOutbox = createOutbox({
    eventSchemas,
    store: createPgStore({ adapter: sequelizeAdapter(sequelize) }),
  });
  await sequelize.transaction(async (t) => {
    await sequelizeOutbox.publish(t, "Ping", { n: 1 });
  });

  const k = knex({ client: "pg" });
  const knexOutbox = createOutbox({
    eventSchemas,
    store: createPgStore({ adapter: knexAdapter(k) }),
  });
  await k.transaction(async (trx) => {
    await knexOutbox.publish(trx, "Ping", { n: 1 });
  });

  const dataSource = new DataSource({ type: "postgres" });
  const typeormOutbox = createOutbox({
    eventSchemas,
    store: createPgStore({ adapter: typeormAdapter(dataSource) }),
  });
  await dataSource.transaction(async (manager) => {
    await typeormOutbox.publish(manager, "Ping", { n: 1 });
  });
  // @ts-expect-error not an EntityManager
  await typeormOutbox.publish(dataSource, "Ping", { n: 1 });
};
