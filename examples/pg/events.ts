import { z } from "zod";
import { createOutbox } from "../../src/index.js";
import { createPgStore } from "../../src/pg/index.js";
import pg from "pg";
import dotenv from "dotenv";
dotenv.config();

export const eventSchemas = {
  ResourceSaved: z.object({
    type: z.literal("activity"),
    id: z.uuid(),
    accountId: z.string(),
  }),
  DeliveryDead: z.object({
    eventId: z.string(),
    eventType: z.string(),
    handler: z.string(),
    error: z.string(),
  }),
};

export const pool = new pg.Pool({
  user: process.env.POSTGRES_USER || "outbox",
  password: process.env.POSTGRES_PASSWORD || "outbox",
  database: process.env.POSTGRES_DB || "outbox",
  port: parseInt(process.env.POSTGRES_PORT || "5434"),
  max: 30,
});

export const store = createPgStore({ pool });

export const outbox = createOutbox({ eventSchemas, store });
