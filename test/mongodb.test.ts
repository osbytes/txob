import { randomBytes } from "node:crypto";
import { MongoClient } from "mongodb";
import { describe, expect, it } from "vitest";
import {
  createMongoStore,
  createMongoWakeupEmitter,
  migrate,
} from "../src/mongodb/index.js";
import { runStoreContract } from "./contract.js";

/**
 * Integration tests against a real MongoDB replica set. Skipped unless TXOB_TEST_MONGO_URL is
 * set, e.g.
 *   docker run --rm -p 27019:27017 mongo:8 --replSet rs0 --bind_ip_all
 *   docker exec <id> mongosh --eval 'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})'
 *   TXOB_TEST_MONGO_URL="mongodb://localhost:27019/?directConnection=true" yarn test
 */
const url = process.env.TXOB_TEST_MONGO_URL;

describe.skipIf(!url)("mongodb", () => {
  runStoreContract("mongodb", async () => {
    const client = await new MongoClient(url!).connect();
    const db = `txob_test_${randomBytes(4).toString("hex")}`;
    const opts = { client, db };
    await migrate(opts);
    const store = createMongoStore(opts);
    const database = client.db(db);
    return {
      store,
      transaction: async (fn) => {
        const session = client.startSession();
        try {
          return await session.withTransaction(() => fn(session));
        } finally {
          await session.endSession();
        }
      },
      getDeliveries: async () =>
        (await database.collection("txob_deliveries").find().toArray()) as any,
      getEvents: async () =>
        (await database.collection("txob_events").find().toArray()).map(
          (e) => ({
            id: e._id as unknown as string,
            type: e.type,
            fanned_out_at: e.fanned_out_at,
          }),
        ),
      createWakeupEmitter: () => createMongoWakeupEmitter(opts),
      teardown: async () => {
        await database.dropDatabase();
        await client.close();
      },
    };
  });

  describe("migrate", () => {
    it("is idempotent", async () => {
      const client = await new MongoClient(url!).connect();
      const db = `txob_test_${randomBytes(4).toString("hex")}`;
      try {
        await migrate({ client, db });
        await migrate({ client, db, collectionPrefix: "txob_" });
        const indexes = await client
          .db(db)
          .collection("txob_deliveries")
          .indexes();
        expect(indexes.length).toBeGreaterThan(1);
      } finally {
        await client.db(db).dropDatabase();
        await client.close();
      }
    });
  });
});
