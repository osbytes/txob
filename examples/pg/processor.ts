import {
  createEventHandlerMap,
  createEventProcessor,
  TxOBError,
} from "../../src/index.js";
import { createPgWakeupEmitter, migrate } from "../../src/pg/index.js";
import { eventSchemas, pool, store } from "./events.js";
import { sleep } from "../../src/sleep.js";

await migrate(pool);

const wakeupEmitter = await createPgWakeupEmitter({
  listenClientConfig: {
    user: process.env.POSTGRES_USER || "outbox",
    password: process.env.POSTGRES_PASSWORD || "outbox",
    database: process.env.POSTGRES_DB || "outbox",
    port: parseInt(process.env.POSTGRES_PORT || "5434"),
  },
});

const handlerMap = createEventHandlerMap({
  eventSchemas,
  handlerMap: {
    ResourceSaved: {
      // A plain function uses the defaults: 5 attempts with exponential backoff
      audit: async (event) => {
        console.log(
          `${event.id} audit ${event.correlation_id} activity=${event.data.id}`,
        );
      },

      // Calls a rate-limited third party: at most 10 starts per second, 3 at a time
      syncCrm: {
        concurrency: 3,
        rateLimit: { max: 10, durationMs: 1_000 },
        retry: { maxAttempts: 8 },
        timeoutMs: 5_000,
        handler: async (event, { signal, attempt }) => {
          await sleep(Math.random() * 200, signal);
          if (Math.random() > 0.9) {
            throw new TxOBError("crm throttled", {
              backoffUntil: new Date(Date.now() + 2_000),
            });
          }
          console.log(`${event.id} syncCrm attempt=${attempt}`);
        },
      },

      // Per-account updates must not race: one at a time per account, accounts in parallel
      updateAccountStats: {
        concurrency: {
          perKey: { key: (event) => event.data.accountId, limit: 1 },
        },
        priority: (event) => (event.data.accountId === "acct-0" ? 10 : 0),
        handler: async (event) => {
          await sleep(Math.random() * 500);
          if (Math.random() > 0.8) throw new Error("some issue");
          console.log(`${event.id} updateAccountStats ${event.data.accountId}`);
        },
      },
    },
    DeliveryDead: {
      alert: async (event) => {
        console.log("delivery dead", event.data);
      },
    },
  },
});

const processor = createEventProcessor({
  store,
  eventSchemas,
  handlerMap,
  concurrency: 50,
  wakeupEmitter,
  logger: {
    info: console.log,
    error: console.error,
    warn: console.warn,
    debug: () => {},
  },
  // Runs in the same transaction that marks the delivery dead
  onDeliveryDead: async ({ event, handler, error, publish }) => {
    await publish(
      "DeliveryDead",
      {
        eventId: event.id,
        eventType: event.type,
        handler,
        error: error instanceof Error ? error.message : String(error),
      },
      { correlation_id: event.correlation_id },
    );
  },
});
processor.start();

const shutdown = (() => {
  let shutdownStarted = false;
  return async () => {
    if (shutdownStarted) return;
    shutdownStarted = true;

    try {
      await processor.stop();
      await wakeupEmitter.close();
      await pool.end();
    } catch (err) {
      console.error(err);
      process.exit(1);
    }
    process.exit(0);
  };
})();
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
