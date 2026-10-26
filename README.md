<h1 align="center">txob</h1>
<p align="center">Reliably process side-effects in your Node.js applications without data loss</p>
<p align="center">
  <a href="https://www.osbytes.io" target="_blank" rel="noopener noreferrer" title="osbytes — open source bytes">
     <img
       src="https://www.osbytes.io/badge.svg"
       alt="osbytes — open source bytes"
       width="24"
       height="24"
     />
  </a>
  <a href="https://codecov.io/gh/osbytes/txob" >
    <img src="https://codecov.io/gh/osbytes/txob/graph/badge.svg?token=E9M7G67VLL"/>
  </a>
  <a aria-label="NPM version" href="https://www.npmjs.com/package/txob">
    <img alt="" src="https://badgen.net/npm/v/txob">
  </a>
  <a aria-label="License" href="https://github.com/osbytes/txob/blob/main/LICENSE">
    <img alt="" src="https://badgen.net/npm/license/txob">
  </a>
  <a aria-label="Typescript" href="https://github.com/osbytes/txob/blob/main/src/cache.ts">
    <img alt="" src="https://badgen.net/npm/types/txob">
  </a>
  <a aria-label="CodeFactor" href="https://www.codefactor.io/repository/github/osbytes/txob">
    <img alt="" src="https://www.codefactor.io/repository/github/osbytes/txob/badge">
  </a>
</p>

> **txob 0.5** reworks the processing model and is not backwards compatible. Coming from 0.4? See [MIGRATING.md](./MIGRATING.md).

## The Problem

When building applications, you often need to perform multiple operations together: update your database AND send an email, publish an event, trigger a webhook, or notify another service.

```typescript
// ❌ What if the email fails after the database commit?
await db.createUser(user);
await db.commit();
await emailService.sendWelcomeEmail(user.email); // 💥 User created, no email sent

// ❌ What about a message queue?
await db.createUser(user);
await messageQueue.publish("user.created", user); // ✅ Message queued
await db.commit(); // 💥 Message is in the queue but there's no user record
```

The queue and the database are separate systems; you can't make them atomic.

**The Transactional Outbox Pattern** solves this by writing the event in the same database transaction as your business data, then delivering it to handlers asynchronously with retries.

```typescript
// ✅ Both commit, or neither does
await db.transaction(async (tx) => {
  await createUser(tx, user);
  await outbox.publish(tx, "UserCreated", {
    userId: user.id,
    email: user.email,
  });
});
// A processor delivers UserCreated to every registered handler, retrying until each succeeds
```

## Features

- ✅ **At-least-once delivery** - events commit with your data and are never lost, even through crashes
- ✅ **Typed events** - define events with any [Standard Schema](https://standardschema.dev) library (Zod, Valibot, ArkType, ...); payloads are validated on publish and typed in handlers
- ✅ **Independent handlers** - each handler of an event is its own delivery with its own retries, so one failing side effect never re-runs the others
- ✅ **Concurrency controls that hold across processors** - per-handler concurrency limits, per-key concurrency (e.g. one at a time per account) and rate limits are enforced across every processor you run, plus priorities, timeouts and retry policies
- ✅ **Short transactions** - work is claimed with leases, so no transaction or connection is held while handlers run; works with connection poolers and ORM transaction timeouts
- ✅ **Horizontal scalability** - run any number of processors; claims use `FOR UPDATE SKIP LOCKED` and lease tokens fence out stale writers
- ✅ **Near-realtime** - Postgres `LISTEN/NOTIFY` wakes processors immediately, with polling as a fallback
- ✅ **Graceful shutdown** - in-flight handlers finish, and claimed-but-unstarted work is handed back
- ✅ **Pluggable storage** - PostgreSQL, MongoDB, Redis/Valkey, DynamoDB and in-memory stores included, with recipes for their ORMs; implement `TxOBStore` for anything else
- ✅ **OpenTelemetry-compatible** spans and metrics, without depending on an SDK

## Quick Start

```bash
npm install txob pg   # or mongodb, ioredis / redis, @aws-sdk/client-dynamodb + @aws-sdk/lib-dynamodb
```

**1. Define your events:**

```typescript
// events.ts
import { z } from "zod";
import { createOutbox } from "txob";
import { createPgStore } from "txob/pg";
import pg from "pg";

export const eventSchemas = {
  UserCreated: z.object({ userId: z.string(), email: z.email() }),
  OrderPlaced: z.object({ orderId: z.string(), accountId: z.string() }),
};

export const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
export const store = createPgStore({ pool });
export const outbox = createOutbox({ eventSchemas, store });
```

**2. Create the tables** (Postgres 14+). Either run `migrate` at startup, or add the output of `getMigrationSql()` to your own migrations:

```typescript
import { migrate } from "txob/pg";
await migrate(pool); // idempotent and safe to run from many processes at once
```

**3. Publish events in your business transactions:**

```typescript
const client = await pool.connect();
try {
  await client.query("BEGIN");
  await client.query("INSERT INTO users (id, email) VALUES ($1, $2)", [
    userId,
    email,
  ]);
  await outbox.publish(
    client,
    "UserCreated",
    { userId, email },
    { correlation_id: requestId },
  );
  await client.query("COMMIT");
} catch (error) {
  await client.query("ROLLBACK");
  throw error;
} finally {
  client.release();
}
```

`publish` validates the data against the event's schema (throwing `TxOBValidationError` on failure) and inserts it using the connection you pass, so it commits or rolls back with your data.

**4. Process them:**

```typescript
import { createEventProcessor } from "txob";
import { createPgWakeupEmitter } from "txob/pg";
import { eventSchemas, store } from "./events.js";

const processor = createEventProcessor({
  store,
  eventSchemas,
  wakeupEmitter: await createPgWakeupEmitter({
    listenClientConfig: { connectionString: process.env.DATABASE_URL },
  }),
  handlerMap: {
    UserCreated: {
      // A plain function: 5 attempts with exponential backoff
      sendWelcomeEmail: async (event, { signal }) => {
        await emailService.send(
          { to: event.data.email, template: "welcome" },
          { signal },
        );
      },
      // Or a config object with concurrency controls
      createStripeCustomer: {
        concurrency: 5,
        rateLimit: { max: 25, durationMs: 1_000 },
        retry: { maxAttempts: 10 },
        timeoutMs: 15_000,
        handler: async (event) => {
          await stripe.customers.create(
            {
              email: event.data.email,
              metadata: { userId: event.data.userId },
            },
            { idempotencyKey: `${event.id}:createStripeCustomer` },
          );
        },
      },
    },
    OrderPlaced: {
      updateAccountBalance: {
        // One at a time per account; different accounts run in parallel
        concurrency: {
          perKey: { key: (event) => event.data.accountId, limit: 1 },
        },
        handler: async (event) => {
          /* ... */
        },
      },
    },
  },
});

processor.start();

process.on("SIGTERM", async () => {
  await processor.stop(); // waits for in-flight handlers (default timeout 10s)
  process.exit(0);
});
```

The handler map must list every event type in `eventSchemas` (TypeScript enforces this). An empty object (`OrderPlaced: {}`) is valid and means "no side effects".

## How It Works

```
publish(tx) ──► txob_events ──fan-out──► txob_deliveries ──claim──► handler
               (in your tx)              one per (event, handler)   (with lease)
```

1. **Publish.** `outbox.publish(tx, ...)` inserts one row into `txob_events` inside your transaction. Publishing doesn't need to know which handlers exist.
2. **Fan-out.** A processor leases unfanned events of the types it handles and, in a single statement, creates one **delivery** per registered handler and marks the events fanned out. Fan-out is idempotent: deliveries are keyed by `(event_id, handler)`.
3. **Claim.** The processor claims pending deliveries up to its free capacity, highest priority first, within each handler's concurrency, per-key concurrency and rate limits. Limits count deliveries leased by **all** processors, and claims for a limited handler are serialized in the store, so they hold however many processors run. Claiming sets a lease (`leaseMs`, default 30s) and a fresh lease token, then commits. No transaction is held while handlers run.
4. **Run.** Claimed deliveries run immediately. Leases of in-flight deliveries are renewed every `leaseMs / 3`.
5. **Record.** Success marks the delivery `completed`. Failure schedules a retry with backoff, or marks it `dead` after `maxAttempts` (or immediately for `ErrorUnprocessableEventHandler`). Every write is conditional on the lease token, so a processor that lost its lease can't overwrite the work of the processor that took over.

If a processor crashes, its leases expire and other processors pick the work up. A claim counts as an attempt, so a handler that crashes the process can't loop forever: once its attempts are used up, the delivery is marked dead without running again.

### Delivery guarantees

txob guarantees **at-least-once** delivery per handler. A handler can run more than once for the same event if the process crashes after the side effect but before the result is recorded, or if a lease is lost (for example, a long GC pause or network partition). **Make handlers idempotent**, e.g. by using `` `${event.id}:${handlerName}` `` as an idempotency key with external APIs.

## Handler Configuration

Each handler is either a function or a config object:

```typescript
{
  handler: async (event, { signal, attempt, handler }) => {},

  // Enforced across all processors. A number is shorthand for { limit }.
  concurrency: {
    limit: 10, // max deliveries of this handler at once
    perKey: { key: (event) => event.data.accountId, limit: 1 }, // max at once per key
  },

  rateLimit: { max: 100, durationMs: 60_000 }, // max handler starts per sliding window

  retry: {
    maxAttempts: 5, // total attempts before the delivery is dead (default 5)
    backoff: ({ attempt, error, event }) => 1000 * 2 ** attempt, // ms or Date (default: 1s doubling, max 60s)
  },

  timeoutMs: 30_000, // abort the handler's signal after this long

  priority: (event) => (event.data.plan === "enterprise" ? 10 : 0), // or a number; higher first
}
```

- **Concurrency, per-key concurrency and rate limits are global.** `concurrency: 5` means at most 5 deliveries of the handler run at once across all processors. Running deliveries are counted by their live leases, so a processor that crashes keeps holding its share until its leases expire (at most `leaseMs`).
- **Claims for a limited handler are serialized** (a transaction-scoped advisory lock on Postgres, a short-lived lock document on MongoDB). Claims take milliseconds, but if one handler sees very high claim rates, prefer a higher `concurrency` on the processor over many tiny claims.
- **`priority` and the `perKey` key are computed at fan-out** and stored with the delivery, so they're evaluated once per event, not per attempt.
- **`timeoutMs` cancels the handler's context.** When it elapses, the `signal` passed to the handler is aborted with a `TxOBTimeoutError` reason. Honoring it is up to the handler: pass the signal to `fetch`, database drivers and so on, or check `signal.aborted`. Whatever the handler then does decides the outcome. If it throws, the attempt fails and is retried; if it finishes successfully anyway, the delivery is completed. The lease stays renewed while the handler runs.
- **Process-wide defaults** go in `handlerDefaults: { retry, timeoutMs }`.

### Tuning at runtime

Limits aren't stored on queued deliveries. The processor passes its current settings with every claim, so raising a limit applies to work that is already queued, whether you redeploy with new values or change them live:

```typescript
// e.g. from a feature flag, shared config or an admin endpoint
processor.configure({
  concurrency: 50, // this processor's capacity
  handlers: {
    UserCreated: {
      createStripeCustomer: {
        concurrency: 20, // global, across all processors
        rateLimit: { max: 100, durationMs: 1_000 }, // or null to remove the limit
        retry: { maxAttempts: 10 },
        timeoutMs: 30_000, // or null for no timeout
      },
    },
  },
});

processor.getConfig(); // effective settings per handler, after overrides
```

- **Typed:** event types, handler names and values are checked against your handler map.
- **Declarative and idempotent:** `handlers` replaces any previous overrides, and handlers it doesn't mention revert to their definitions. Applying the same config twice changes nothing. Invalid config throws and leaves the current settings untouched.
- **When it takes effect:** from the next claim. Attempts already running keep the settings they started with.
- **What can't be changed live:** `priority` and the `perKey` key function. They are computed when an event fans out, so changes apply to events fanned out afterwards; the per-key _limit_ is tunable via `perKeyConcurrency`.
- **Scope:** `configure` changes this processor only. Global limits are checked with the claiming processor's settings, so apply the same config to every processor (shared config, a feature flag, or a broadcast to all instances) to keep them in agreement.

### Errors

| Thrown                                                 | Result                                                         |
| ------------------------------------------------------ | -------------------------------------------------------------- |
| any error                                              | retry after `retry.backoff`, dead after `retry.maxAttempts`    |
| `new TxOBError(msg, { backoffUntil })`                 | retry no earlier than `backoffUntil` (e.g. from `Retry-After`) |
| `new ErrorUnprocessableEventHandler(error)`            | dead immediately; retrying won't help                          |
| `signal.reason` after `timeoutMs` (`TxOBTimeoutError`) | retry                                                          |

Events whose stored data no longer passes their schema are marked dead without running the handler.

### Dead deliveries

`onDeliveryDead` runs **in the same transaction** that marks the delivery dead, so follow-up events it publishes commit atomically with that state change:

```typescript
createEventProcessor({
  // ...
  onDeliveryDead: async ({ event, handler, attempts, error, publish }) => {
    await publish("DeliveryDead", {
      eventId: event.id,
      handler,
      error: String(error),
    });
  },
});
```

If the hook throws, the transaction rolls back and the delivery is retried after its lease expires.

Like any transaction callback, the hook can run more than once: when the hook throws, when a MongoDB transaction hits a transient error, or when a Redis `WATCH` sees a concurrent change. Only one run commits, so keep side effects inside `tx` and `publish`.

## Processor Options

| Option              | Default  | Description                                                            |
| ------------------- | -------- | ---------------------------------------------------------------------- |
| `store`             | required | Storage backend, e.g. `createPgStore({ pool })`                        |
| `eventSchemas`      | required | Standard Schema per event type                                         |
| `handlerMap`        | required | Handlers per event type                                                |
| `handlerDefaults`   | `{}`     | Default `retry` and `timeoutMs` for all handlers                       |
| `concurrency`       | `20`     | Max deliveries running in this processor at once                       |
| `leaseMs`           | `30000`  | Lease duration; renewed every `leaseMs / 3` while work is in progress  |
| `fanoutBatchSize`   | `100`    | Max events fanned out per poll                                         |
| `pollingIntervalMs` | `5000`   | Poll interval; wakeup signals and completions trigger polls in between |
| `wakeupEmitter`     | none     | Source of `wakeup` events, e.g. `createPgWakeupEmitter`                |
| `wakeupThrottleMs`  | `100`    | Minimum time between wakeup-triggered polls                            |
| `onDeliveryDead`    | none     | Hook run in the transaction that marks a delivery dead                 |
| `logger`            | none     | `{ debug, info, warn, error }` (pino, console, ...)                    |
| `telemetry`         | none     | `{ tracer, meter, attributes }`, OpenTelemetry-compatible              |

## PostgreSQL

```typescript
import {
  createPgStore,
  migrate,
  getMigrationSql,
  createPgWakeupEmitter,
} from "txob/pg";

const store = createPgStore({
  pool, // pg.Pool, or any pg-compatible pool (e.g. @neondatabase/serverless)
  schema: "app", // optional: Postgres schema (default: search_path)
  tablePrefix: "txob_", // optional
});

await migrate(pool, { schema: "app", tablePrefix: "txob_", channel: "txob" });
```

`migrate` creates the `txob_events` and `txob_deliveries` tables, a small `txob_rate_limit_claims` log for rate-limited handlers, their indexes, and statement-level `AFTER INSERT` triggers that `pg_notify` the wakeup channel. If you manage schema with Prisma, Drizzle Kit, Knex, etc., put `getMigrationSql(opts)` in a migration instead.

`createPgWakeupEmitter` uses a dedicated connection for `LISTEN`; pass the same `channel` you migrated with. If the connection drops, processors keep polling; attach an `error` listener to be told about it.

**Connections:** completions and lease renewals are single short statements on the pool. Claims for handlers with limits run in a short transaction that takes a transaction-scoped advisory lock per limited handler, and marking a delivery dead runs in a transaction when `onDeliveryDead` is set. This works with PgBouncer in transaction mode, except for the `LISTEN` connection, which needs a direct (session-mode) connection.

**Retention:** completed and dead deliveries stay in the tables. Prune them on a schedule; deleting an event cascades to its deliveries:

```sql
DELETE FROM txob_events e
WHERE e.fanned_out_at < now() - interval '7 days'
  AND NOT EXISTS (SELECT 1 FROM txob_deliveries d WHERE d.event_id = e.id AND d.status = 'pending');
```

**Inspecting state:**

```sql
-- Backlog and failures per handler
SELECT event_type, handler, status, count(*), max(attempts)
FROM txob_deliveries GROUP BY 1, 2, 3;

-- Why did a delivery die?
SELECT event_id, handler, attempts, last_error FROM txob_deliveries WHERE status = 'dead';

-- Retry a dead delivery
UPDATE txob_deliveries SET status = 'pending', attempts = 0, run_after = now()
WHERE event_id = $1 AND handler = $2;
```

## MongoDB

```typescript
import { MongoClient } from "mongodb";
import { createOutbox } from "txob";
import {
  createMongoStore,
  migrate,
  createMongoWakeupEmitter,
} from "txob/mongodb";

const client = await new MongoClient(process.env.MONGO_URL).connect();
const opts = { client, db: "app", collectionPrefix: "txob_" };

await migrate(opts); // creates indexes; idempotent
const store = createMongoStore(opts);
const outbox = createOutbox({ eventSchemas, store });

// Publish inside your transaction by passing the session
const session = client.startSession();
try {
  await session.withTransaction(async () => {
    await client.db("app").collection("users").insertOne(user, { session });
    await outbox.publish(session, "UserCreated", {
      userId: user.id,
      email: user.email,
    });
  });
} finally {
  await session.endSession();
}

const processor = createEventProcessor({
  store,
  eventSchemas,
  handlerMap,
  wakeupEmitter: await createMongoWakeupEmitter(opts), // change stream on inserts
});
```

- **Requires a replica set or sharded cluster.** Atomic publishing and `onDeliveryDead` use multi-document transactions, and wakeups use change streams. A standalone server can run as a single-node replica set.
- **Timestamps come from the processors' clocks.** Keep them synchronized (e.g. NTP); skew well below `leaseMs` is harmless.
- **Retention:** deleting an event does not cascade. Delete its deliveries (`{ event_id }`) along with it.

## Redis and Valkey

```typescript
import { Redis } from "ioredis"; // or a connected node-redis client, or iovalkey
import { createOutbox } from "txob";
import { createRedisStore, createRedisWakeupEmitter } from "txob/redis";

const redis = new Redis(process.env.REDIS_URL);
const store = createRedisStore({ client: redis }); // optional: prefix (default "{txob}:")
const outbox = createOutbox({ eventSchemas, store });

// Publish on the same MULTI as your writes; the event commits with them
const multi = redis.multi();
multi.hset(`user:${user.id}`, user);
await outbox.publish(multi, "UserCreated", {
  userId: user.id,
  email: user.email,
});
await multi.exec();

const processor = createEventProcessor({
  store,
  eventSchemas,
  handlerMap,
  wakeupEmitter: await createRedisWakeupEmitter({ client: redis }), // pub/sub
});
```

- **Clients:** ioredis, iovalkey and node-redis are detected automatically (tested with ioredis 6, iovalkey 0.4 and node-redis 6); txob has no runtime dependency on any of them. `TTx` is your client's own `MULTI` type, so `publish` only accepts a `MULTI` from that client.
- **Servers:** tested against Redis 8 and Valkey 8. Every claim and state change is one Lua script, so the limits hold across processors without locks. The scripts build key names at runtime, which some Redis-compatible servers (e.g. Dragonfly) only allow with extra configuration. Those servers are untested.
- **Read-modify-write:** `WATCH` the keys you read before `MULTI`, as usual. The event is only written if `EXEC` succeeds.
- **`MULTI` doesn't roll back.** If one of your queued commands fails at runtime (e.g. `WRONGTYPE`), the rest, including the event, still apply. Publishing with an `id` that already exists is a no-op instead of an error.
- **Durability** is whatever your persistence settings give you. For an outbox, use AOF (`appendfsync always` for no loss, `everysec` to risk up to a second) and `maxmemory-policy noeviction` so txob's keys are never evicted.
- **Cluster:** all of txob's keys share the `{txob}` hash tag, so they live in one slot. To publish in the same `MULTI` as your writes, your keys must use the same hash tag. Cluster mode isn't covered by the test suite yet.
- **ORMs:** Redis OM's `repository.save` can't join a `MULTI`, so writes made through it can't be published atomically. Write those keys with the client on the same `MULTI` instead.
- **Retention:** completed deliveries keep their hash. Prune `{txob}:d:*`, `{txob}:e:*` and their entries in the `{txob}:deliveries` / `{txob}:events` sorted sets on a schedule.

## DynamoDB

```typescript
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  TransactWriteCommand,
} from "@aws-sdk/lib-dynamodb";
import { createOutbox } from "txob";
import {
  createDynamoStore,
  migrate,
  type DynamoTransactItems,
} from "txob/dynamodb";

const client = new DynamoDBClient({});
const documentClient = DynamoDBDocumentClient.from(client);
await migrate({ client, tableName: "txob" }); // or getTableDefinition() for CDK/Terraform
const store = createDynamoStore({ client, tableName: "txob" });
const outbox = createOutbox({ eventSchemas, store });

// The transaction is the list of TransactWriteItems; add yours, publish, send them together
const tx: DynamoTransactItems = [
  {
    Put: {
      TableName: "users",
      Item: user,
      ConditionExpression: "attribute_not_exists(pk)",
    },
  },
];
await outbox.publish(tx, "UserCreated", { userId: user.id, email: user.email });
await documentClient.send(new TransactWriteCommand({ TransactItems: tx }));

const processor = createEventProcessor({
  store,
  eventSchemas,
  handlerMap,
  pollingIntervalMs: 1_000, // no wakeup emitter: DynamoDB Streams can't push to a process
});
```

ORMs build the same kind of items, so their writes and your event go out in one request:

```typescript
// ElectroDB: build the service transaction's params instead of calling .go()
const { TransactItems = [] } = app.transaction
  .write(({ users }) => [users.create(user).commit()])
  .params();
const tx = [...TransactItems] as DynamoTransactItems;
await outbox.publish(tx, "UserCreated", { userId: user.id, email: user.email });
await documentClient.send(new TransactWriteCommand({ TransactItems: tx }));

// DynamoDB Toolbox: collect each transaction's .params()
const tx: DynamoTransactItems = [
  UserEntity.build(PutTransaction).item(user).params(),
];
await outbox.publish(tx, "UserCreated", { userId: user.id, email: user.email });
await documentClient.send(new TransactWriteCommand({ TransactItems: tx }));

// Dynamoose: get the request instead of sending it; it uses low-level AttributeValues,
// so convert txob's items with marshallTransactItems
import { marshallTransactItems } from "txob/dynamodb";
const request = await dynamoose.transaction([User.transaction.create(user)], {
  return: "request",
});
const tx: DynamoTransactItems = [];
await outbox.publish(tx, "UserCreated", { userId: user.id, email: user.email });
await client.send(
  new TransactWriteItemsCommand({
    TransactItems: [...request.TransactItems, ...marshallTransactItems(tx)],
  }),
);
```

- **Table:** one table with a string partition key `pk` and two sparse global secondary indexes (see `getTableDefinition`). Pending deliveries are indexed per handler and priority, sorted by when they're due, so claims never read leased deliveries or ones waiting on a backoff. Index reads are eventually consistent, but every claim is a conditional write, so a stale read can't double-claim.
- **Limits:** each handler with a concurrency, per-key or rate limit has a small limiter item listing its running deliveries. Claims update it in the same transaction as the deliveries, guarded by a version number, so limits hold across processors; completions and lease renewals update it in the same transaction as the delivery. DynamoDB caps a single item at 1,000 write units per second, and transactional writes cost two, so one limiter item handles about 500 claims, completions and renewal batches per second. Its 400 KB size limit caps it at a few thousand running deliveries or rate-limit entries per handler.
- **Per-key limits:** keys at their limit are filtered out of claim queries. If one key's backlog is larger than a claim reads (a few thousand deliveries), each claim continues where the last one stopped, so other keys at the same priority may wait a few polls but are never starved.
- **Transactions** hold at most 100 items, and the event takes one of them. `onDeliveryDead` writes join the transaction that marks the delivery dead, so they share that limit.
- **Priorities** must be 32-bit integers. Timestamps come from the processors' clocks, so keep them synchronized.
- **Retention:** add a TTL attribute in your own retention job, or delete completed `d#` and `e#` items on a schedule.

## Testing

`txob/memory` provides an in-process store with the same semantics, so you can run your real handler map in unit tests without a database:

```typescript
import { createMemoryStore } from "txob/memory";

const store = createMemoryStore();
const outbox = createOutbox({ eventSchemas, store });
const processor = createEventProcessor({
  store,
  eventSchemas,
  handlerMap,
  pollingIntervalMs: 10,
});
processor.start();

await store.transaction((tx) =>
  outbox.publish(tx, "UserCreated", { userId: "u1", email: "a@b.co" }),
);
// ...wait, then assert on side effects or store.getDeliveries()
await processor.stop();
```

## Custom Stores

A store implements `TxOBStore<TTx>`, where `TTx` is whatever handle your writes run on (a connection, an ORM transaction, a Redis `MULTI`, a DynamoDB `TransactWriteItems` batch, ...). The built-in stores show how different backends meet the contract: SQL with row locks (`txob/pg`), documents with compare-and-set (`txob/mongodb`), Lua scripts (`txob/redis`) and conditional transactions (`txob/dynamodb`). A store needs three primitives:

1. **An atomic write with business data**, for `insertEvent`. Without this you don't have an outbox.
2. **Compare-and-set**, for lease-fenced `completeDelivery`, `failDelivery`, `extendLeases` and `releaseDeliveries`.
3. **An ordered query on due time**, for `claimEventsForFanout` and `claimDeliveries`.

The behavioral contract every store must satisfy lives in this repo at [`test/contract.ts`](./test/contract.ts) (it is not part of the npm package); copy it and run `runStoreContract(name, setup)` against your store. [`src/memory/store.ts`](./src/memory/store.ts) is a compact reference implementation.

## Telemetry

Pass an OpenTelemetry `Tracer` and/or `Meter` to opt in:

```typescript
import { metrics, trace } from "@opentelemetry/api";

createEventProcessor({
  // ...
  telemetry: {
    tracer: trace.getTracer("txob"),
    meter: metrics.getMeter("txob"),
    attributes: { "service.name": "billing" },
  },
});
```

| Name                                           | Kind                | Attributes                                                                                                             |
| ---------------------------------------------- | ------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `txob.poll`                                    | span                | `txob.deliveries.active`, `txob.events.fanned_out`, `txob.deliveries.claimed`                                          |
| `txob.delivery.process`                        | span                | event id/type/correlation id, `txob.handler.name`, `txob.delivery.attempt`, `txob.delivery.outcome`                    |
| `txob.poll.count` / `.duration`                | counter / histogram | `txob.poll.outcome` (`success`, `error`)                                                                               |
| `txob.fanout.count`                            | counter             | events fanned out                                                                                                      |
| `txob.delivery.processing.count` / `.duration` | counter / histogram | `txob.event.type`, `txob.handler.name`, `txob.delivery.outcome` (`success`, `retry`, `dead`, `lease_lost`, `released`) |

The names are exported as constants (`TxOBTelemetrySpanName`, `TxOBTelemetryMetricName`, `TxOBTelemetryAttributeKey`, ...). Instrument creation errors surface at construction; runtime telemetry failures never affect processing.

## Using txob with Queues and Job Runners

txob is the reliable bridge between your database and everything else. To hand work to Kafka, SQS, BullMQ or pg-boss, publish from a handler and use the event id for deduplication:

```typescript
UserCreated: {
  toKafka: async (event) => {
    await producer.send({ topic: "users", messages: [{ key: event.id, value: JSON.stringify(event.data) }] });
  },
  toBullMQ: async (event) => {
    await queue.add("UserCreated", event.data, { jobId: event.id }); // re-adds are no-ops
  },
},
```

## When to Use txob

**Use txob when:**

- you need side effects to happen reliably after a database commit
- you want retries, backoff and concurrency control per side effect without running a separate queue
- your handlers can be made idempotent

**Consider alternatives when:**

- you need exactly-once semantics or ordered streams (Kafka)
- throughput exceeds what your database can absorb in writes
- you need fan-out to many independent consumer services (a message broker; txob can feed one)

## License

MIT
