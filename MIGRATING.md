# Migrating from 0.4 to 0.5

0.5 reworks the processing model and is not backwards compatible. The ideas you're used to stay the same: typed event schemas, a handler map keyed by event type, at-least-once delivery. Storage, the processing model and most APIs change.

## What changed and why

| 0.4                                                                        | 0.5                                                                                                                | Why                                                          |
| -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------ |
| One `events` row per event; per-handler state in `handler_results` JSON    | `txob_events` (immutable) plus `txob_deliveries` (one row per event × handler)                                     | Gives each handler its own retries, priority and concurrency |
| Row locked with `FOR UPDATE` while all handlers ran inside one transaction | Short lease-based claims; no transaction held during handlers                                                      | Works with pools, PgBouncer and ORM transaction timeouts     |
| You wrote the `INSERT INTO events` yourself                                | `outbox.publish(tx, type, data)` validates and inserts                                                             | Typed, schema-validated writes                               |
| `maxErrors` per event                                                      | `retry.maxAttempts` per handler                                                                                    | Handlers fail independently                                  |
| `maxEventConcurrency`, `maxHandlerConcurrency`, `maxQueuedEvents`          | `concurrency` (per processor) plus per-handler `concurrency`, `perKey`, `rateLimit` enforced across all processors | Limits hold when scaled horizontally                         |
| `onEventMaxErrorsReached({ event, txClient })`                             | `onDeliveryDead({ event, handler, error, publish, tx })`                                                           | Per handler; `publish` is typed                              |
| `createProcessorClient` (`txob/pg`, `txob/mongodb`)                        | `createPgStore` (`txob/pg`), `createMongoStore` (`txob/mongodb`), `createMemoryStore` (`txob/memory`)              | New store interface                                          |
| `createWakeupEmitter`                                                      | Postgres: `migrate()` creates the triggers, `createPgWakeupEmitter` listens. MongoDB: `createMongoWakeupEmitter`   | Schema setup happens in one place                            |

## Steps

1. **Create the new tables** with `migrate(pool)`, or add `getMigrationSql()` to your migrations. They live next to your old `events` table.

2. **Switch writers** to `outbox.publish(tx, type, data, { correlation_id })`. `tx` is the connection your transaction runs on (e.g. a `pg.PoolClient` after `BEGIN`).

3. **Update the processor:**

   ```diff
   - new EventProcessor({
   -   client: createProcessorClient({ querier: client, eventSchemas }),
   -   handlerMap,
   -   maxErrors: 5,
   -   maxEventConcurrency: 20,
   -   onEventMaxErrorsReached: async ({ event, txClient }) => { ... },
   - })
   + createEventProcessor({
   +   store: createPgStore({ pool }),
   +   eventSchemas,
   +   handlerMap,
   +   handlerDefaults: { retry: { maxAttempts: 5 } },
   +   concurrency: 20,
   +   onDeliveryDead: async ({ event, handler, publish }) => { ... },
   + })
   ```

   Handler functions keep the same signature `(event, { signal })` and gain `attempt` and `handler` in the context. Event fields: `timestamp` is now `created_at`; `handler_results`, `errors`, `backoff_until` and `processed_at` are gone from the event (state lives on deliveries).

   `timeoutMs` (new) aborts the handler's `signal`; honoring it is up to the handler.

   Custom `backoff` now returns a delay in ms (or a `Date`) and receives `{ attempt, error, event, handler }`.

   **MongoDB:** `createProcessorClient({ mongo, db })` becomes `createMongoStore({ client, db })`; run `migrate({ client, db })` once to create indexes. Publish with `outbox.publish(session, ...)` inside `session.withTransaction`. New collections are `txob_events` and `txob_deliveries` (plus `txob_locks` and `txob_rate_limit_claims`).

4. **Drain the old table.** Keep a 0.4 processor running against `events` until it has no unprocessed rows, then stop it. New events go to `txob_events` from step 2 onward.

   ```sql
   SELECT count(*) FROM events WHERE processed_at IS NULL;
   -- MongoDB: db.events.countDocuments({ processed_at: null })
   ```

   Alternatively, copy unprocessed events across and let 0.5 deliver them. Handlers that already succeeded under 0.4 will run again, so this relies on idempotent handlers:

   ```sql
   INSERT INTO txob_events (id, type, data, correlation_id, created_at)
   SELECT id::text, type, data, correlation_id::text, timestamp FROM events WHERE processed_at IS NULL
   ON CONFLICT DO NOTHING;
   ```

5. **Requirements:** Node.js 20.3+, and Postgres 14+ or a MongoDB replica set.
