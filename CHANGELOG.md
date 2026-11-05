# txob

## 0.6.0

### Minor Changes

- [#94](https://github.com/osbytes/txob/pull/94) [`ffb6a51`](https://github.com/osbytes/txob/commit/ffb6a5160cd78821648a38014cc17e90a8965ea5) Thanks [@dillonstreator](https://github.com/dillonstreator)! - Deferred delivery: `defer: { delayMs } | { until }` on handler definitions sets when deliveries become claimable, and `publish(..., { defer })` overrides it per event. The publish-time defer is resolved into `event.defer_until` when published; handler defers are resolved at fan-out. Either way the result lands in the delivery's `run_after`, and `delayMs` counts from the event's `created_at`.

  **Breaking for Postgres and custom stores:** `txob_events` gains a `defer_until TIMESTAMPTZ` column (`ALTER TABLE txob_events ADD COLUMN IF NOT EXISTS defer_until TIMESTAMPTZ` on existing installs), `TxOBEvent` gains `defer_until`, and `TxOBNewDelivery` gains `run_after`. See MIGRATING.md.

### Patch Changes

- [#96](https://github.com/osbytes/txob/pull/96) [`1bd84f8`](https://github.com/osbytes/txob/commit/1bd84f8dee4808cadc42edc280591afe4d14ae80) Thanks [@dillonstreator](https://github.com/dillonstreator)! - Fix CommonJS consumers: each export now gives `require` its own CommonJS type declarations, the build no longer copies a `"type": "module"` package.json into `dist/`, and `engines.node` is now `^20.19.0 || >=22.12.0` (the versions that can `require()` the ESM-only `p-limit` and `p-queue`).

## 0.5.0

### Minor Changes

- [#88](https://github.com/osbytes/txob/pull/88) [`d5ce2ea`](https://github.com/osbytes/txob/commit/d5ce2ea80ca6c9d4d561dfc2f3f7bea40f518723) Thanks [@dillonstreator](https://github.com/dillonstreator)! - txob 0.5 reworks processing around a pluggable store. **Breaking:** see [MIGRATING.md](https://github.com/osbytes/txob/blob/main/MIGRATING.md) to upgrade from 0.4.
  - Stores for Postgres (`txob/pg`), MongoDB (`txob/mongodb`), Redis/Valkey (`txob/redis`), DynamoDB (`txob/dynamodb`) and in-memory (`txob/memory`)
  - Postgres ORM adapters for Drizzle, Kysely, Prisma, postgres.js, Sequelize, Knex and TypeORM, with schema and migration helpers
  - Lazy fan-out: publishing writes one event; processors create a delivery per handler, each retried independently
  - Lease-based claims fenced by lease tokens, so no transaction is held while handlers run
  - Per-handler concurrency, per-key concurrency and rate limits, enforced across all processors
  - `timeoutMs` aborts the handler's `signal`; `processor.configure()` tunes limits, retries and timeouts at runtime with type checking
  - `onDeliveryDead` runs in the transaction that marks the delivery dead
