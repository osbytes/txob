# txob

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
