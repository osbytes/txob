---
"txob": minor
---

Deferred delivery: `defer: { delayMs } | { until }` on handler definitions sets when deliveries become claimable, and `publish(..., { defer })` overrides it per event. Resolved at fan-out into the delivery's `run_after`; `delayMs` counts from the event's `created_at`.

**Breaking for Postgres and custom stores:** `txob_events` gains a `defer JSONB` column (`ALTER TABLE txob_events ADD COLUMN IF NOT EXISTS defer JSONB` on existing installs), `TxOBEvent` gains `defer`, and `TxOBNewDelivery` gains `run_after`. See MIGRATING.md.
