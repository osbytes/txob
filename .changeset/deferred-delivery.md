---
"txob": minor
---

Deferred delivery: `defer: { delayMs } | { until }` on handler definitions sets when deliveries become claimable, and `publish(..., { defer })` overrides it per event. The publish-time defer is resolved into `event.defer_until` when published; handler defers are resolved at fan-out. Either way the result lands in the delivery's `run_after`, and `delayMs` counts from the event's `created_at`.

**Breaking for Postgres and custom stores:** `txob_events` gains a `defer_until TIMESTAMPTZ` column (`ALTER TABLE txob_events ADD COLUMN IF NOT EXISTS defer_until TIMESTAMPTZ` on existing installs), `TxOBEvent` gains `defer_until`, and `TxOBNewDelivery` gains `run_after`. See MIGRATING.md.
