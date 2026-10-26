import { resolveCollections, type CreateMongoStoreOpts } from "./store.js";

/**
 * Creates the indexes txob's queries rely on. Idempotent; safe to run at startup from many
 * processes.
 */
export const migrate = async (opts: CreateMongoStoreOpts): Promise<void> => {
  const { events, deliveries, rateLimitClaims } = resolveCollections(opts);
  await Promise.all([
    events.createIndexes([
      {
        key: { fanned_out_at: 1, type: 1, fanout_run_after: 1, created_at: 1 },
      },
      { key: { fanout_lease_token: 1 }, sparse: true },
    ]),
    deliveries.createIndexes([
      // claim order
      {
        key: {
          event_type: 1,
          handler: 1,
          status: 1,
          priority: -1,
          run_after: 1,
          created_at: 1,
        },
      },
      // leased deliveries, counted for cross-processor concurrency limits
      {
        key: {
          event_type: 1,
          handler: 1,
          status: 1,
          lease_token: 1,
          concurrency_key: 1,
        },
      },
      { key: { lease_token: 1 }, sparse: true },
      { key: { event_id: 1 } },
    ]),
    rateLimitClaims.createIndexes([
      { key: { event_type: 1, handler: 1, claimed_at: 1 } },
      // claims are pruned on every claim; this is a backstop for handlers that went away
      { key: { claimed_at: 1 }, expireAfterSeconds: 7 * 24 * 60 * 60 },
    ]),
  ]);
};
