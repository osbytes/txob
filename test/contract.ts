import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  createEventProcessor,
  createOutbox,
  ErrorUnprocessableEventHandler,
  TxOBError,
  type CreateEventProcessorOpts,
  type EventProcessor,
  type TxOBEvent,
  type TxOBStore,
  type WakeupEmitter,
} from "../src/index.js";
import { delay, schema, silentLogger, waitFor } from "./helpers.js";

export type ContractDelivery = {
  event_id: string;
  event_type: string;
  handler: string;
  status: "pending" | "completed" | "dead";
  attempts: number;
};

export type ContractHarness<TTx> = {
  store: TxOBStore<TTx>;
  transaction<T>(fn: (tx: TTx) => Promise<T>): Promise<T>;
  getDeliveries(): Promise<ContractDelivery[]>;
  getEvents(): Promise<
    { id: string; type: string; fanned_out_at: Date | null }[]
  >;
  /** A wakeup source driven by the store's inserts, if the store has one. */
  createWakeupEmitter?(): Promise<WakeupEmitter>;
  teardown(): Promise<void>;
};

const eventSchemas = {
  UserCreated: schema<{ userId: string; email: string }>((v) =>
    typeof v.userId === "string" ? undefined : "userId must be a string",
  ),
  OrderPlaced: schema<{ orderId: string; accountId: string }>(),
  DeliveryDead: schema<{ eventId: string; handler: string }>(),
  Unhandled: schema<{ n: number }>(),
};
type Schemas = typeof eventSchemas;

/**
 * Behavioral tests every store must pass, run through the public processor API plus direct
 * store calls for lease semantics.
 */
export const runStoreContract = <TTx>(
  name: string,
  setup: () => Promise<ContractHarness<TTx>>,
) => {
  describe(`store contract: ${name}`, () => {
    let h: ContractHarness<TTx>;
    let processors: EventProcessor<Schemas, TTx>[];
    let outbox: ReturnType<typeof createOutbox<Schemas, TTx>>;

    beforeEach(async () => {
      h = await setup();
      processors = [];
      outbox = createOutbox({ eventSchemas, store: h.store });
    });

    afterEach(async () => {
      await Promise.all(
        processors.map((p) => p.stop({ timeoutMs: 2_000 }).catch(() => {})),
      );
      await h.teardown();
    });

    const startProcessor = (
      opts: Partial<CreateEventProcessorOpts<Schemas, TTx>> &
        Pick<CreateEventProcessorOpts<Schemas, TTx>, "handlerMap">,
    ) => {
      const processor = createEventProcessor({
        store: h.store,
        eventSchemas,
        pollingIntervalMs: 20,
        logger: silentLogger,
        ...opts,
      });
      processors.push(processor);
      processor.start();
      return processor;
    };

    const noop = async () => {};
    const baseHandlers = {
      UserCreated: {},
      OrderPlaced: {},
      DeliveryDead: {},
      Unhandled: {},
    };

    const publishUsers = (count: number) =>
      h.transaction(async (tx) => {
        const events = [];
        for (let i = 0; i < count; i++) {
          events.push(
            await outbox.publish(tx, "UserCreated", {
              userId: `u${i}`,
              email: `u${i}@x.io`,
            }),
          );
        }
        return events;
      });

    const deliveriesBy = async (status: ContractDelivery["status"]) =>
      (await h.getDeliveries()).filter((d) => d.status === status);

    describe("outbox", () => {
      it("persists events only when the transaction commits", async () => {
        await expect(
          h.transaction(async (tx) => {
            await outbox.publish(tx, "UserCreated", {
              userId: "a",
              email: "a@x.io",
            });
            throw new Error("rollback");
          }),
        ).rejects.toThrow("rollback");
        expect(await h.getEvents()).toHaveLength(0);

        const [event] = await publishUsers(1);
        expect(await h.getEvents()).toMatchObject([
          { id: event.id, type: "UserCreated", fanned_out_at: null },
        ]);
      });

      it("rejects data that fails schema validation", async () => {
        await expect(
          h.transaction((tx) =>
            outbox.publish(tx, "UserCreated", {
              userId: 1 as any,
              email: "a@x.io",
            }),
          ),
        ).rejects.toThrow("userId must be a string");
      });
    });

    describe("leases", () => {
      it("hides claimed events from other fan-out claims until the lease expires", async () => {
        await publishUsers(3);
        const types = ["UserCreated"];
        const first = await h.store.claimEventsForFanout({
          types,
          limit: 2,
          leaseMs: 200,
        });
        const second = await h.store.claimEventsForFanout({
          types,
          limit: 10,
          leaseMs: 200,
        });
        expect(first).toHaveLength(2);
        expect(second).toHaveLength(1);
        expect(
          await h.store.claimEventsForFanout({
            types,
            limit: 10,
            leaseMs: 200,
          }),
        ).toHaveLength(0);
        expect(
          await h.store.claimEventsForFanout({
            types: ["OrderPlaced"],
            limit: 10,
            leaseMs: 200,
          }),
        ).toHaveLength(0);

        await delay(250);
        const reclaimed = await h.store.claimEventsForFanout({
          types,
          limit: 10,
          leaseMs: 200,
        });
        expect(reclaimed).toHaveLength(3);
        expect(reclaimed.every((e) => e.fanout_attempts === 2)).toBe(true);
      });

      it("creates deliveries idempotently on fan-out", async () => {
        const [event] = await publishUsers(1);
        const delivery = {
          event_id: event.id,
          event_type: "UserCreated",
          handler: "email",
          priority: 0,
          concurrency_key: null,
        };
        await h.store.completeFanout({
          event_ids: [event.id],
          deliveries: [delivery],
        });
        await h.store.completeFanout({
          event_ids: [event.id],
          deliveries: [delivery],
        });
        expect(await h.getDeliveries()).toHaveLength(1);
        expect((await h.getEvents())[0].fanned_out_at).toBeInstanceOf(Date);
        expect(
          await h.store.claimEventsForFanout({
            types: ["UserCreated"],
            limit: 10,
            leaseMs: 1_000,
          }),
        ).toHaveLength(0);
      });

      it("claims deliveries within per-handler and overall limits, highest priority first", async () => {
        const events = await publishUsers(6);
        await h.store.completeFanout({
          event_ids: events.map((e) => e.id),
          deliveries: events.flatMap((e, i) => [
            {
              event_id: e.id,
              event_type: "UserCreated",
              handler: "a",
              priority: i,
              concurrency_key: null,
            },
            {
              event_id: e.id,
              event_type: "UserCreated",
              handler: "b",
              priority: 0,
              concurrency_key: null,
            },
          ]),
        });

        const claimed = await h.store.claimDeliveries({
          slots: [
            { event_type: "UserCreated", handler: "a", limit: 3 },
            { event_type: "UserCreated", handler: "b", limit: 10 },
          ],
          limit: 5,
          leaseMs: 1_000,
        });
        expect(claimed).toHaveLength(5);
        const a = claimed.filter((d) => d.handler === "a");
        expect(a).toHaveLength(3);
        expect(a.map((d) => d.priority).sort()).toEqual([3, 4, 5]);
        expect(claimed[0]).toMatchObject({
          handler: "a",
          priority: 5,
          attempts: 1,
          event: {
            id: events[5].id,
            type: "UserCreated",
            data: { userId: "u5" },
          },
        });
        expect(claimed[0].event.created_at).toBeInstanceOf(Date);
      });

      it("never exceeds limits when processors claim concurrently", async () => {
        const events = await h.transaction(async (tx) => {
          const published = [];
          for (let i = 0; i < 20; i++) {
            published.push(
              await outbox.publish(tx, "OrderPlaced", {
                orderId: `o${i}`,
                accountId: `acct${i % 2}`,
              }),
            );
          }
          return published;
        });
        await h.store.completeFanout({
          event_ids: events.map((e) => e.id),
          deliveries: events.flatMap((e) => [
            {
              event_id: e.id,
              event_type: "OrderPlaced",
              handler: "capped",
              priority: 0,
              concurrency_key: null,
            },
            {
              event_id: e.id,
              event_type: "OrderPlaced",
              handler: "keyed",
              priority: 0,
              concurrency_key: e.data.accountId,
            },
            {
              event_id: e.id,
              event_type: "OrderPlaced",
              handler: "rated",
              priority: 0,
              concurrency_key: null,
            },
          ]),
        });
        const slots = [
          {
            event_type: "OrderPlaced",
            handler: "capped",
            limit: 10,
            concurrency: 3,
          },
          {
            event_type: "OrderPlaced",
            handler: "keyed",
            limit: 10,
            concurrencyPerKey: 1,
          },
          {
            event_type: "OrderPlaced",
            handler: "rated",
            limit: 10,
            rateLimit: { max: 4, durationMs: 60_000 },
          },
        ];
        const claims = await Promise.all(
          Array.from({ length: 8 }, () =>
            h.store.claimDeliveries({ slots, limit: 30, leaseMs: 5_000 }),
          ),
        );
        const claimed = claims.flat();
        expect(claimed.filter((d) => d.handler === "capped")).toHaveLength(3);
        const keyed = claimed.filter((d) => d.handler === "keyed");
        expect(keyed.map((d) => d.concurrency_key).sort()).toEqual([
          "acct0",
          "acct1",
        ]);
        expect(claimed.filter((d) => d.handler === "rated")).toHaveLength(4);

        // Finishing frees capacity; the rate limit window does not
        await h.store.completeDelivery(
          claimed.find((d) => d.handler === "capped")!,
        );
        await h.store.completeDelivery(keyed[0]);
        const next = await h.store.claimDeliveries({
          slots,
          limit: 30,
          leaseMs: 5_000,
        });
        expect(next.filter((d) => d.handler === "capped")).toHaveLength(1);
        expect(next.filter((d) => d.handler === "keyed")).toMatchObject([
          { concurrency_key: keyed[0].concurrency_key },
        ]);
        expect(next.filter((d) => d.handler === "rated")).toHaveLength(0);
      });

      it("never exceeds concurrency under heavy concurrent claiming", async () => {
        const events = await h.transaction(async (tx) => {
          const published = [];
          for (let i = 0; i < 100; i++) {
            published.push(
              await outbox.publish(tx, "OrderPlaced", {
                orderId: `o${i}`,
                accountId: `acct${i % 4}`,
              }),
            );
          }
          return published;
        });
        for (let round = 0; round < 5; round++) {
          await h.store.completeFanout({
            event_ids: events.map((e) => e.id),
            deliveries: events.flatMap((e) => [
              {
                event_id: e.id,
                event_type: "OrderPlaced",
                handler: `capped${round}`,
                priority: 0,
                concurrency_key: null,
              },
              {
                event_id: e.id,
                event_type: "OrderPlaced",
                handler: `keyed${round}`,
                priority: 0,
                concurrency_key: e.data.accountId,
              },
            ]),
          });
          const slots = [
            {
              event_type: "OrderPlaced",
              handler: `capped${round}`,
              limit: 50,
              concurrency: 5,
            },
            {
              event_type: "OrderPlaced",
              handler: `keyed${round}`,
              limit: 50,
              concurrencyPerKey: 1,
            },
          ];
          const claimed = (
            await Promise.all(
              Array.from({ length: 30 }, () =>
                h.store.claimDeliveries({ slots, limit: 100, leaseMs: 60_000 }),
              ),
            )
          ).flat();
          expect(
            claimed.filter((d) => d.handler === `capped${round}`),
          ).toHaveLength(5);
          expect(
            claimed.filter((d) => d.handler === `keyed${round}`),
          ).toHaveLength(4);
        }
      }, 30_000);

      it("doesn't let one key's backlog starve other keys", async () => {
        // The hot key's deliveries are all older than the other keys' ones
        const hot = 2_500;
        const events = await h.transaction(async (tx) => {
          const published = [];
          for (let i = 0; i < hot + 5; i++) {
            published.push(
              await outbox.publish(tx, "OrderPlaced", {
                orderId: `o${i}`,
                accountId: i < hot ? "hot" : `cold${i - hot}`,
              }),
            );
          }
          return published;
        });
        await h.store.completeFanout({
          event_ids: events.map((e) => e.id),
          deliveries: events.map((e) => ({
            event_id: e.id,
            event_type: "OrderPlaced",
            handler: "keyed",
            priority: 0,
            concurrency_key: e.data.accountId,
          })),
        });
        const slots = [
          {
            event_type: "OrderPlaced",
            handler: "keyed",
            limit: 10,
            concurrencyPerKey: 1,
          },
        ];
        // Stores may need a second claim to look past the hot key once it is at its limit
        const first = [
          ...(await h.store.claimDeliveries({
            slots,
            limit: 10,
            leaseMs: 60_000,
          })),
          ...(await h.store.claimDeliveries({
            slots,
            limit: 10,
            leaseMs: 60_000,
          })),
        ];
        expect(first.map((d) => d.concurrency_key).sort()).toEqual([
          "cold0",
          "cold1",
          "cold2",
          "cold3",
          "cold4",
          "hot",
        ]);
        // Every key is now at its limit
        expect(
          await h.store.claimDeliveries({ slots, limit: 10, leaseMs: 60_000 }),
        ).toHaveLength(0);
        await h.store.completeDelivery(
          first.find((d) => d.concurrency_key === "cold0")!,
        );
        expect(
          await h.store.claimDeliveries({ slots, limit: 10, leaseMs: 60_000 }),
        ).toHaveLength(0);
        await h.store.completeDelivery(
          first.find((d) => d.concurrency_key === "hot")!,
        );
        expect(
          await h.store.claimDeliveries({ slots, limit: 10, leaseMs: 60_000 }),
        ).toMatchObject([{ concurrency_key: "hot", attempts: 1 }]);
      }, 120_000);

      it("renews many leases of a limited handler at once", async () => {
        const events = await publishUsers(40);
        await h.store.completeFanout({
          event_ids: events.map((e) => e.id),
          deliveries: events.map((e) => ({
            event_id: e.id,
            event_type: "UserCreated",
            handler: "a",
            priority: 0,
            concurrency_key: null,
          })),
        });
        const slots = [
          {
            event_type: "UserCreated",
            handler: "a",
            limit: 50,
            concurrency: 50,
          },
        ];
        const claimed = await h.store.claimDeliveries({
          slots,
          limit: 50,
          leaseMs: 200,
        });
        expect(claimed).toHaveLength(40);
        for (let round = 0; round < 3; round++) {
          expect(await h.store.extendLeases(claimed, { leaseMs: 200 })).toEqual(
            [],
          );
          await delay(100);
        }
        // Still held 300ms after claiming with a 200ms lease
        expect(
          await h.store.claimDeliveries({ slots, limit: 50, leaseMs: 200 }),
        ).toHaveLength(0);
      });

      it("doesn't run the dead-delivery hook for a lease that was lost", async () => {
        const [event] = await publishUsers(1);
        await h.store.completeFanout({
          event_ids: [event.id],
          deliveries: [
            {
              event_id: event.id,
              event_type: "UserCreated",
              handler: "a",
              priority: 0,
              concurrency_key: null,
            },
          ],
        });
        const slots = [{ event_type: "UserCreated", handler: "a", limit: 1 }];
        const [stale] = await h.store.claimDeliveries({
          slots,
          limit: 1,
          leaseMs: 50,
        });
        await delay(80);
        await h.store.claimDeliveries({ slots, limit: 1, leaseMs: 1_000 });
        let hookRan = false;
        expect(
          await h.store.failDelivery(
            stale,
            {
              error: { message: "x", timestamp: new Date().toISOString() },
              run_after: null,
            },
            {
              within: async () => {
                hookRan = true;
              },
            },
          ),
        ).toBe(false);
        expect(hookRan).toBe(false);
      });

      it("frees concurrency held by expired leases", async () => {
        const [event] = await publishUsers(2);
        const events = await h.getEvents();
        await h.store.completeFanout({
          event_ids: events.map((e) => e.id),
          deliveries: events.map((e) => ({
            event_id: e.id,
            event_type: "UserCreated",
            handler: "a",
            priority: 0,
            concurrency_key: null,
          })),
        });
        void event;
        const slots = [
          {
            event_type: "UserCreated",
            handler: "a",
            limit: 10,
            concurrency: 1,
          },
        ];
        expect(
          await h.store.claimDeliveries({ slots, limit: 10, leaseMs: 100 }),
        ).toHaveLength(1);
        expect(
          await h.store.claimDeliveries({ slots, limit: 10, leaseMs: 100 }),
        ).toHaveLength(0);
        // The holder "crashes": its lease lapses and the slot opens up
        await delay(150);
        expect(
          await h.store.claimDeliveries({ slots, limit: 10, leaseMs: 100 }),
        ).toHaveLength(1);
      });

      it("fences writes with the lease token", async () => {
        const [event] = await publishUsers(1);
        await h.store.completeFanout({
          event_ids: [event.id],
          deliveries: [
            {
              event_id: event.id,
              event_type: "UserCreated",
              handler: "a",
              priority: 0,
              concurrency_key: null,
            },
          ],
        });
        const slots = [{ event_type: "UserCreated", handler: "a", limit: 1 }];
        const [stale] = await h.store.claimDeliveries({
          slots,
          limit: 1,
          leaseMs: 100,
        });
        expect(await h.store.extendLeases([stale], { leaseMs: 100 })).toEqual(
          [],
        );
        await delay(150);
        const [fresh] = await h.store.claimDeliveries({
          slots,
          limit: 1,
          leaseMs: 1_000,
        });
        expect(fresh.attempts).toBe(2);

        expect(await h.store.completeDelivery(stale)).toBe(false);
        expect(
          await h.store.failDelivery(stale, {
            error: { message: "x", timestamp: new Date().toISOString() },
            run_after: null,
          }),
        ).toBe(false);
        expect(
          await h.store.extendLeases([stale, fresh], { leaseMs: 1_000 }),
        ).toEqual([stale]);

        await h.store.releaseDeliveries([fresh]);
        const [released] = await h.store.claimDeliveries({
          slots,
          limit: 1,
          leaseMs: 1_000,
        });
        expect(released.attempts).toBe(2);
        expect(await h.store.completeDelivery(released)).toBe(true);
        expect(await deliveriesBy("completed")).toHaveLength(1);
      });
    });

    describe("processor", () => {
      it("delivers every event to every handler exactly once", async () => {
        const calls = new Map<string, number>();
        const count = (key: string) =>
          calls.set(key, (calls.get(key) ?? 0) + 1);
        startProcessor({
          concurrency: 10,
          handlerMap: {
            ...baseHandlers,
            UserCreated: {
              email: async (e) => void count(`email:${e.id}`),
              crm: async (e) => void count(`crm:${e.id}`),
            },
          },
        });
        const events = await publishUsers(40);
        await waitFor(
          async () => (await deliveriesBy("completed")).length === 80,
        );
        expect(calls.size).toBe(80);
        expect([...calls.values()].every((n) => n === 1)).toBe(true);
        for (const e of events) expect(calls.get(`email:${e.id}`)).toBe(1);
      });

      it("does not run a delivery twice across competing processors", async () => {
        const calls = new Map<string, number>();
        const handlerMap = {
          ...baseHandlers,
          UserCreated: {
            email: async (e: TxOBEvent) => {
              calls.set(e.id, (calls.get(e.id) ?? 0) + 1);
              await delay(2);
            },
          },
        };
        for (let i = 0; i < 3; i++)
          startProcessor({ concurrency: 5, handlerMap });
        await publishUsers(60);
        await waitFor(
          async () => (await deliveriesBy("completed")).length === 60,
          {
            timeoutMs: 10_000,
          },
        );
        expect(calls.size).toBe(60);
        expect([...calls.values()].every((n) => n === 1)).toBe(true);
      });

      it("renews leases so long-running handlers aren't picked up by other processors", async () => {
        let calls = 0;
        const handlerMap = {
          ...baseHandlers,
          UserCreated: {
            slow: async () => {
              calls++;
              await delay(400);
            },
          },
        };
        startProcessor({ leaseMs: 100, handlerMap });
        startProcessor({ leaseMs: 100, handlerMap });
        await publishUsers(1);
        await waitFor(
          async () => (await deliveriesBy("completed")).length === 1,
        );
        expect(calls).toBe(1);
      });

      it("passes validated event data and attempt context to handlers", async () => {
        const seen: unknown[] = [];
        startProcessor({
          handlerMap: {
            ...baseHandlers,
            UserCreated: {
              email: async (event, ctx) => {
                seen.push({
                  event,
                  attempt: ctx.attempt,
                  handler: ctx.handler,
                  aborted: ctx.signal.aborted,
                });
              },
            },
          },
        });
        const [event] = await h.transaction(async (tx) => [
          await outbox.publish(
            tx,
            "UserCreated",
            { userId: "u1", email: "e" },
            { correlation_id: "c-1" },
          ),
        ]);
        await waitFor(() => seen.length === 1);
        expect(seen[0]).toMatchObject({
          event: {
            id: event.id,
            type: "UserCreated",
            data: { userId: "u1", email: "e" },
            correlation_id: "c-1",
          },
          attempt: 1,
          handler: "email",
          aborted: false,
        });
      });

      it("retries failed handlers independently with backoff", async () => {
        let crmAttempts = 0;
        let emailCalls = 0;
        startProcessor({
          handlerMap: {
            ...baseHandlers,
            UserCreated: {
              email: async () => void emailCalls++,
              crm: {
                retry: { maxAttempts: 5, backoff: () => 10 },
                handler: async (_, { attempt }) => {
                  crmAttempts = attempt;
                  if (attempt < 3) throw new Error(`fail ${attempt}`);
                },
              },
            },
          },
        });
        await publishUsers(1);
        await waitFor(
          async () => (await deliveriesBy("completed")).length === 2,
        );
        expect(crmAttempts).toBe(3);
        expect(emailCalls).toBe(1);
        expect(
          (await h.getDeliveries()).find((d) => d.handler === "crm")?.attempts,
        ).toBe(3);
      });

      it("honors TxOBError.backoffUntil", async () => {
        const attemptsAt: number[] = [];
        startProcessor({
          handlerMap: {
            ...baseHandlers,
            UserCreated: {
              crm: {
                retry: { backoff: () => 1 },
                handler: async () => {
                  attemptsAt.push(Date.now());
                  if (attemptsAt.length === 1) {
                    throw new TxOBError("later", {
                      backoffUntil: new Date(Date.now() + 300),
                    });
                  }
                },
              },
            },
          },
        });
        await publishUsers(1);
        await waitFor(() => attemptsAt.length === 2);
        expect(attemptsAt[1] - attemptsAt[0]).toBeGreaterThanOrEqual(250);
      });

      it("marks deliveries dead after max attempts and runs onDeliveryDead in the same transaction", async () => {
        const deadEvents: TxOBEvent[] = [];
        startProcessor({
          handlerMap: {
            ...baseHandlers,
            UserCreated: {
              crm: {
                retry: { maxAttempts: 2, backoff: () => 5 },
                handler: async () => {
                  throw new Error("down");
                },
              },
            },
            DeliveryDead: { record: async (e) => void deadEvents.push(e) },
          },
          onDeliveryDead: async ({ event, handler, attempts, publish }) => {
            expect(attempts).toBe(2);
            await publish("DeliveryDead", { eventId: event.id, handler });
          },
        });
        const [event] = await publishUsers(1);
        await waitFor(() => deadEvents.length === 1);
        expect(deadEvents[0].data).toEqual({
          eventId: event.id,
          handler: "crm",
        });
        expect(
          (await h.getDeliveries()).find((d) => d.handler === "crm"),
        ).toMatchObject({
          status: "dead",
          attempts: 2,
        });
      });

      it("leaves the delivery for retry when onDeliveryDead throws", async () => {
        let hookCalls = 0;
        startProcessor({
          leaseMs: 100,
          handlerMap: {
            ...baseHandlers,
            UserCreated: {
              crm: {
                retry: { maxAttempts: 1 },
                handler: async () => {
                  throw new Error("down");
                },
              },
            },
          },
          onDeliveryDead: async ({ publish }) => {
            hookCalls++;
            await publish("DeliveryDead", { eventId: "x", handler: "y" });
            if (hookCalls === 1) throw new Error("hook failed");
          },
        });
        await publishUsers(1);
        await waitFor(() => hookCalls === 2, { timeoutMs: 3_000 });
        await waitFor(async () => (await deliveriesBy("dead")).length === 1);
        // The first hook's publish was rolled back with it
        expect(
          (await h.getEvents()).filter((e) => e.type === "DeliveryDead"),
        ).toHaveLength(1);
      });

      it("marks unprocessable deliveries dead without retrying", async () => {
        let calls = 0;
        startProcessor({
          handlerMap: {
            ...baseHandlers,
            UserCreated: {
              crm: async () => {
                calls++;
                throw new ErrorUnprocessableEventHandler(
                  new Error("bad input"),
                );
              },
            },
          },
        });
        await publishUsers(1);
        await waitFor(async () => (await deliveriesBy("dead")).length === 1);
        await delay(50);
        expect(calls).toBe(1);
      });

      it("marks deliveries dead when stored data fails validation", async () => {
        let calls = 0;
        startProcessor({
          handlerMap: {
            ...baseHandlers,
            UserCreated: { crm: async () => void calls++ },
          },
        });
        await h.transaction((tx) =>
          h.store.insertEvent(tx, {
            id: "invalid-1",
            type: "UserCreated",
            data: { userId: 42 },
            correlation_id: null,
            created_at: new Date(),
          }),
        );
        await waitFor(async () => (await deliveriesBy("dead")).length === 1);
        expect(calls).toBe(0);
      });

      it("cancels the handler's signal after timeoutMs and lets the handler decide the outcome", async () => {
        const reasons: unknown[] = [];
        startProcessor({
          handlerMap: {
            ...baseHandlers,
            UserCreated: {
              // Honors the signal: the attempt fails and is retried
              respectful: {
                timeoutMs: 30,
                retry: { maxAttempts: 2, backoff: () => 5 },
                handler: async (_, { signal, attempt }) => {
                  if (attempt === 2) return;
                  await new Promise((_, reject) => {
                    signal.addEventListener("abort", () => {
                      reasons.push(signal.reason);
                      reject(signal.reason);
                    });
                  });
                },
              },
              // Ignores the signal and finishes successfully: its result stands
              stubborn: {
                timeoutMs: 30,
                handler: async (_, { signal }) => {
                  await delay(80);
                  expect(signal.aborted).toBe(true);
                },
              },
            },
          },
        });
        await publishUsers(1);
        await waitFor(
          async () => (await deliveriesBy("completed")).length === 2,
        );
        expect(reasons).toHaveLength(1);
        expect((reasons[0] as Error).name).toBe("TxOBTimeoutError");
        const deliveries = await h.getDeliveries();
        expect(
          deliveries.find((d) => d.handler === "respectful")?.attempts,
        ).toBe(2);
        expect(deliveries.find((d) => d.handler === "stubborn")?.attempts).toBe(
          1,
        );
      });

      it("enforces handler concurrency across processors", async () => {
        let running = 0;
        let maxRunning = 0;
        const handlerMap = {
          ...baseHandlers,
          UserCreated: {
            limited: {
              concurrency: 2,
              handler: async () => {
                maxRunning = Math.max(maxRunning, ++running);
                await delay(15);
                running--;
              },
            },
          },
        };
        for (let i = 0; i < 3; i++)
          startProcessor({ concurrency: 20, handlerMap });
        await publishUsers(12);
        await waitFor(
          async () => (await deliveriesBy("completed")).length === 12,
          { timeoutMs: 10_000 },
        );
        expect(maxRunning).toBe(2);
      });

      it("enforces per-key concurrency across processors without blocking other keys", async () => {
        const running = new Map<string, number>();
        let maxPerKey = 0;
        let maxTotal = 0;
        const handlerMap = {
          ...baseHandlers,
          OrderPlaced: {
            sync: {
              concurrency: {
                perKey: {
                  key: (e: { data: { accountId: string } }) => e.data.accountId,
                  limit: 1,
                },
              },
              handler: async (e: { data: { accountId: string } }) => {
                const n = (running.get(e.data.accountId) ?? 0) + 1;
                running.set(e.data.accountId, n);
                maxPerKey = Math.max(maxPerKey, n);
                maxTotal = Math.max(
                  maxTotal,
                  [...running.values()].reduce((a, b) => a + b, 0),
                );
                await delay(15);
                running.set(
                  e.data.accountId,
                  running.get(e.data.accountId)! - 1,
                );
              },
            },
          },
        };
        for (let i = 0; i < 3; i++)
          startProcessor({ concurrency: 20, handlerMap });
        await h.transaction(async (tx) => {
          for (let i = 0; i < 12; i++) {
            await outbox.publish(tx, "OrderPlaced", {
              orderId: `o${i}`,
              accountId: `acct${i % 3}`,
            });
          }
        });
        await waitFor(
          async () => (await deliveriesBy("completed")).length === 12,
          { timeoutMs: 10_000 },
        );
        expect(maxPerKey).toBe(1);
        expect(maxTotal).toBeGreaterThan(1);
      });

      it("enforces rate limits across processors", async () => {
        const startedAt: number[] = [];
        const handlerMap = {
          ...baseHandlers,
          UserCreated: {
            limited: {
              rateLimit: { max: 2, durationMs: 100 },
              handler: async () => void startedAt.push(Date.now()),
            },
          },
        };
        for (let i = 0; i < 3; i++) startProcessor({ handlerMap });
        await publishUsers(6);
        await waitFor(
          async () => (await deliveriesBy("completed")).length === 6,
        );
        startedAt.sort((a, b) => a - b);
        for (let i = 2; i < startedAt.length; i++) {
          // Allow for clock granularity between the store and the handler
          expect(startedAt[i] - startedAt[i - 2]).toBeGreaterThanOrEqual(90);
        }
      });

      it("runs higher priority deliveries first", async () => {
        const order: string[] = [];
        await h.transaction(async (tx) => {
          for (let i = 0; i < 5; i++) {
            await outbox.publish(tx, "OrderPlaced", {
              orderId: `o${i}`,
              accountId: i === 3 ? "vip" : "std",
            });
          }
        });
        startProcessor({
          concurrency: 1,
          handlerMap: {
            ...baseHandlers,
            OrderPlaced: {
              sync: {
                priority: (e) => (e.data.accountId === "vip" ? 10 : 0),
                handler: async (e) => void order.push(e.data.orderId),
              },
            },
          },
        });
        await waitFor(() => order.length === 5);
        expect(order[0]).toBe("o3");
      });

      it("leaves events of types the processor doesn't handle unfanned", async () => {
        const { Unhandled: _, ...withoutUnhandled } = baseHandlers;
        startProcessor({
          handlerMap: {
            ...withoutUnhandled,
            UserCreated: { email: noop },
          } as any,
        });
        await h.transaction(async (tx) => {
          await outbox.publish(tx, "Unhandled", { n: 1 });
          await outbox.publish(tx, "UserCreated", { userId: "u", email: "e" });
        });
        await waitFor(
          async () => (await deliveriesBy("completed")).length === 1,
        );
        const events = await h.getEvents();
        expect(
          events.find((e) => e.type === "Unhandled")?.fanned_out_at,
        ).toBeNull();
      });

      it("marks events with no handlers fanned out", async () => {
        startProcessor({ handlerMap: baseHandlers });
        await h.transaction((tx) => outbox.publish(tx, "Unhandled", { n: 1 }));
        await waitFor(
          async () => (await h.getEvents())[0].fanned_out_at !== null,
        );
        expect(await h.getDeliveries()).toHaveLength(0);
      });

      it("releases deliveries aborted by shutdown without consuming attempts", async () => {
        let started = 0;
        const processor = startProcessor({
          concurrency: 5,
          handlerMap: {
            ...baseHandlers,
            OrderPlaced: {
              sync: {
                concurrency: {
                  perKey: { key: (e) => e.data.accountId, limit: 1 },
                },
                handler: async (_, { signal }) => {
                  started++;
                  await new Promise((resolve) =>
                    signal.addEventListener("abort", resolve),
                  );
                  throw new Error("aborted");
                },
              },
            },
          },
        });
        await h.transaction(async (tx) => {
          for (let i = 0; i < 3; i++) {
            await outbox.publish(tx, "OrderPlaced", {
              orderId: `o${i}`,
              accountId: "same",
            });
          }
        });
        await waitFor(() => started === 1);
        await delay(50);
        await processor.stop();
        const deliveries = await h.getDeliveries();
        expect(deliveries).toHaveLength(3);
        // The running handler honored the abort signal and was released too
        expect(
          deliveries.every((d) => d.status === "pending" && d.attempts === 0),
        ).toBe(true);
      });

      it("picks up new events immediately on wakeup", async () => {
        if (!h.createWakeupEmitter) return;
        const wakeupEmitter = await h.createWakeupEmitter();
        const processed: number[] = [];
        startProcessor({
          pollingIntervalMs: 60_000,
          wakeupEmitter,
          wakeupThrottleMs: 10,
          handlerMap: {
            ...baseHandlers,
            UserCreated: { email: async () => void processed.push(Date.now()) },
          },
        });
        await delay(100);
        const publishedAt = Date.now();
        await publishUsers(1);
        await waitFor(() => processed.length === 1, { timeoutMs: 2_000 });
        expect(processed[0] - publishedAt).toBeLessThan(1_000);
        await wakeupEmitter.close();
      });
    });
  });
};
