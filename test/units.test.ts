import { describe, expect, it } from "vitest";
import { compileHandlerMap, defaultBackoff } from "../src/handlers.js";
import { createEventProcessor } from "../src/processor.js";
import {
  createMemoryStore,
  type MemoryStore,
  type MemoryTx,
} from "../src/memory/index.js";
import { createOutbox } from "../src/outbox.js";
import { delay, schema, silentLogger, waitFor } from "./helpers.js";

describe("compileHandlerMap", () => {
  const fn = async () => {};

  it("normalizes function and object definitions with defaults", () => {
    const compiled = compileHandlerMap(
      {
        A: {
          plain: fn,
          configured: {
            handler: fn,
            concurrency: 3,
            priority: 7,
            timeoutMs: 100,
          },
        },
      },
      { retry: { maxAttempts: 9 }, timeoutMs: 50 },
    );
    const [plain, configured] = compiled.get("A")!;
    expect(plain).toMatchObject({
      name: "plain",
      concurrency: undefined,
      maxAttempts: 9,
      timeoutMs: 50,
    });
    expect(plain.priority({})).toBe(0);
    expect(configured).toMatchObject({ concurrency: 3, timeoutMs: 100 });
    expect(configured.priority({})).toBe(7);
  });

  it.each([
    [{ handler: fn, concurrency: 0 }, "concurrency.limit"],
    [
      { handler: fn, concurrency: { perKey: { key: () => "k", limit: 1.5 } } },
      "perKey.limit",
    ],
    [
      { handler: fn, rateLimit: { max: 1, durationMs: 0 } },
      "rateLimit.durationMs",
    ],
    [{ handler: fn, retry: { maxAttempts: 0 } }, "retry.maxAttempts"],
    [{ handler: fn, timeoutMs: -1 }, "timeoutMs"],
    [{ handler: "nope" }, "handler must be a function"],
  ])("rejects invalid config %#", (definition, message) => {
    expect(() => compileHandlerMap({ A: { h: definition as any } })).toThrow(
      message,
    );
  });

  it("rejects handler names containing ':'", () => {
    expect(() => compileHandlerMap({ A: { "a:b": fn } })).toThrow(
      "cannot contain ':'",
    );
  });
});

describe("defaultBackoff", () => {
  it("doubles from 1s and caps at 60s", () => {
    expect(defaultBackoff({ attempt: 1 })).toBe(1000);
    expect(defaultBackoff({ attempt: 3 })).toBe(4000);
    expect(defaultBackoff({ attempt: 20 })).toBe(60_000);
  });
});

describe("EventProcessor", () => {
  const store = createMemoryStore();

  it("validates options at construction", () => {
    expect(() =>
      createEventProcessor({
        store,
        eventSchemas: {},
        handlerMap: {},
        concurrency: 0,
      }),
    ).toThrow("concurrency");
    expect(() =>
      createEventProcessor({
        store,
        eventSchemas: {},
        handlerMap: {},
        leaseMs: 0,
      }),
    ).toThrow("leaseMs");
  });

  it("ignores start/stop from the wrong state", async () => {
    const warnings: unknown[] = [];
    const logger = {
      debug() {},
      info() {},
      error() {},
      warn: (m: unknown) => warnings.push(m),
    };
    const processor = createEventProcessor({
      store,
      eventSchemas: {},
      handlerMap: {},
      logger,
    });
    await processor.stop();
    processor.start();
    processor.start();
    await processor.stop();
    expect(warnings).toEqual([
      "cannot stop processor from 'stopped'",
      "cannot start processor from 'started'",
    ]);
  });
});

describe("EventProcessor with a faulty store", () => {
  const eventSchemas = { E: schema<{ k: string }>() };

  const setup = async (
    overrides: (store: MemoryStore) => Partial<MemoryStore>,
    handlerMap: Parameters<
      typeof createEventProcessor<typeof eventSchemas, MemoryTx>
    >[0]["handlerMap"],
    opts: Partial<
      Parameters<typeof createEventProcessor<typeof eventSchemas, MemoryTx>>[0]
    > = {},
  ) => {
    const base = createMemoryStore();
    const store = { ...base, ...overrides(base) };
    const errors: unknown[] = [];
    const logger = { ...silentLogger, error: (m: unknown) => errors.push(m) };
    const processor = createEventProcessor({
      store,
      eventSchemas,
      handlerMap,
      pollingIntervalMs: 10,
      logger,
      ...opts,
    });
    const outbox = createOutbox({ eventSchemas, store });
    await base.transaction((tx) => outbox.publish(tx, "E", { k: "a" }));
    processor.start();
    return { base, processor, errors };
  };

  it("recovers from poll errors", async () => {
    let failures = 0;
    const { base, processor, errors } = await setup(
      (s) => ({
        claimEventsForFanout: async (opts) => {
          if (failures++ < 2) throw new Error("db down");
          return s.claimEventsForFanout(opts);
        },
      }),
      { E: { h: async () => {} } },
    );
    await waitFor(() =>
      base.getDeliveries().some((d) => d.status === "completed"),
    );
    await processor.stop();
    expect(errors).toContainEqual(
      expect.objectContaining({ error: expect.any(Error) }),
    );
  });

  it("aborts the handler and discards its result when the lease is lost", async () => {
    let abortReason: unknown;
    const writes: number[] = [];
    const { processor } = await setup(
      (s) => ({
        // Every renewal reports the lease as lost
        extendLeases: async (items) => items,
        completeDelivery: async (delivery) => {
          writes.push(delivery.attempts);
          return s.completeDelivery(delivery);
        },
        failDelivery: async (delivery, failure, opts) => {
          writes.push(delivery.attempts);
          return s.failDelivery(delivery, failure, opts);
        },
      }),
      {
        E: {
          h: async (_, { signal }) => {
            await new Promise((resolve) =>
              signal.addEventListener("abort", resolve),
            );
            // The delivery is re-claimed after its lease expires; keep the first attempt's reason
            abortReason ??= signal.reason;
          },
        },
      },
      { leaseMs: 30 },
    );
    await waitFor(() => abortReason !== undefined);
    await delay(20);
    await processor.stop();
    expect((abortReason as Error).name).toBe("LeaseLostError");
    // The first attempt finished after losing its lease, so its result was never written
    expect(writes).not.toContain(1);
  });

  it("logs and keeps running when renewing leases fails", async () => {
    let release!: () => void;
    const { processor, errors } = await setup(
      () => ({
        extendLeases: async () => {
          throw new Error("renew failed");
        },
      }),
      { E: { h: () => new Promise<void>((resolve) => (release = resolve)) } },
      { leaseMs: 30 },
    );
    await waitFor(() => errors.length > 0);
    release();
    await processor.stop();
    expect(errors[0]).toMatchObject({ error: { message: "renew failed" } });
  });

  it("leaves events unfanned when a concurrency key function throws", async () => {
    const { base, processor, errors } = await setup(() => ({}), {
      E: {
        h: {
          concurrency: {
            perKey: {
              key: () => {
                throw new Error("bad key");
              },
              limit: 1,
            },
          },
          handler: async () => {},
        },
      },
    });
    await waitFor(() => errors.length > 0);
    await processor.stop();
    expect(base.getEvents()[0].fanned_out_at).toBeNull();
    expect(base.getDeliveries()).toHaveLength(0);
  });

  it("fans out the rest of a batch when one event gets an invalid priority", async () => {
    const { base, processor, errors } = await setup(
      () => ({}),
      {
        E: {
          h: {
            priority: (event) => (event.data.k === "bad" ? 1.5 : 0),
            handler: async () => {},
          },
        },
      },
      { fanoutBatchSize: 10 },
    );
    const outbox = createOutbox({ eventSchemas, store: base });
    await base.transaction(async (tx) => {
      await outbox.publish(tx, "E", { k: "bad" });
      await outbox.publish(tx, "E", { k: "b" });
    });
    await waitFor(
      () =>
        base.getDeliveries().filter((d) => d.status === "completed").length ===
        2,
    );
    await processor.stop();
    expect(
      base
        .getEvents()
        .filter((e) => e.fanned_out_at === null)
        .map((e) => e.data),
    ).toEqual([{ k: "bad" }]);
    expect(errors).toContainEqual(
      expect.objectContaining({
        error: expect.objectContaining({
          message: expect.stringContaining("priority must be a 32-bit integer"),
        }),
      }),
    );
  });

  it("aborts handlers whose lease could not be renewed before it expired", async () => {
    let abortReason: unknown;
    const { processor } = await setup(
      () => ({
        extendLeases: async () => {
          throw new Error("renew failed");
        },
      }),
      {
        E: {
          h: ({}, { signal }) =>
            new Promise<void>((resolve) => {
              signal.addEventListener("abort", () => {
                // Later attempts are aborted by stop(); keep the first reason
                abortReason ??= signal.reason;
                resolve();
              });
            }),
        },
      },
      { leaseMs: 60 },
    );
    await waitFor(() => abortReason !== undefined);
    await processor.stop();
    expect((abortReason as Error).name).toBe("LeaseLostError");
  });

  it("keeps a reclaimed delivery's attempts apart when the old attempt is still running", async () => {
    const results: string[] = [];
    let finishFirst!: () => void;
    const { base, processor } = await setup(
      () => ({
        // Renewals fail, so the first lease lapses and this processor reclaims the delivery
        extendLeases: async () => {
          throw new Error("renew failed");
        },
      }),
      {
        E: {
          h: async ({}, { attempt }) => {
            if (attempt === 1) {
              // Ignores the abort signal and finishes late
              await new Promise<void>((resolve) => (finishFirst = resolve));
            }
            results.push(`attempt ${attempt} done`);
          },
        },
      },
      { leaseMs: 60 },
    );
    await waitFor(() =>
      base.getDeliveries().some((d) => d.status === "completed"),
    );
    finishFirst();
    await processor.stop();
    expect(base.getDeliveries()).toMatchObject([
      { status: "completed", attempts: 2 },
    ]);
    expect(results).toEqual(["attempt 2 done", "attempt 1 done"]);
  });

  it("releases deliveries claimed while stopping instead of orphaning their leases", async () => {
    let claimStarted = false;
    let handlerCalls = 0;
    const { base, processor } = await setup(
      (s) => ({
        claimDeliveries: async (opts) => {
          claimStarted = true;
          await delay(50);
          return s.claimDeliveries(opts);
        },
      }),
      { E: { h: async () => void handlerCalls++ } },
    );
    await waitFor(() => claimStarted);
    await processor.stop();
    // Let the in-flight claim settle had stop() not waited for it
    await delay(100);
    expect(handlerCalls).toBe(0);
    expect(base.getDeliveries()).toMatchObject([
      { status: "pending", attempts: 0, lease_token: null },
    ]);
  });

  it("rejects stop when handlers don't finish within the timeout", async () => {
    let release!: () => void;
    const { processor } = await setup(() => ({}), {
      E: { h: () => new Promise<void>((resolve) => (release = resolve)) },
    });
    await waitFor(() => release !== undefined);
    await expect(processor.stop({ timeoutMs: 20 })).rejects.toThrow(
      "shutdown timeout 20ms elapsed",
    );
    release();
  });

  it("records telemetry for polls, fan-out and deliveries", async () => {
    const counters = new Map<string, unknown[]>();
    const spans: string[] = [];
    const meter = {
      createCounter: (name: string) => ({
        add: (value: number, attributes?: unknown) =>
          counters.set(name, [
            ...(counters.get(name) ?? []),
            { value, attributes },
          ]),
      }),
      createHistogram: () => ({ record: () => {} }),
    };
    const tracer = {
      startSpan: (name: string) => {
        spans.push(name);
        return { end: () => {}, setAttributes: () => {}, setStatus: () => {} };
      },
    };
    const { base, processor } = await setup(
      () => ({}),
      { E: { h: async () => {} } },
      {
        telemetry: { meter, tracer, attributes: { service: "test" } },
      },
    );
    await waitFor(() =>
      base.getDeliveries().some((d) => d.status === "completed"),
    );
    await processor.stop();
    expect(counters.get("txob.fanout.count")).toContainEqual({
      value: 1,
      attributes: { service: "test" },
    });
    expect(counters.get("txob.delivery.processing.count")).toContainEqual({
      value: 1,
      attributes: {
        service: "test",
        "txob.event.type": "E",
        "txob.handler.name": "h",
        "txob.delivery.outcome": "success",
      },
    });
    expect(spans).toContain("txob.poll");
    expect(spans).toContain("txob.delivery.process");
  });
});

describe("EventProcessor.configure", () => {
  const eventSchemas = { E: schema<{ k: string }>() };

  const create = () => {
    const store = createMemoryStore();
    let running = 0;
    let maxRunning = 0;
    const processor = createEventProcessor({
      store,
      eventSchemas,
      pollingIntervalMs: 10,
      logger: silentLogger,
      handlerMap: {
        E: {
          work: {
            concurrency: 1,
            rateLimit: { max: 1000, durationMs: 1000 },
            timeoutMs: 1000,
            handler: async () => {
              maxRunning = Math.max(maxRunning, ++running);
              await delay(20);
              running--;
            },
          },
          plain: async () => {},
        },
      },
    });
    const outbox = createOutbox({ eventSchemas, store });
    return {
      store,
      processor,
      outbox,
      maxRunning: () => maxRunning,
      resetMax: () => (maxRunning = 0),
    };
  };

  it("applies raised limits to work that is already queued", async () => {
    const { store, processor, outbox, maxRunning, resetMax } = create();
    await store.transaction(async (tx) => {
      for (let i = 0; i < 30; i++) await outbox.publish(tx, "E", { k: `${i}` });
    });
    processor.start();
    await waitFor(
      () =>
        store.getDeliveries().filter((d) => d.status === "completed").length >=
        3,
    );
    expect(maxRunning()).toBe(1);

    resetMax();
    processor.configure({ handlers: { E: { work: { concurrency: 5 } } } });
    await waitFor(
      () =>
        store.getDeliveries().filter((d) => d.status === "completed").length ===
        60,
    );
    await processor.stop();
    expect(maxRunning()).toBe(5);
  });

  it("is declarative: unlisted handlers revert and repeating a config changes nothing", () => {
    const { processor } = create();
    const original = processor.getConfig();
    expect(original.handlers.E.work).toEqual({
      concurrency: 1,
      perKeyConcurrency: null,
      rateLimit: { max: 1000, durationMs: 1000 },
      maxAttempts: 5,
      timeoutMs: 1000,
    });

    const tuning = {
      concurrency: 40,
      handlers: {
        E: {
          work: {
            concurrency: null,
            rateLimit: null,
            timeoutMs: null,
            retry: { maxAttempts: 9 },
          },
        },
      },
    };
    processor.configure(tuning);
    const tuned = processor.getConfig();
    processor.configure(tuning);
    expect(processor.getConfig()).toEqual(tuned);
    expect(tuned).toEqual({
      concurrency: 40,
      handlers: {
        E: {
          work: {
            concurrency: null,
            perKeyConcurrency: null,
            rateLimit: null,
            maxAttempts: 9,
            timeoutMs: null,
          },
          plain: original.handlers.E.plain,
        },
      },
    });

    // Omitting `handlers` keeps the overrides; an empty map reverts to the definitions
    processor.configure({ concurrency: 20 });
    expect(processor.getConfig().handlers).toEqual(tuned.handlers);
    processor.configure({ handlers: {} });
    expect(processor.getConfig()).toEqual({ ...original, concurrency: 20 });
  });

  it.each([
    [{ concurrency: 0 }, "concurrency must be a positive integer"],
    [{ handlers: { E: { work: { concurrency: -1 } } } }, "concurrency.limit"],
    [
      { handlers: { E: { work: { perKeyConcurrency: 2 } } } },
      "requires concurrency.perKey.key",
    ],
    [
      { handlers: { E: { missing: { concurrency: 1 } } } },
      "unknown handler 'E.missing'",
    ],
    [{ handlers: { X: { work: {} } } }, "unknown handler 'X.work'"],
  ])(
    "rejects invalid config without applying any of it %#",
    (config, message) => {
      const { processor } = create();
      const before = processor.getConfig();
      expect(() => processor.configure(config as any)).toThrow(message);
      expect(processor.getConfig()).toEqual(before);
    },
  );
});
