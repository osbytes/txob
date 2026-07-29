import { describe, it, expect, vi, afterEach, expectTypeOf } from "vitest";
import type { StandardSchemaV1 } from "@standard-schema/spec";
import {
  createEventHandlerMap,
  createEventProcessor,
  EventProcessor,
  TxOBEvent,
  defaultBackoff,
} from "./processor.js";
import { TxOBError, ErrorUnprocessableEventHandler } from "./error.js";
import { sleep } from "./sleep.js";
import {
  TxOBTelemetryAttributeKey,
  TxOBTelemetryEventOutcome,
  TxOBTelemetryHandlerOutcome,
  TxOBTelemetryMetricName,
  TxOBTelemetrySpanName,
} from "./telemetry.js";
import * as sleepModule from "./sleep.js";

const mockTxClient = {
  getEventByIdForUpdateSkipLocked: vi.fn(),
  updateEvent: vi.fn(),
  createEvent: vi.fn(),
};
const mockClient = {
  getEventsToProcess: vi.fn(),
  transaction: vi.fn(async (fn) => fn(mockTxClient)),
};

const now = new Date();
vi.mock("./date", async (getOg) => {
  const mod = await getOg();
  return {
    ...(mod as Object),
    getDate: vi.fn(() => now),
  };
});

afterEach(() => {
  vi.clearAllMocks();
});

describe("createEventProcessor", () => {
  it("returns an EventProcessor wired with the same options", async () => {
    const handlerMap = {
      evtType1: { h1: vi.fn(() => Promise.resolve()) },
    };
    mockClient.getEventsToProcess.mockResolvedValue([]);

    const processor = createEventProcessor({
      client: mockClient,
      handlerMap,
      pollingIntervalMs: 10,
      eventSchemas: {
        evtType1: {
          "~standard": {
            version: 1,
            vendor: "test",
            validate: (value) => ({ value: value as Record<string, unknown> }),
          },
        },
      },
    });

    expect(processor).toBeInstanceOf(EventProcessor);
    processor.start();
    await sleep(30);
    await processor.stop();
    expect(mockClient.getEventsToProcess).toHaveBeenCalled();
  });
});

function createTestWakeupEmitter() {
  const listeners = new Map<string, Set<() => void>>();
  return {
    on: vi.fn((event: "wakeup", listener: () => void) => {
      if (event !== "wakeup") return;
      let set = listeners.get(event);
      if (!set) {
        set = new Set();
        listeners.set(event, set);
      }
      set.add(listener);
    }),
    off: vi.fn((event: "wakeup", listener: () => void) => {
      listeners.get(event)?.delete(listener);
    }),
    close: vi.fn(async () => {}),
    emitWakeup: () => {
      for (const l of listeners.get("wakeup") ?? []) l();
    },
  };
}

describe("EventProcessor - schema typing", () => {
  it("infers handler event data from Standard Schema outputs", () => {
    const createSchema = <
      TOutput extends Record<string, unknown>,
    >(): StandardSchemaV1<unknown, TOutput> => ({
      "~standard": {
        version: 1,
        vendor: "test",
        validate: (value) => ({ value: value as TOutput }),
      },
    });

    const eventSchemas = {
      UserCreated: createSchema<{ userId: string; email: string }>(),
      SubscriptionCancelled: createSchema<{
        subscriptionId: string;
        reason?: string;
      }>(),
    };

    const handlerMap = createEventHandlerMap({
      eventSchemas,
      handlerMap: {
        UserCreated: {
          sendWelcomeEmail: async (event) => {
            expectTypeOf(event.type).toEqualTypeOf<"UserCreated">();
            expectTypeOf(event.data).toEqualTypeOf<{
              userId: string;
              email: string;
            }>();
          },
        },
        SubscriptionCancelled: {
          syncBilling: async (event) => {
            expectTypeOf(event.type).toEqualTypeOf<"SubscriptionCancelled">();
            expectTypeOf(event.data).toEqualTypeOf<{
              subscriptionId: string;
              reason?: string;
            }>();
          },
        },
      },
    });

    expectTypeOf(handlerMap.UserCreated.sendWelcomeEmail).toBeFunction();
    expectTypeOf(handlerMap.SubscriptionCancelled.syncBilling).toBeFunction();
  });
});

describe("EventProcessor - processEvents", () => {
  it("does nothing when no events to process", async () => {
    const opts = {
      maxErrors: 5,
      backoff: () => now,
      pollingIntervalMs: 10,
    };
    const handlerMap = {};
    mockClient.getEventsToProcess.mockImplementation(() => Promise.resolve([]));
    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for polling
    await processor.stop();

    expect(mockClient.getEventsToProcess).toHaveBeenCalled();
    expect(mockClient.transaction).not.toHaveBeenCalled();
    expect(mockTxClient.getEventByIdForUpdateSkipLocked).not.toHaveBeenCalled();
    expect(mockTxClient.updateEvent).not.toHaveBeenCalled();
  });

  it("handles handler results and updates", async () => {
    const opts = {
      maxErrors: 5,
      backoff: vi.fn(),
      pollingIntervalMs: 10,
    };
    const err = new Error("some error");
    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.resolve()),
        handler2: vi.fn(() => Promise.reject(err)),
        handler3: vi.fn(() => Promise.resolve()),
      },
      evtType2: {
        handler1: vi.fn(() => Promise.resolve()),
      },
    };
    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {
        handler1: {
          errors: [{ error: err.message, timestamp: now }],
        },
        handler3: {
          processed_at: now,
        },
      },
      errors: opts.maxErrors - 1,
    };
    // should skip evt2 due to max errors
    const evt2: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "2",
      timestamp: now,
      data: {},
      correlation_id: "abc456",
      handler_results: {},
      errors: opts.maxErrors,
    };
    const evt3: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "3",
      timestamp: now,
      data: {},
      correlation_id: "abc789",
      handler_results: {},
      errors: 0,
    };
    // skip processed
    const evt4: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "4",
      timestamp: now,
      data: {},
      correlation_id: "xyz123",
      handler_results: {},
      errors: 0,
      processed_at: now,
    };
    const events = [evt1, evt2, evt3, evt4];
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      // Return events only on first call, then empty array
      return Promise.resolve(callCount === 1 ? events : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      if (id === evt3.id) return Promise.resolve(null);

      return Promise.resolve(events.find((e) => e.id === id) ?? null);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for processing
    await processor.stop();

    expect(mockClient.getEventsToProcess).toHaveBeenCalled();

    expect(mockClient.transaction).toHaveBeenCalledTimes(3);

    expect(handlerMap.evtType1.handler1).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler1).toHaveBeenCalledWith(evt1, {
      signal: expect.any(AbortSignal),
    });
    expect(handlerMap.evtType1.handler2).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler2).toHaveBeenCalledWith(evt1, {
      signal: expect.any(AbortSignal),
    });
    expect(handlerMap.evtType1.handler3).not.toHaveBeenCalled();

    expect(mockTxClient.getEventByIdForUpdateSkipLocked).toHaveBeenCalledTimes(
      3,
    );

    expect(opts.backoff).toHaveBeenCalledOnce();
    expect(opts.backoff).toHaveBeenCalledWith(
      expect.objectContaining({
        attempt: 5,
        error: err,
        errors: expect.arrayContaining([err]),
        event: expect.objectContaining({
          id: "1",
          errors: 5,
        }),
        maxErrors: opts.maxErrors,
      }),
    ); // evt.errors + 1

    expect(mockTxClient.updateEvent).toHaveBeenCalledTimes(1);
    expect(mockTxClient.updateEvent).toHaveBeenCalledWith({
      processed_at: now,
      backoff_until: null,
      correlation_id: "abc123",
      data: {},
      errors: 5,
      handler_results: {
        handler1: {
          errors: [
            {
              error: err.message,
              timestamp: now,
            },
          ],
          processed_at: now,
        },
        handler2: {
          errors: [
            {
              error: err.message,
              timestamp: now,
            },
          ],
        },
        handler3: {
          processed_at: now,
        },
      },
      id: "1",
      timestamp: now,
      type: "evtType1",
    });
  });

  it("process event if no handler errors encountered", async () => {
    const opts = {
      maxErrors: 5,
      backoff: vi.fn(),
      pollingIntervalMs: 10,
    };
    const err = new Error("some error");
    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.resolve()),
        handler2: vi.fn(() => Promise.resolve()),
      },
    };
    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {
        handler1: {
          errors: [{ error: err.message, timestamp: now }],
        },
        handler2: {
          processed_at: now,
        },
      },
      errors: 1,
    };
    const events = [evt1];
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? events : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      return Promise.resolve(events.find((e) => e.id === id) ?? null);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for processing
    await processor.stop();

    expect(mockClient.getEventsToProcess).toHaveBeenCalled();

    expect(mockClient.transaction).toHaveBeenCalledTimes(1);

    expect(handlerMap.evtType1.handler1).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler1).toHaveBeenCalledWith(evt1, {
      signal: expect.any(AbortSignal),
    });
    expect(mockTxClient.getEventByIdForUpdateSkipLocked).toHaveBeenCalledTimes(
      1,
    );

    expect(mockTxClient.updateEvent).toHaveBeenCalledTimes(1);
    expect(mockTxClient.updateEvent).toHaveBeenCalledWith({
      backoff_until: null,
      correlation_id: "abc123",
      data: {},
      errors: 1,
      handler_results: {
        handler1: {
          errors: [
            {
              error: err.message,
              timestamp: now,
            },
          ],
          processed_at: now,
        },
        handler2: {
          processed_at: now,
        },
      },
      id: "1",
      timestamp: now,
      type: "evtType1",
      processed_at: now,
    });
  });

  it("respects ErrorUnprocessableEventHandler sentinel error to stop handler processing", async () => {
    const opts = {
      maxErrors: 5,
      backoff: vi.fn(),
      onEventMaxErrorsReached: vi.fn(),
      pollingIntervalMs: 10,
    };
    const err = new Error("some error");
    const errUnprocessable = new ErrorUnprocessableEventHandler(
      new Error("err1"),
    );
    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.reject(errUnprocessable)),
        handler2: vi.fn(() => Promise.reject(errUnprocessable)),
      },
    };
    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {
        handler1: {
          errors: [{ error: err.message, timestamp: now }],
        },
        handler2: {
          processed_at: now,
        },
        handler3: {
          unprocessable_at: now,
          errors: [
            {
              error: errUnprocessable.message,
              timestamp: now,
            },
          ],
        },
      },
      errors: 1,
    };
    const events = [evt1];
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? events : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      return Promise.resolve(events.find((e) => e.id === id) ?? null);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for processing
    await processor.stop();

    expect(mockClient.getEventsToProcess).toHaveBeenCalled();

    expect(mockClient.transaction).toHaveBeenCalledTimes(1);

    expect(handlerMap.evtType1.handler1).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler1).toHaveBeenCalledWith(evt1, {
      signal: expect.any(AbortSignal),
    });
    expect(mockTxClient.getEventByIdForUpdateSkipLocked).toHaveBeenCalledTimes(
      1,
    );

    // Should call onEventMaxErrorsReached since all remaining handlers are unprocessable
    expect(opts.onEventMaxErrorsReached).toHaveBeenCalledOnce();
    expect(opts.onEventMaxErrorsReached).toHaveBeenCalledWith({
      event: expect.objectContaining({
        id: "1",
        errors: opts.maxErrors,
      }),
      txClient: mockTxClient,
      signal: expect.any(AbortSignal),
    });

    expect(mockTxClient.updateEvent).toHaveBeenCalledTimes(1);
    expect(mockTxClient.updateEvent).toHaveBeenCalledWith({
      backoff_until: null,
      correlation_id: "abc123",
      data: {},
      errors: opts.maxErrors,
      handler_results: {
        handler1: {
          unprocessable_at: now,
          errors: [
            { error: err.message, timestamp: now },
            { error: errUnprocessable.message, timestamp: now },
          ],
        },
        handler2: {
          processed_at: now,
        },
        handler3: {
          unprocessable_at: now,
          errors: [
            {
              error: errUnprocessable.message,
              timestamp: now,
            },
          ],
        },
      },
      id: "1",
      timestamp: now,
      type: "evtType1",
      processed_at: now,
    });
  });

  it("sets errors to maxErrors when all handlers are unprocessable and calls onEventMaxErrorsReached", async () => {
    const opts = {
      maxErrors: 5,
      backoff: vi.fn(),
      onEventMaxErrorsReached: vi.fn(),
      pollingIntervalMs: 10,
    };
    const errUnprocessable1 = new ErrorUnprocessableEventHandler(
      new Error("err1"),
    );
    const errUnprocessable2 = new ErrorUnprocessableEventHandler(
      new Error("err2"),
    );
    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.reject(errUnprocessable1)),
        handler2: vi.fn(() => Promise.reject(errUnprocessable2)),
      },
    };
    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {},
      errors: 0,
    };
    const events = [evt1];
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? events : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      return Promise.resolve(events.find((e) => e.id === id) ?? null);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for processing
    await processor.stop();

    expect(mockClient.getEventsToProcess).toHaveBeenCalled();
    expect(mockClient.transaction).toHaveBeenCalledTimes(1);
    expect(handlerMap.evtType1.handler1).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler2).toHaveBeenCalledOnce();

    // Backoff is called even when reaching maxErrors (then backoff_until is nulled)
    expect(opts.backoff).toHaveBeenCalledOnce();
    expect(opts.backoff).toHaveBeenCalledWith(
      expect.objectContaining({
        attempt: opts.maxErrors,
        maxErrors: opts.maxErrors,
      }),
    );

    // Should call the maxErrors callback
    expect(opts.onEventMaxErrorsReached).toHaveBeenCalledOnce();
    expect(opts.onEventMaxErrorsReached).toHaveBeenCalledWith({
      event: expect.objectContaining({
        id: "1",
        errors: opts.maxErrors,
      }),
      txClient: mockTxClient,
      signal: expect.any(AbortSignal),
    });

    expect(mockTxClient.updateEvent).toHaveBeenCalledWith({
      backoff_until: null,
      correlation_id: "abc123",
      data: {},
      errors: opts.maxErrors,
      handler_results: {
        handler1: {
          unprocessable_at: now,
          errors: [
            {
              error: errUnprocessable1.message,
              timestamp: now,
            },
          ],
        },
        handler2: {
          unprocessable_at: now,
          errors: [
            {
              error: errUnprocessable2.message,
              timestamp: now,
            },
          ],
        },
      },
      id: "1",
      timestamp: now,
      type: "evtType1",
      processed_at: now,
    });
  });

  it("sets errors to maxErrors when some handlers succeed and remaining are unprocessable", async () => {
    const opts = {
      maxErrors: 5,
      backoff: vi.fn(),
      onEventMaxErrorsReached: vi.fn(),
      pollingIntervalMs: 10,
    };
    const errUnprocessable = new ErrorUnprocessableEventHandler(
      new Error("err1"),
    );
    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.resolve()),
        handler2: vi.fn(() => Promise.reject(errUnprocessable)),
      },
    };
    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {},
      errors: 0,
    };
    const events = [evt1];
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? events : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      return Promise.resolve(events.find((e) => e.id === id) ?? null);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for processing
    await processor.stop();

    expect(mockClient.getEventsToProcess).toHaveBeenCalled();
    expect(mockClient.transaction).toHaveBeenCalledTimes(1);
    expect(handlerMap.evtType1.handler1).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler2).toHaveBeenCalledOnce();

    // Should call backoff even when jumping to maxErrors
    expect(opts.backoff).toHaveBeenCalledWith(
      expect.objectContaining({
        attempt: opts.maxErrors,
        maxErrors: opts.maxErrors,
      }),
    );

    // Should call the maxErrors callback since all remaining handlers are unprocessable
    expect(opts.onEventMaxErrorsReached).toHaveBeenCalledOnce();

    expect(mockTxClient.updateEvent).toHaveBeenCalledWith({
      backoff_until: null,
      correlation_id: "abc123",
      data: {},
      errors: opts.maxErrors,
      handler_results: {
        handler1: {
          errors: [],
          processed_at: now,
        },
        handler2: {
          unprocessable_at: now,
          errors: [
            {
              error: errUnprocessable.message,
              timestamp: now,
            },
          ],
        },
      },
      id: "1",
      timestamp: now,
      type: "evtType1",
      processed_at: now,
    });
  });

  it("does not set errors to maxErrors when remaining handlers have retryable errors", async () => {
    const opts = {
      maxErrors: 5,
      backoff: vi.fn(() => now),
      onEventMaxErrorsReached: vi.fn(),
      pollingIntervalMs: 10,
    };
    const retryableError = new Error("temporary failure");
    const errUnprocessable = new ErrorUnprocessableEventHandler(
      new Error("err1"),
    );
    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.resolve()),
        handler2: vi.fn(() => Promise.reject(errUnprocessable)),
        handler3: vi.fn(() => Promise.reject(retryableError)),
      },
    };
    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {},
      errors: 0,
    };
    const events = [evt1];
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? events : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      return Promise.resolve(events.find((e) => e.id === id) ?? null);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for processing
    await processor.stop();

    expect(mockClient.getEventsToProcess).toHaveBeenCalled();
    expect(mockClient.transaction).toHaveBeenCalledTimes(1);
    expect(handlerMap.evtType1.handler1).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler2).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler3).toHaveBeenCalledOnce();

    // Should call backoff normally since handler3 has retryable error
    expect(opts.backoff).toHaveBeenCalledWith(
      expect.objectContaining({
        attempt: 1,
        error: retryableError,
        errors: expect.arrayContaining([retryableError]),
      }),
    );

    // Should NOT call the maxErrors callback since errors < maxErrors
    expect(opts.onEventMaxErrorsReached).not.toHaveBeenCalled();

    expect(mockTxClient.updateEvent).toHaveBeenCalledWith({
      backoff_until: expect.any(Date),
      correlation_id: "abc123",
      data: {},
      errors: 1,
      handler_results: {
        handler1: {
          errors: [],
          processed_at: now,
        },
        handler2: {
          unprocessable_at: now,
          errors: [
            {
              error: errUnprocessable.message,
              timestamp: now,
            },
          ],
        },
        handler3: {
          errors: [
            {
              error: retryableError.message,
              timestamp: now,
            },
          ],
        },
      },
      id: "1",
      timestamp: now,
      type: "evtType1",
    });
  });

  it("should throw and log when onEventMaxErrorsReached hook throws an error", async () => {
    const hookError = new Error("hook error");
    const logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };
    const opts = {
      maxErrors: 5,
      backoff: vi.fn(),
      onEventMaxErrorsReached: vi.fn(() => {
        throw hookError;
      }),
      logger,
      pollingIntervalMs: 10,
    };
    const errUnprocessable = new ErrorUnprocessableEventHandler(
      new Error("err1"),
    );
    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.reject(errUnprocessable)),
      },
    };
    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {},
      errors: 0,
    };
    const events = [evt1];
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? events : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      return Promise.resolve(events.find((e) => e.id === id) ?? null);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for processing
    await processor.stop();

    expect(opts.onEventMaxErrorsReached).toHaveBeenCalledOnce();
    expect(logger.error).toHaveBeenCalledWith(
      {
        eventId: "1",
        error: hookError,
      },
      "error in onEventMaxErrorsReached hook",
    );
    expect(logger.error).toHaveBeenCalledWith(
      {
        eventId: "1",
        error: hookError,
      },
      "error processing event",
    );
  });

  it("should use the latest backoff when multiple TxOBErrors have different backoffUntil dates", async () => {
    const opts = {
      maxErrors: 5,
      backoff: vi.fn(() => new Date(now.getTime() + 5000)), // Default backoff: 5 seconds
      pollingIntervalMs: 10,
    };

    // Create different backoff times
    const backoff1 = new Date(now.getTime() + 10000); // 10 seconds
    const backoff2 = new Date(now.getTime() + 20000); // 20 seconds (latest)
    const backoff3 = new Date(now.getTime() + 15000); // 15 seconds

    const error1 = new TxOBError("error 1", { backoffUntil: backoff1 });
    const error2 = new TxOBError("error 2", { backoffUntil: backoff2 });
    const error3 = new TxOBError("error 3", { backoffUntil: backoff3 });

    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.reject(error1)),
        handler2: vi.fn(() => Promise.reject(error2)),
        handler3: vi.fn(() => Promise.reject(error3)),
      },
    };

    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {},
      errors: 0,
    };

    const events = [evt1];
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? events : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      return Promise.resolve(events.find((e) => e.id === id) ?? null);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for processing
    await processor.stop();

    expect(mockClient.getEventsToProcess).toHaveBeenCalled();
    expect(mockClient.transaction).toHaveBeenCalledTimes(1);

    // All handlers should have been called
    expect(handlerMap.evtType1.handler1).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler2).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler3).toHaveBeenCalledOnce();

    // Default backoff should also be called
    expect(opts.backoff).toHaveBeenCalledWith(
      expect.objectContaining({
        attempt: 1,
        maxErrors: opts.maxErrors,
      }),
    );

    // The latest backoff (backoff2 = 20 seconds) should be used
    expect(mockTxClient.updateEvent).toHaveBeenCalledTimes(1);
    const updateCall = mockTxClient.updateEvent.mock.calls[0][0];
    expect(updateCall.backoff_until).toEqual(backoff2);
    expect(updateCall.errors).toBe(1);
  });

  it("should use the latest backoff when TxOBError backoff is later than default backoff", async () => {
    const laterBackoff = new Date(now.getTime() + 30000); // 30 seconds
    const defaultBackoffTime = new Date(now.getTime() + 5000); // 5 seconds

    const opts = {
      maxErrors: 5,
      backoff: vi.fn(() => defaultBackoffTime),
      pollingIntervalMs: 10,
    };

    const error = new TxOBError("error with backoff", {
      backoffUntil: laterBackoff,
    });

    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.reject(error)),
      },
    };

    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {},
      errors: 0,
    };

    const events = [evt1];
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? events : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      return Promise.resolve(events.find((e) => e.id === id) ?? null);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for processing
    await processor.stop();

    expect(mockClient.transaction).toHaveBeenCalledTimes(1);
    expect(opts.backoff).toHaveBeenCalledWith(
      expect.objectContaining({
        attempt: 1,
        error,
        errors: expect.arrayContaining([error]),
      }),
    );

    // The latest backoff (laterBackoff = 30 seconds) should be used, not the default (5 seconds)
    const updateCall = mockTxClient.updateEvent.mock.calls[0][0];
    expect(updateCall.backoff_until).toEqual(laterBackoff);
    expect(updateCall.backoff_until?.getTime()).toBeGreaterThan(
      defaultBackoffTime.getTime(),
    );
  });

  it("should use default backoff when TxOBError backoff is earlier than default backoff", async () => {
    const earlierBackoff = new Date(now.getTime() + 2000); // 2 seconds
    const defaultBackoffTime = new Date(now.getTime() + 5000); // 5 seconds

    const opts = {
      maxErrors: 5,
      backoff: vi.fn(() => defaultBackoffTime),
      pollingIntervalMs: 10,
    };

    const error = new TxOBError("error with backoff", {
      backoffUntil: earlierBackoff,
    });

    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.reject(error)),
      },
    };

    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {},
      errors: 0,
    };

    const events = [evt1];
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? events : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      return Promise.resolve(events.find((e) => e.id === id) ?? null);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for processing
    await processor.stop();

    expect(mockClient.transaction).toHaveBeenCalledTimes(1);
    expect(opts.backoff).toHaveBeenCalledWith(
      expect.objectContaining({
        attempt: 1,
        error,
        errors: expect.arrayContaining([error]),
      }),
    );

    // The latest backoff (defaultBackoffTime = 5 seconds) should be used, not the earlier one (2 seconds)
    const updateCall = mockTxClient.updateEvent.mock.calls[0][0];
    expect(updateCall.backoff_until).toEqual(defaultBackoffTime);
    expect(updateCall.backoff_until?.getTime()).toBeGreaterThan(
      earlierBackoff.getTime(),
    );
  });
});

describe("defaultBackoff", () => {
  it("should calculate a backoff", () => {
    const backoff = defaultBackoff({
      attempt: 3,
      error: new Error("test"),
      errors: [],
      event: {
        id: "event-1",
        type: "evtType1",
        timestamp: now,
        data: {},
        correlation_id: "abc123",
        handler_results: {},
        errors: 3,
      },
      maxErrors: 5,
    });
    const actual = backoff.getTime();
    const expected = Date.now() + 1000 * 2 ** 3;
    const diff = Math.abs(actual - expected);

    expect(diff).lessThanOrEqual(1);
  });

  it("should cap backoff at maxDelayMs for large error counts", () => {
    const maxDelayMs = 1000 * 60; // 60 seconds
    const backoff = defaultBackoff({
      attempt: 20,
      error: new Error("test"),
      errors: [],
      event: {
        id: "event-1",
        type: "evtType1",
        timestamp: now,
        data: {},
        correlation_id: "abc123",
        handler_results: {},
        errors: 20,
      },
      maxErrors: 30,
    }); // Large error count that would exceed max
    const actual = backoff.getTime();
    const expected = Date.now() + maxDelayMs;
    const diff = Math.abs(actual - expected);

    expect(diff).lessThanOrEqual(1);
  });
});

describe("EventProcessor - lifecycle", () => {
  it("should shutdown gracefully", async () => {
    let calls = 0;
    let aborted = false;
    const handlerMap = {
      evtType1: {
        handler1: vi.fn((event, { signal }) => {
          calls++;
          return new Promise<void>((r) => {
            signal.addEventListener("abort", () => {
              aborted = true;
              r();
            });
          });
        }),
      },
    };
    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {},
      errors: 0,
    };
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation((opts) => {
      // Check if signal is aborted - reject immediately to be caught by polling loop
      if (opts?.signal?.aborted) {
        return Promise.reject(new DOMException("Aborted", "AbortError"));
      }
      callCount++;
      return Promise.resolve(callCount === 1 ? [evt1] : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation(() =>
      Promise.resolve(evt1),
    );
    mockTxClient.updateEvent.mockImplementation(() => Promise.resolve());

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      pollingIntervalMs: 10,
    });
    processor.start();
    await sleep(20); // Let it start processing
    await processor.stop();
    // The stop() method now waits for the polling loop to complete,
    // ensuring any pending rejections are caught by the polling loop's catch block

    expect(calls).toBe(1);
    expect(aborted).toBe(true);
  });
  it("should respect shutdown timeout and throw", async () => {
    const logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };
    const handlerMap = {
      evtType1: {
        // Never resolves so work would stay pending if the queue did not abort it
        handler1: vi.fn(() => new Promise<void>(() => {})),
      },
    };
    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {},
      errors: 0,
    };
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation((opts) => {
      if (opts?.signal?.aborted) {
        return Promise.reject(new DOMException("Aborted", "AbortError"));
      }
      callCount++;
      return Promise.resolve(callCount === 1 ? [evt1] : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation(() =>
      Promise.resolve(evt1),
    );
    mockTxClient.updateEvent.mockImplementation(() => Promise.resolve());

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      pollingIntervalMs: 10,
      logger,
    });
    processor.start();

    for (let i = 0; i < 100; i++) {
      if (handlerMap.evtType1.handler1.mock.calls.length > 0) break;
      await sleep(5);
    }
    expect(handlerMap.evtType1.handler1).toHaveBeenCalled();

    // Abort clears in-flight queue work quickly, so onPendingZero() normally settles
    // before the shutdown timer. Stub it to model a hung queue and exercise the
    // timeout + error logging path.
    const queue = (
      processor as unknown as { queue: { onPendingZero: () => Promise<void> } }
    ).queue;
    vi.spyOn(queue, "onPendingZero").mockImplementation(
      () => new Promise<void>(() => {}),
    );

    const start = Date.now();
    await expect(processor.stop({ timeoutMs: 40 })).rejects.toThrow(
      "shutdown timeout 40ms elapsed",
    );
    expect(Date.now() - start).toBeLessThan(500);
    expect(logger.error).toHaveBeenCalledWith(
      { error: expect.any(Error) },
      "shutdown error",
    );
  });
  it("should warn when stopping a processor that is not started", async () => {
    const logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };
    const processor = new EventProcessor({
      client: mockClient,
      handlerMap: {},
      pollingIntervalMs: 10,
      logger,
    });

    await processor.stop();

    expect(logger.warn).toHaveBeenCalledWith(
      "cannot stop processor from 'stopped'",
    );
  });
  it("should handle shutdown when processor completes before stop is called", async () => {
    let resolve: (() => void) | null = null;
    const promise = new Promise<void>((r) => {
      resolve = r;
    });

    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => promise),
      },
    };
    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {},
      errors: 0,
    };
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation((opts) => {
      // Respect abort signal to prevent unhandled rejections
      if (opts?.signal?.aborted) {
        return Promise.reject(new DOMException("Aborted", "AbortError"));
      }
      callCount++;
      return Promise.resolve(callCount === 1 ? [evt1] : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation(() =>
      Promise.resolve(evt1),
    );
    mockTxClient.updateEvent.mockImplementation(() => Promise.resolve());

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      pollingIntervalMs: 10,
    });
    processor.start();

    // Complete the processor's work
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resolve!();

    // Wait a bit for the processor to complete
    await sleep(20);

    // Now stop should handle the already-completed case
    await processor.stop();
    await sleep(10); // Let abort operations settle
  });
  it("should warn when starting a processor that is already started", () => {
    const logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };
    const processor = new EventProcessor({
      client: mockClient,
      handlerMap: {},
      pollingIntervalMs: 10,
      logger,
    });

    processor.start();
    processor.start(); // Try to start again

    expect(logger.warn).toHaveBeenCalledWith(
      "cannot start processor from 'started'",
    );
  });
  it("should handle non-abort errors and continue processing", async () => {
    let calls = 0;
    const logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };
    const error = new Error("processing error");

    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => {
          calls++;
          if (calls === 1) {
            throw error;
          }
          return Promise.resolve();
        }),
      },
    };
    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {},
      errors: 0,
    };
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? [evt1] : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation(() =>
      Promise.resolve(evt1),
    );
    mockTxClient.updateEvent.mockImplementation(() => Promise.resolve());

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      pollingIntervalMs: 10,
      logger,
    });
    processor.start();

    // Wait for the error to be logged and processing to continue
    await sleep(50);

    await processor.stop();

    expect(logger.error).toHaveBeenCalled();
    expect(calls).toBeGreaterThan(0);
  });
});

describe("EventProcessor - wakeup and polling edge cases", () => {
  it("skips a poll when the previous poll is still awaiting the client", async () => {
    const logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };
    let releaseGetEvents!: (value: { id: string; errors: number }[]) => void;
    const getEventsBlocked = new Promise<{ id: string; errors: number }[]>(
      (resolve) => {
        releaseGetEvents = resolve;
      },
    );
    mockClient.getEventsToProcess.mockImplementationOnce(
      () => getEventsBlocked,
    );

    const wakeup = createTestWakeupEmitter();
    const processor = new EventProcessor({
      client: mockClient,
      handlerMap: {},
      pollingIntervalMs: 1000,
      wakeupEmitter: wakeup,
      wakeupThrottleMs: 0,
      wakeupTimeoutMs: 60_000,
      logger,
    });
    processor.start();

    await vi.waitFor(
      () => {
        expect(mockClient.getEventsToProcess).toHaveBeenCalled();
      },
      { timeout: 2000 },
    );

    wakeup.emitWakeup();
    await sleep(15);

    expect(logger.debug).toHaveBeenCalledWith(
      "skipping poll - already polling",
    );

    releaseGetEvents([]);
    await processor.stop();
  });

  it("skips polling when the in-memory queue is at capacity", async () => {
    const logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };
    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => new Promise<void>(() => {})),
      },
    };
    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {},
      errors: 0,
    };
    mockClient.getEventsToProcess.mockImplementation((opts) => {
      if (opts?.signal?.aborted) {
        return Promise.reject(new DOMException("Aborted", "AbortError"));
      }
      return Promise.resolve([evt1]);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation(() =>
      Promise.resolve(evt1),
    );
    mockTxClient.updateEvent.mockImplementation(() => Promise.resolve());

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      pollingIntervalMs: 15,
      maxQueuedEvents: 1,
      logger,
    });
    processor.start();

    await vi.waitFor(
      () => {
        expect(logger.debug).toHaveBeenCalledWith(
          expect.objectContaining({
            queuedCount: 1,
            maxQueuedEvents: 1,
          }),
          "skipping poll - queue at capacity",
        );
      },
      { timeout: 3000 },
    );

    await processor.stop();
  });

  it("logs and continues when getEventsToProcess rejects", async () => {
    const logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };
    const pollError = new Error("database unavailable");
    let calls = 0;
    mockClient.getEventsToProcess.mockImplementation((opts) => {
      if (opts?.signal?.aborted) {
        return Promise.reject(new DOMException("Aborted", "AbortError"));
      }
      calls++;
      if (calls === 1) return Promise.reject(pollError);
      return Promise.resolve([]);
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap: {},
      pollingIntervalMs: 10,
      logger,
    });
    processor.start();

    await vi.waitFor(
      () => {
        expect(logger.error).toHaveBeenCalledWith(
          { error: pollError },
          "error polling for events, will retry",
        );
      },
      { timeout: 2000 },
    );

    expect(calls).toBeGreaterThanOrEqual(2);
    await processor.stop();
  });

  it("runs fallback polling when no wakeup arrives within wakeupTimeoutMs", async () => {
    const logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };
    mockClient.getEventsToProcess.mockImplementation((opts) => {
      if (opts?.signal?.aborted) {
        return Promise.reject(new DOMException("Aborted", "AbortError"));
      }
      return Promise.resolve([]);
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap: {},
      pollingIntervalMs: 15,
      wakeupEmitter: createTestWakeupEmitter(),
      wakeupThrottleMs: 60_000,
      wakeupTimeoutMs: 5,
      logger,
    });
    processor.start();

    await vi.waitFor(
      () => {
        expect(logger.debug).toHaveBeenCalledWith(
          expect.objectContaining({
            timeSinceLastWakeup: expect.any(Number),
            wakeupTimeoutMs: 5,
          }),
          "fallback poll triggered - no wakeup signal received",
        );
      },
      { timeout: 3000 },
    );

    await processor.stop();
  });

  it("skips fallback polling after a recent wakeup", async () => {
    const logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };
    mockClient.getEventsToProcess.mockResolvedValue([]);
    const wakeup = createTestWakeupEmitter();
    const processor = new EventProcessor({
      client: mockClient,
      handlerMap: {},
      pollingIntervalMs: 25,
      wakeupEmitter: wakeup,
      wakeupThrottleMs: 0,
      wakeupTimeoutMs: 60_000,
      logger,
    });
    processor.start();
    await sleep(5);
    wakeup.emitWakeup();
    await sleep(35);

    await vi.waitFor(
      () => {
        expect(logger.debug).toHaveBeenCalledWith(
          expect.objectContaining({
            timeSinceLastWakeup: expect.any(Number),
            wakeupTimeoutMs: 60_000,
          }),
          "skipping fallback poll - wakeup signal received recently",
        );
      },
      { timeout: 3000 },
    );

    await processor.stop();
  });

  it("unregisters the wakeup listener and cancels throttle timers on stop", async () => {
    mockClient.getEventsToProcess.mockResolvedValue([]);
    const wakeup = createTestWakeupEmitter();
    const processor = new EventProcessor({
      client: mockClient,
      handlerMap: {},
      pollingIntervalMs: 100,
      wakeupEmitter: wakeup,
      wakeupThrottleMs: 50,
    });
    processor.start();
    await sleep(20);
    await processor.stop();

    expect(wakeup.off).toHaveBeenCalled();
  });

  it("logs when the standard polling loop's sleep step fails unexpectedly", async () => {
    const logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };
    mockClient.getEventsToProcess.mockResolvedValue([]);
    const sleepSpy = vi
      .spyOn(sleepModule, "sleep")
      .mockImplementationOnce(() => {
        throw new Error("sleep interrupted");
      });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap: {},
      pollingIntervalMs: 10,
      logger,
    });
    processor.start();

    await vi.waitFor(
      () => {
        expect(logger.error).toHaveBeenCalledWith(
          { error: expect.any(Error) },
          "polling loop error",
        );
      },
      { timeout: 2000 },
    );

    sleepSpy.mockRestore();
    await processor.stop();
  });

  it("logs when the wakeup fallback loop's sleep step fails unexpectedly", async () => {
    const logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };
    mockClient.getEventsToProcess.mockResolvedValue([]);
    const sleepSpy = vi
      .spyOn(sleepModule, "sleep")
      .mockImplementationOnce(() => {
        throw new Error("sleep interrupted");
      });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap: {},
      pollingIntervalMs: 10,
      wakeupEmitter: createTestWakeupEmitter(),
      wakeupThrottleMs: 10,
      wakeupTimeoutMs: 60_000,
      logger,
    });
    processor.start();

    await vi.waitFor(
      () => {
        expect(logger.error).toHaveBeenCalledWith(
          { error: expect.any(Error) },
          "fallback polling loop error",
        );
      },
      { timeout: 2000 },
    );

    sleepSpy.mockRestore();
    await processor.stop();
  });
});

describe("EventProcessor - basic", () => {
  it("should call into processEvents", async () => {
    const opts = {
      maxErrors: 5,
      backoff: () => now,
      pollingIntervalMs: 10,
    };
    const handlerMap = {};
    mockClient.getEventsToProcess.mockImplementation(() => Promise.resolve([]));
    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();

    await sleep(50); // Wait for polling
    await processor.stop();

    expect(mockClient.getEventsToProcess).toHaveBeenCalled();
    expect(mockClient.transaction).not.toHaveBeenCalled();
    expect(mockTxClient.getEventByIdForUpdateSkipLocked).not.toHaveBeenCalled();
    expect(mockTxClient.updateEvent).not.toHaveBeenCalled();
  });
});

describe("EventProcessor - telemetry", () => {
  it("records OpenTelemetry-compatible spans and metrics when enabled", async () => {
    const counters = new Map<string, { add: ReturnType<typeof vi.fn> }>();
    const histograms = new Map<string, { record: ReturnType<typeof vi.fn> }>();
    const spans: {
      name: string;
      setAttributes: ReturnType<typeof vi.fn>;
      recordException: ReturnType<typeof vi.fn>;
      setStatus: ReturnType<typeof vi.fn>;
      end: ReturnType<typeof vi.fn>;
    }[] = [];
    const meter = {
      createCounter: vi.fn((name: string) => {
        const counter = { add: vi.fn() };
        counters.set(name, counter);
        return counter;
      }),
      createHistogram: vi.fn((name: string) => {
        const histogram = { record: vi.fn() };
        histograms.set(name, histogram);
        return histogram;
      }),
    };
    const tracer = {
      startSpan: vi.fn((name: string) => {
        const span = {
          name,
          setAttributes: vi.fn(),
          recordException: vi.fn(),
          setStatus: vi.fn(),
          end: vi.fn(),
        };
        spans.push(span);
        return span;
      }),
    };
    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.resolve()),
      },
    };
    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {},
      errors: 0,
    };
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? [evt1] : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation(() =>
      Promise.resolve(evt1),
    );
    mockTxClient.updateEvent.mockImplementation(() => Promise.resolve());

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      pollingIntervalMs: 10,
      telemetry: {
        meter,
        tracer,
        attributes: {
          "service.name": "txob-test",
        },
      },
    });
    processor.start();
    await sleep(50);
    await processor.stop();

    expect(meter.createCounter).toHaveBeenCalledWith(
      TxOBTelemetryMetricName.EventProcessingCount,
      expect.any(Object),
    );
    expect(meter.createHistogram).toHaveBeenCalledWith(
      TxOBTelemetryMetricName.HandlerProcessingDuration,
      expect.any(Object),
    );
    expect(tracer.startSpan).toHaveBeenCalledWith(
      TxOBTelemetrySpanName.EventProcess,
      expect.objectContaining({
        attributes: expect.objectContaining({
          "service.name": "txob-test",
          [TxOBTelemetryAttributeKey.EventId]: "1",
          [TxOBTelemetryAttributeKey.EventType]: "evtType1",
          [TxOBTelemetryAttributeKey.EventCorrelationId]: "abc123",
        }),
      }),
    );
    expect(spans.map((span) => span.name)).toEqual(
      expect.arrayContaining([
        TxOBTelemetrySpanName.Poll,
        TxOBTelemetrySpanName.EventProcess,
        TxOBTelemetrySpanName.HandlerProcess,
      ]),
    );
    expect(
      counters.get(TxOBTelemetryMetricName.EventProcessingCount)?.add,
    ).toHaveBeenCalledWith(
      1,
      expect.objectContaining({
        "service.name": "txob-test",
        [TxOBTelemetryAttributeKey.EventType]: "evtType1",
        [TxOBTelemetryAttributeKey.EventOutcome]:
          TxOBTelemetryEventOutcome.Success,
      }),
    );
    expect(
      counters.get(TxOBTelemetryMetricName.HandlerProcessingCount)?.add,
    ).toHaveBeenCalledWith(
      1,
      expect.objectContaining({
        "service.name": "txob-test",
        [TxOBTelemetryAttributeKey.EventType]: "evtType1",
        [TxOBTelemetryAttributeKey.HandlerName]: "handler1",
        [TxOBTelemetryAttributeKey.HandlerOutcome]:
          TxOBTelemetryHandlerOutcome.Success,
      }),
    );
    expect(
      histograms.get(TxOBTelemetryMetricName.EventProcessingDuration)?.record,
    ).toHaveBeenCalledWith(
      expect.any(Number),
      expect.objectContaining({
        [TxOBTelemetryAttributeKey.EventType]: "evtType1",
        [TxOBTelemetryAttributeKey.EventOutcome]:
          TxOBTelemetryEventOutcome.Success,
      }),
    );
    expect(
      spans.find((span) => span.name === TxOBTelemetrySpanName.EventProcess)
        ?.setStatus,
    ).toHaveBeenCalledWith({ code: 1 });
  });

  it("surfaces failures when creating metric instruments", () => {
    const metricError = new Error("counter failed");

    expect(
      () =>
        new EventProcessor({
          client: mockClient,
          handlerMap: {},
          pollingIntervalMs: 10,
          telemetry: {
            meter: {
              createCounter: vi.fn(() => {
                throw metricError;
              }),
              createHistogram: vi.fn(),
            },
          },
        }),
    ).toThrow(metricError);
  });

  it("does not let runtime telemetry failures interrupt processing", async () => {
    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.resolve()),
      },
    };
    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {},
      errors: 0,
    };
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? [evt1] : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation(() =>
      Promise.resolve(evt1),
    );
    mockTxClient.updateEvent.mockImplementation(() => Promise.resolve());

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      pollingIntervalMs: 10,
      telemetry: {
        meter: {
          createCounter: vi.fn(() => ({
            add: vi.fn(() => {
              throw new Error("counter add failed");
            }),
          })),
          createHistogram: vi.fn(() => ({
            record: vi.fn(() => {
              throw new Error("histogram record failed");
            }),
          })),
        },
        tracer: {
          startSpan: vi.fn(() => {
            throw new Error("span failed");
          }),
        },
      },
    });
    processor.start();
    await sleep(50);
    await processor.stop();

    expect(handlerMap.evtType1.handler1).toHaveBeenCalledOnce();
    expect(mockTxClient.updateEvent).toHaveBeenCalledOnce();
  });
});
