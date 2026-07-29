import { describe, it, expect, vi, afterEach } from "vitest";
import {
  TxOBEvent,
  TxOBEventDataMap,
  TxOBEventHandlerMap,
  defaultBackoff,
} from "./processor.js";
import { processEvent } from "./process-event.js";

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

describe("processEvent", () => {
  it("returns before opening a transaction when the signal is already aborted", async () => {
    const ac = new AbortController();
    ac.abort();

    const result = await processEvent({
      client: mockClient,
      handlerMap: { evtType1: { h: vi.fn() } },
      unlockedEvent: { id: "1", errors: 0 },
      opts: { signal: ac.signal, maxErrors: 5 },
    });

    expect(result).toEqual({});
    expect(mockClient.transaction).not.toHaveBeenCalled();
  });

  it("skips work after lock when the row is already at max errors", async () => {
    const logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };
    const locked: TxOBEvent<"evtType1"> = {
      id: "1",
      type: "evtType1",
      timestamp: now,
      data: {},
      correlation_id: "c1",
      handler_results: {},
      errors: 5,
    };
    mockTxClient.getEventByIdForUpdateSkipLocked.mockResolvedValue(locked);

    await processEvent({
      client: mockClient,
      handlerMap: { evtType1: { h: vi.fn() } },
      unlockedEvent: { id: "1", errors: 0 },
      opts: { maxErrors: 5, logger },
    });

    expect(logger.debug).toHaveBeenCalledWith(
      expect.objectContaining({ eventId: "1", correlationId: "c1" }),
      "skipping event with maximum errors",
    );
    expect(mockTxClient.updateEvent).not.toHaveBeenCalled();
  });

  it("treats an event type absent from the handler map as a configuration error", async () => {
    const logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };
    const handlerMap: TxOBEventHandlerMap<string, TxOBEventDataMap<string>> = {
      evtType1: { h: vi.fn() },
    };
    const locked: TxOBEvent<string> = {
      id: "1",
      type: "orphanType",
      timestamp: now,
      data: {},
      correlation_id: "c1",
      handler_results: {},
      errors: 0,
    };
    mockTxClient.getEventByIdForUpdateSkipLocked.mockResolvedValue(locked);

    await processEvent({
      client: mockClient,
      handlerMap,
      unlockedEvent: { id: "1", errors: 0 },
      opts: { maxErrors: 5, backoff: defaultBackoff, logger },
    });

    expect(logger.warn).toHaveBeenCalledWith(
      expect.objectContaining({
        eventId: "1",
        type: "orphanType",
        correlationId: "c1",
      }),
      "missing event handler map",
    );
    expect(mockTxClient.updateEvent).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "orphanType",
        errors: 5,
      }),
    );
  });

  it("does not rerun a handler that is already marked unprocessable", async () => {
    const handler = vi.fn(() => Promise.resolve());
    const locked: TxOBEvent<"evtType1"> = {
      id: "1",
      type: "evtType1",
      timestamp: now,
      data: {},
      correlation_id: "c1",
      handler_results: {
        h: { unprocessable_at: now },
      },
      errors: 0,
    };
    mockTxClient.getEventByIdForUpdateSkipLocked.mockResolvedValue(locked);

    await processEvent({
      client: mockClient,
      handlerMap: { evtType1: { h: handler } },
      unlockedEvent: { id: "1", errors: 0 },
      opts: { maxErrors: 5, backoff: defaultBackoff },
    });

    expect(handler).not.toHaveBeenCalled();
    expect(mockTxClient.updateEvent).toHaveBeenCalled();
  });

  it("propagates when transaction rejects before the callback runs", async () => {
    mockClient.transaction.mockRejectedValueOnce(
      new Error("transaction start failed"),
    );

    await expect(
      processEvent({
        client: mockClient,
        handlerMap: { evtType1: { h: vi.fn() } },
        unlockedEvent: { id: "1", errors: 0 },
        opts: { maxErrors: 5, backoff: defaultBackoff },
      }),
    ).rejects.toThrow("transaction start failed");
  });
});
