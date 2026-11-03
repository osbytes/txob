import { describe, expect, it } from "vitest";
import { normalizeDefer, resolveDefer } from "./defer.js";

describe("normalizeDefer", () => {
  it("passes through null and well-formed values, dropping extra keys", () => {
    expect(normalizeDefer(null, "p")).toBeNull();
    expect(normalizeDefer(undefined, "p")).toBeNull();
    expect(normalizeDefer({ delayMs: 0 }, "p")).toEqual({ delayMs: 0 });
    expect(normalizeDefer({ delayMs: 5, extra: 1 } as any, "p")).toEqual({
      delayMs: 5,
    });
    const until = new Date("2030-01-01T00:00:00Z");
    expect(normalizeDefer({ until }, "p")).toEqual({ until });
  });

  it("accepts an ISO string for until", () => {
    const until = new Date("2030-01-01T00:00:00Z");
    expect(normalizeDefer({ until: until.toISOString() } as any, "p")).toEqual({
      until,
    });
  });

  it.each([
    [{ delayMs: -1 }, "non-negative"],
    [{ delayMs: Infinity }, "non-negative"],
    [{ delayMs: "5" }, "non-negative"],
    [{ until: "nope" }, "valid date"],
    [{ until: new Date(NaN) }, "valid date"],
    [{}, "{ delayMs } or { until }"],
    [5, "must be an object"],
  ])("rejects %j", (defer, message) => {
    expect(() => normalizeDefer(defer as any, "p")).toThrow(message);
    expect(() => normalizeDefer(defer as any, "p")).toThrow("p:");
  });
});

describe("resolveDefer", () => {
  const created_at = new Date("2030-01-01T00:00:00Z");

  it("counts delayMs from created_at and passes until through", () => {
    expect(resolveDefer(null, { created_at })).toBeNull();
    expect(resolveDefer({ delayMs: 60_000 }, { created_at })).toEqual(
      new Date("2030-01-01T00:01:00Z"),
    );
    const until = new Date("2031-01-01T00:00:00Z");
    expect(resolveDefer({ until }, { created_at })).toBe(until);
  });
});
