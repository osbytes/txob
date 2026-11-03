import type { TxOBDefer, TxOBEvent } from "./types.js";

/**
 * Validates a deferral and strips anything else, so what gets persisted is exactly
 * `{ delayMs }` or `{ until }`. Accepts an ISO string for `until`, as read back from JSON.
 */
export const normalizeDefer = (
  defer: TxOBDefer | null | undefined,
  path: string,
): TxOBDefer | null => {
  if (defer === null || defer === undefined) return null;
  if (typeof defer !== "object") {
    throw new Error(`${path}: defer must be an object, received ${defer}`);
  }
  if ("until" in defer) {
    const until =
      defer.until instanceof Date
        ? defer.until
        : new Date(defer.until as unknown as string);
    if (Number.isNaN(until.getTime())) {
      throw new Error(`${path}: defer.until must be a valid date`);
    }
    return { until };
  }
  if ("delayMs" in defer) {
    const { delayMs } = defer;
    if (
      !(typeof delayMs === "number" && Number.isFinite(delayMs)) ||
      delayMs < 0
    ) {
      throw new Error(
        `${path}: defer.delayMs must be a non-negative number, received ${delayMs}`,
      );
    }
    return { delayMs };
  }
  throw new Error(`${path}: defer must be { delayMs } or { until }`);
};

/**
 * The earliest claim time a deferral stands for, or `null` for "now". `delayMs` counts from
 * `created_at` so repeated fan-outs of the same event agree.
 */
export const resolveDefer = (
  defer: TxOBDefer | null,
  event: Pick<TxOBEvent, "created_at">,
): Date | null => {
  if (defer === null) return null;
  if ("until" in defer) return defer.until;
  return new Date(event.created_at.getTime() + defer.delayMs);
};
