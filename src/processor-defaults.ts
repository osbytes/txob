export const defaultPollingIntervalMs = 5_000;
export const defaultMaxErrors = 5;
export const defaultMaxEventConcurrency = 20;
export const defaultMaxHandlerConcurrency = 10;
export const defaultMaxQueuedEvents = 500;
export const defaultWakeupTimeoutMs = 60_000;
export const defaultWakeupThrottleMs = 1_000;

export const defaultBackoff = ({ attempt }: { attempt: number }): Date => {
  const baseDelayMs = 1000;
  const maxDelayMs = 1000 * 60;
  const backoffMs = Math.min(baseDelayMs * 2 ** attempt, maxDelayMs);
  const retryTimestamp = new Date(Date.now() + backoffMs);

  return retryTimestamp;
};
