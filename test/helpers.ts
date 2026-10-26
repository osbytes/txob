import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import type { StandardSchemaV1 } from "@standard-schema/spec";

/** Minimal Standard Schema for tests; `check` returns an error message or undefined. */
export const schema = <T extends Record<string, unknown>>(
  check: (value: Record<string, unknown>) => string | undefined = () =>
    undefined,
): StandardSchemaV1<T, T> => ({
  "~standard": {
    version: 1,
    vendor: "test",
    validate: (value) => {
      if (typeof value !== "object" || value === null) {
        return { issues: [{ message: "expected object" }] };
      }
      const message = check(value as Record<string, unknown>);
      return message ? { issues: [{ message }] } : { value: value as T };
    },
  },
});

export const waitFor = async (
  predicate: () => boolean | Promise<boolean>,
  { timeoutMs = 5_000, intervalMs = 10 } = {},
): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await predicate()) return;
    if (Date.now() > deadline) throw new Error("waitFor timed out");
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
};

export const delay = (ms: number) =>
  new Promise((resolve) => setTimeout(resolve, ms));

export const silentLogger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
};

/** A client for DynamoDB Local at TXOB_TEST_DYNAMODB_URL. */
export const createDynamoTestClient = () =>
  new DynamoDBClient({
    endpoint: process.env.TXOB_TEST_DYNAMODB_URL,
    region: "local",
    credentials: { accessKeyId: "local", secretAccessKey: "local" },
  });
