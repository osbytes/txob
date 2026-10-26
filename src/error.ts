import type { StandardSchemaV1 } from "@standard-schema/spec";

/**
 * TxOBError can be thrown by an event handler to indicate that the delivery should be retried.
 * It allows handlers to specify a custom backoff time via the `backoffUntil` property.
 *
 * The later of `backoffUntil` and the handler's configured backoff is used.
 *
 * @example
 * ```typescript
 * throw new TxOBError("Rate limit exceeded", {
 *   backoffUntil: new Date(Date.now() + 60000) // Retry after 1 minute
 * });
 * ```
 */
export class TxOBError extends Error {
  /**
   * Optional date indicating when the delivery should be retried.
   */
  backoffUntil?: Date;

  constructor(
    message: string,
    options: { cause?: unknown; backoffUntil?: Date } = {},
  ) {
    super(message, { cause: options.cause });
    this.name = "TxOBError";
    this.backoffUntil = options.backoffUntil;
  }
}

/**
 * ErrorUnprocessableEventHandler can be thrown by an event handler to indicate that retrying will not help.
 * The delivery is marked dead immediately and `onDeliveryDead` is called.
 */
export class ErrorUnprocessableEventHandler extends Error {
  error: Error;

  constructor(error: Error) {
    super(`unprocessable event handler: ${error.message}`, { cause: error });
    this.name = "ErrorUnprocessableEventHandler";
    this.error = error;
  }
}

/**
 * Thrown by `publish` when event data does not satisfy the event type's schema.
 */
export class TxOBValidationError extends Error {
  issues: ReadonlyArray<StandardSchemaV1.Issue>;

  constructor(
    eventType: string,
    issues: ReadonlyArray<StandardSchemaV1.Issue>,
  ) {
    super(
      `invalid data for event type '${eventType}': ${issues
        .map((issue) => issue.message)
        .join("; ")}`,
    );
    this.name = "TxOBValidationError";
    this.issues = issues;
  }
}

/**
 * Thrown (and recorded on the delivery) when a handler exceeds its `timeoutMs`.
 */
export class TxOBTimeoutError extends Error {
  constructor(timeoutMs: number) {
    super(`handler timed out after ${timeoutMs}ms`);
    this.name = "TxOBTimeoutError";
  }
}
