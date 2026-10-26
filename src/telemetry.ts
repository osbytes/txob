export type TxOBTelemetryAttributeValue =
  | string
  | number
  | boolean
  | string[]
  | number[]
  | boolean[];
export type TxOBTelemetryAttributes = Record<
  string,
  TxOBTelemetryAttributeValue
>;

export interface TxOBTelemetrySpan {
  setAttribute?(key: string, value: TxOBTelemetryAttributeValue): void;
  setAttributes?(attributes: TxOBTelemetryAttributes): void;
  recordException?(exception: unknown): void;
  setStatus?(status: { code: 1 | 2; message?: string }): void;
  end(): void;
}

export interface TxOBTelemetryTracer {
  startSpan(
    name: string,
    options?: { attributes?: TxOBTelemetryAttributes },
  ): TxOBTelemetrySpan;
}

export interface TxOBTelemetryCounter {
  add(value: number, attributes?: TxOBTelemetryAttributes): void;
}

export interface TxOBTelemetryHistogram {
  record(value: number, attributes?: TxOBTelemetryAttributes): void;
}

export interface TxOBTelemetryMeter {
  createCounter(
    name: string,
    options?: { description?: string; unit?: string },
  ): TxOBTelemetryCounter;
  createHistogram(
    name: string,
    options?: { description?: string; unit?: string },
  ): TxOBTelemetryHistogram;
}

export type TxOBTelemetry = {
  tracer?: TxOBTelemetryTracer;
  meter?: TxOBTelemetryMeter;
  attributes?: TxOBTelemetryAttributes;
};

export type TxOBTelemetryInstruments = {
  tracer?: TxOBTelemetryTracer;
  attributes?: TxOBTelemetryAttributes;
  fanoutCounter?: TxOBTelemetryCounter;
  deliveryCounter?: TxOBTelemetryCounter;
  deliveryDuration?: TxOBTelemetryHistogram;
  pollCounter?: TxOBTelemetryCounter;
  pollDuration?: TxOBTelemetryHistogram;
};

export const TxOBTelemetrySpanName = {
  Poll: "txob.poll",
  DeliveryProcess: "txob.delivery.process",
} as const;

export const TxOBTelemetryMetricName = {
  PollCount: "txob.poll.count",
  PollDuration: "txob.poll.duration",
  FanoutCount: "txob.fanout.count",
  DeliveryProcessingCount: "txob.delivery.processing.count",
  DeliveryProcessingDuration: "txob.delivery.processing.duration",
} as const;

export const TxOBTelemetryAttributeKey = {
  EventId: "txob.event.id",
  EventType: "txob.event.type",
  EventCorrelationId: "txob.event.correlation_id",
  HandlerName: "txob.handler.name",
  DeliveryAttempt: "txob.delivery.attempt",
  DeliveryOutcome: "txob.delivery.outcome",
  PollOutcome: "txob.poll.outcome",
  ActiveDeliveries: "txob.deliveries.active",
  EventsFannedOut: "txob.events.fanned_out",
  DeliveriesClaimed: "txob.deliveries.claimed",
} as const;

export const TxOBTelemetryDeliveryOutcome = {
  Success: "success",
  Retry: "retry",
  Dead: "dead",
  LeaseLost: "lease_lost",
  Released: "released",
} as const;

export const TxOBTelemetryPollOutcome = {
  Success: "success",
  Error: "error",
} as const;

export type TxOBTelemetryDeliveryOutcome =
  (typeof TxOBTelemetryDeliveryOutcome)[keyof typeof TxOBTelemetryDeliveryOutcome];
export type TxOBTelemetryPollOutcome =
  (typeof TxOBTelemetryPollOutcome)[keyof typeof TxOBTelemetryPollOutcome];

const telemetryStatusCode = {
  OK: 1,
  ERROR: 2,
} as const;

const suppressTelemetryRuntimeError = <T>(fn: () => T, fallback: T): T => {
  try {
    return fn();
  } catch {
    // Runtime telemetry is best-effort: once processing is underway, a broken
    // tracer/exporter must not change event processing behavior.
    return fallback;
  }
};

export const createTelemetryInstruments = (
  telemetry?: TxOBTelemetry,
): TxOBTelemetryInstruments => {
  // Metric instrument creation happens during processor construction. Surface
  // failures here so callers can catch configuration or SDK setup issues early.
  return {
    tracer: telemetry?.tracer,
    attributes: telemetry?.attributes,
    fanoutCounter: telemetry?.meter?.createCounter(
      TxOBTelemetryMetricName.FanoutCount,
      {
        description:
          "Number of outbox events fanned out to handler deliveries.",
        unit: "{event}",
      },
    ),
    deliveryCounter: telemetry?.meter?.createCounter(
      TxOBTelemetryMetricName.DeliveryProcessingCount,
      {
        description: "Number of handler delivery attempts by outcome.",
        unit: "{delivery}",
      },
    ),
    deliveryDuration: telemetry?.meter?.createHistogram(
      TxOBTelemetryMetricName.DeliveryProcessingDuration,
      {
        description: "Duration of handler delivery attempts.",
        unit: "ms",
      },
    ),
    pollCounter: telemetry?.meter?.createCounter(
      TxOBTelemetryMetricName.PollCount,
      {
        description: "Number of outbox polling attempts by outcome.",
        unit: "{poll}",
      },
    ),
    pollDuration: telemetry?.meter?.createHistogram(
      TxOBTelemetryMetricName.PollDuration,
      {
        description: "Duration of outbox polling attempts.",
        unit: "ms",
      },
    ),
  };
};

const mergeTelemetryAttributes = (
  telemetry: TxOBTelemetryInstruments | undefined,
  attributes: TxOBTelemetryAttributes = {},
): TxOBTelemetryAttributes => ({
  ...(telemetry?.attributes ?? {}),
  ...attributes,
});

export const startTelemetrySpan = (
  telemetry: TxOBTelemetryInstruments | undefined,
  name: string,
  attributes?: TxOBTelemetryAttributes,
): TxOBTelemetrySpan | undefined =>
  suppressTelemetryRuntimeError(
    () =>
      telemetry?.tracer?.startSpan(name, {
        attributes: mergeTelemetryAttributes(telemetry, attributes),
      }),
    undefined,
  );

export const setTelemetrySpanAttributes = (
  span: TxOBTelemetrySpan | undefined,
  attributes: TxOBTelemetryAttributes,
): void => {
  suppressTelemetryRuntimeError(() => {
    span?.setAttributes?.(attributes);
  }, undefined);
};

export const endTelemetrySpan = (
  span: TxOBTelemetrySpan | undefined,
  error?: unknown,
): void => {
  suppressTelemetryRuntimeError(() => {
    if (error) {
      span?.recordException?.(error);
      span?.setStatus?.({
        code: telemetryStatusCode.ERROR,
        message: error instanceof Error ? error.message : undefined,
      });
    } else {
      span?.setStatus?.({ code: telemetryStatusCode.OK });
    }
    span?.end();
  }, undefined);
};

export const recordTelemetryCounter = (
  counter: TxOBTelemetryCounter | undefined,
  telemetry: TxOBTelemetryInstruments | undefined,
  attributes?: TxOBTelemetryAttributes,
  value: number = 1,
): void => {
  suppressTelemetryRuntimeError(() => {
    counter?.add(value, mergeTelemetryAttributes(telemetry, attributes));
  }, undefined);
};

export const recordTelemetryDuration = (
  histogram: TxOBTelemetryHistogram | undefined,
  telemetry: TxOBTelemetryInstruments | undefined,
  startedAt: number,
  attributes?: TxOBTelemetryAttributes,
): void => {
  suppressTelemetryRuntimeError(() => {
    histogram?.record(
      Date.now() - startedAt,
      mergeTelemetryAttributes(telemetry, attributes),
    );
  }, undefined);
};
