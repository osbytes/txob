import { getDate } from "./date.js";
import pLimit from "p-limit";
import { deepClone } from "./clone.js";
import { ErrorUnprocessableEventHandler, TxOBError } from "./error.js";
import {
  defaultBackoff,
  defaultMaxErrors,
  defaultMaxHandlerConcurrency,
} from "./processor-defaults.js";
import {
  endTelemetrySpan,
  recordTelemetryCounter,
  recordTelemetryDuration,
  setTelemetrySpanAttributes,
  startTelemetrySpan,
  TxOBTelemetryAttributeKey,
  TxOBTelemetryEventOutcome,
  TxOBTelemetryHandlerOutcome,
  TxOBTelemetrySpanName,
  type TxOBTelemetryAttributes,
  type TxOBTelemetryInstruments,
  type TxOBTelemetrySpan,
} from "./telemetry.js";
import type {
  Logger,
  TxOBBackoffContext,
  TxOBEvent,
  TxOBEventDataMap,
  TxOBEventHandlerMap,
  TxOBProcessorClient,
  TxOBTransactionProcessorClient,
} from "./processor.js";

type TxOBEventByType<
  TxOBEventType extends string,
  TEventDataMap extends TxOBEventDataMap<TxOBEventType>,
> = {
  [TType in TxOBEventType]: TxOBEvent<TType, TEventDataMap[TType]>;
}[TxOBEventType];

type TxOBProcessEventsOpts<
  TxOBEventType extends string,
  TEventDataMap extends TxOBEventDataMap<TxOBEventType>,
> = {
  maxErrors: number;
  backoff: (context: TxOBBackoffContext<TxOBEventType, TEventDataMap>) => Date;
  signal?: AbortSignal;
  logger?: Logger;
  maxEventConcurrency?: number;
  maxHandlerConcurrency?: number;
  maxQueuedEvents?: number;
  onEventMaxErrorsReached?: (opts: {
    event: Readonly<TxOBEventByType<TxOBEventType, TEventDataMap>>;
    txClient: TxOBTransactionProcessorClient<TxOBEventType, TEventDataMap>;
    signal?: AbortSignal;
  }) => Promise<void>;
  telemetry?: TxOBTelemetryInstruments;
};

export const processEvent = async <
  TxOBEventType extends string,
  TEventDataMap extends TxOBEventDataMap<TxOBEventType>,
>({
  client,
  handlerMap,
  unlockedEvent,
  opts,
}: {
  client: TxOBProcessorClient<TxOBEventType, TEventDataMap>;
  handlerMap: TxOBEventHandlerMap<TxOBEventType, TEventDataMap>;
  unlockedEvent: Pick<
    TxOBEventByType<TxOBEventType, TEventDataMap>,
    "id" | "errors"
  >;
  opts?: Partial<TxOBProcessEventsOpts<TxOBEventType, TEventDataMap>>;
}): Promise<{ backoffUntil?: Date }> => {
  const {
    logger,
    maxErrors = defaultMaxErrors,
    signal,
    backoff = defaultBackoff,
    maxHandlerConcurrency = defaultMaxHandlerConcurrency,
    onEventMaxErrorsReached,
    telemetry,
  } = opts ?? {};
  const eventStartedAt = Date.now();

  if (signal?.aborted) {
    return {};
  }
  if (unlockedEvent.errors >= maxErrors) {
    // Potential issue with client configuration on finding unprocessed events
    // Events with maximum allowed errors should not be returned from `getEventsToProcess`
    logger?.warn(
      {
        eventId: unlockedEvent.id,
        errors: unlockedEvent.errors,
        maxErrors,
      },
      "unexpected event with max errors returned from `getEventsToProcess`",
    );
    recordTelemetryCounter(telemetry?.eventCounter, telemetry, {
      [TxOBTelemetryAttributeKey.EventOutcome]:
        TxOBTelemetryEventOutcome.SkippedMaxErrors,
    });
    recordTelemetryDuration(
      telemetry?.eventDuration,
      telemetry,
      eventStartedAt,
      {
        [TxOBTelemetryAttributeKey.EventOutcome]:
          TxOBTelemetryEventOutcome.SkippedMaxErrors,
      },
    );
    return {};
  }

  let backoffUntil: Date | undefined;
  let eventSpan: TxOBTelemetrySpan | undefined;
  let eventError: unknown;
  let eventOutcome: TxOBTelemetryEventOutcome | undefined;
  let eventMetricAttributes: TxOBTelemetryAttributes = {};

  try {
    await client.transaction(async (txClient) => {
      const lockedEvent = await txClient.getEventByIdForUpdateSkipLocked(
        unlockedEvent.id,
        { signal, maxErrors },
      );
      if (!lockedEvent) {
        eventOutcome = TxOBTelemetryEventOutcome.SkippedLocked;
        logger?.debug(
          {
            eventId: unlockedEvent.id,
          },
          "skipping locked or already processed event",
        );
        return;
      }

      eventMetricAttributes = {
        [TxOBTelemetryAttributeKey.EventType]: lockedEvent.type,
      };
      eventSpan = startTelemetrySpan(
        telemetry,
        TxOBTelemetrySpanName.EventProcess,
        {
          [TxOBTelemetryAttributeKey.EventId]: lockedEvent.id,
          [TxOBTelemetryAttributeKey.EventType]: lockedEvent.type,
          [TxOBTelemetryAttributeKey.EventCorrelationId]:
            lockedEvent.correlation_id,
          [TxOBTelemetryAttributeKey.EventErrors]: lockedEvent.errors,
        },
      );

      // While unlikely, the following two conditions are possible if a concurrent processor finished processing this event or reaching maximum errors between the time
      // that this processor found the event with `getEventsToProcess` and called `getEventByIdForUpdateSkipLocked`
      // `getEventByIdForUpdateSkipLocked` should handle this in its query implementation and return null to save resources
      if (lockedEvent.processed_at) {
        eventOutcome = TxOBTelemetryEventOutcome.SkippedProcessed;
        logger?.debug(
          {
            eventId: lockedEvent.id,
            correlationId: lockedEvent.correlation_id,
          },
          "skipping already processed event",
        );
        return;
      }
      if (lockedEvent.errors >= maxErrors) {
        eventOutcome = TxOBTelemetryEventOutcome.SkippedMaxErrors;
        logger?.debug(
          {
            eventId: lockedEvent.id,
            correlationId: lockedEvent.correlation_id,
          },
          "skipping event with maximum errors",
        );
        return;
      }

      let errored = false;

      const eventHandlerMap = handlerMap[lockedEvent.type] ?? {};

      // Typescript should prevent the caller from passing a handler map that doesn't specify all event types but we'll check for it anyway
      // This is distinct from an empty handler map for an event type which is valid
      // We just want the caller to be explicit about the event types they are interested in handling and not accidentally skip events
      if (!(lockedEvent.type in handlerMap)) {
        logger?.warn(
          {
            eventId: lockedEvent.id,
            type: lockedEvent.type,
            correlationId: lockedEvent.correlation_id,
          },
          "missing event handler map",
        );
        errored = true;
        lockedEvent.errors = maxErrors;
      }

      logger?.debug(
        {
          eventId: lockedEvent.id,
          type: lockedEvent.type,
          correlationId: lockedEvent.correlation_id,
        },
        `processing event`,
      );

      const backoffs: Date[] = [];
      const backoffErrors: unknown[] = [];
      let latestBackoffError: unknown;

      const handlerLimit = pLimit(maxHandlerConcurrency);
      await Promise.allSettled(
        Object.entries(eventHandlerMap).map(([handlerName, handler]) =>
          handlerLimit(async (): Promise<void> => {
            const handlerMetricAttributes = {
              [TxOBTelemetryAttributeKey.EventType]: lockedEvent.type,
              [TxOBTelemetryAttributeKey.HandlerName]: handlerName,
            };
            const handlerResults =
              lockedEvent.handler_results[handlerName] ?? {};
            if (handlerResults.processed_at) {
              logger?.debug(
                {
                  eventId: lockedEvent.id,
                  type: lockedEvent.type,
                  handlerName,
                  correlationId: lockedEvent.correlation_id,
                },
                "handler already processed",
              );
              recordTelemetryCounter(telemetry?.handlerCounter, telemetry, {
                ...handlerMetricAttributes,
                [TxOBTelemetryAttributeKey.HandlerOutcome]:
                  TxOBTelemetryHandlerOutcome.SkippedProcessed,
              });
              return;
            }
            if (handlerResults.unprocessable_at) {
              logger?.debug(
                {
                  eventId: lockedEvent.id,
                  type: lockedEvent.type,
                  handlerName,
                  correlationId: lockedEvent.correlation_id,
                },
                "handler unprocessable",
              );
              recordTelemetryCounter(telemetry?.handlerCounter, telemetry, {
                ...handlerMetricAttributes,
                [TxOBTelemetryAttributeKey.HandlerOutcome]:
                  TxOBTelemetryHandlerOutcome.SkippedUnprocessable,
              });
              return;
            }

            handlerResults.errors ??= [];
            const handlerStartedAt = Date.now();
            const handlerSpan = startTelemetrySpan(
              telemetry,
              TxOBTelemetrySpanName.HandlerProcess,
              {
                [TxOBTelemetryAttributeKey.EventId]: lockedEvent.id,
                [TxOBTelemetryAttributeKey.EventType]: lockedEvent.type,
                [TxOBTelemetryAttributeKey.EventCorrelationId]:
                  lockedEvent.correlation_id,
                [TxOBTelemetryAttributeKey.HandlerName]: handlerName,
              },
            );
            let handlerOutcome: TxOBTelemetryHandlerOutcome =
              TxOBTelemetryHandlerOutcome.Success;
            let handlerError: unknown;

            try {
              await handler(lockedEvent, { signal });
              handlerResults.processed_at = getDate();
              logger?.debug(
                {
                  eventId: lockedEvent.id,
                  type: lockedEvent.type,
                  handlerName,
                  correlationId: lockedEvent.correlation_id,
                },
                "handler succeeded",
              );
            } catch (error) {
              handlerError = error;
              latestBackoffError = error;
              backoffErrors.push(error);
              logger?.error(
                {
                  eventId: lockedEvent.id,
                  type: lockedEvent.type,
                  handlerName,
                  error,
                  correlationId: lockedEvent.correlation_id,
                },
                "handler errored",
              );

              if (error instanceof ErrorUnprocessableEventHandler) {
                handlerOutcome = TxOBTelemetryHandlerOutcome.Unprocessable;
                handlerResults.unprocessable_at = getDate();
                handlerResults.errors?.push({
                  error: error.message ?? error,
                  timestamp: getDate(),
                });
                errored = true;
              } else {
                handlerOutcome = TxOBTelemetryHandlerOutcome.Error;
                if (error instanceof TxOBError && error.backoffUntil) {
                  backoffs.push(error.backoffUntil);
                }

                errored = true;
                handlerResults.errors?.push({
                  error: (error as Error)?.message ?? error,
                  timestamp: getDate(),
                });
              }
            } finally {
              const handlerAttributes = {
                ...handlerMetricAttributes,
                [TxOBTelemetryAttributeKey.HandlerOutcome]: handlerOutcome,
              };
              recordTelemetryCounter(
                telemetry?.handlerCounter,
                telemetry,
                handlerAttributes,
              );
              recordTelemetryDuration(
                telemetry?.handlerDuration,
                telemetry,
                handlerStartedAt,
                handlerAttributes,
              );
              endTelemetrySpan(handlerSpan, handlerError);
            }

            lockedEvent.handler_results[handlerName] = handlerResults;
          }),
        ),
      );

      // Check if all remaining handlers (those that haven't succeeded) are unprocessable
      // If so, there's nothing left to retry, so set errors to maxErrors to stop processing
      const remainingHandlers = Object.entries(eventHandlerMap).filter(
        ([handlerName, _]) => {
          const result = lockedEvent.handler_results[handlerName];
          return !result?.processed_at;
        },
      );

      const allRemainingHandlersUnprocessable =
        remainingHandlers.length > 0 &&
        remainingHandlers.every(([handlerName, _]) => {
          const result = lockedEvent.handler_results[handlerName];
          return result?.unprocessable_at;
        });

      if (allRemainingHandlersUnprocessable) {
        lockedEvent.errors = maxErrors;
        errored = true;
      }

      if (errored) {
        lockedEvent.errors = Math.min(lockedEvent.errors + 1, maxErrors);
        const backoffContext: TxOBBackoffContext<TxOBEventType, TEventDataMap> =
          {
            attempt: lockedEvent.errors,
            error: latestBackoffError,
            errors: backoffErrors,
            event: deepClone(lockedEvent) as Readonly<
              TxOBEventByType<TxOBEventType, TEventDataMap>
            >,
            maxErrors,
          };
        backoffs.push(backoff(backoffContext));
        const latestBackoff = backoffs.sort(
          (a, b) => b.getTime() - a.getTime(),
        )[0];
        lockedEvent.backoff_until = latestBackoff;
        if (lockedEvent.errors === maxErrors) {
          lockedEvent.backoff_until = null;
          lockedEvent.processed_at = getDate();

          if (onEventMaxErrorsReached) {
            try {
              await onEventMaxErrorsReached({
                event: deepClone(lockedEvent) as Readonly<
                  TxOBEventByType<TxOBEventType, TEventDataMap>
                >,
                txClient,
                signal,
              });
            } catch (hookError) {
              logger?.error(
                {
                  eventId: lockedEvent.id,
                  error: hookError,
                },
                "error in onEventMaxErrorsReached hook",
              );

              throw hookError;
            }
          }
        }
      } else {
        lockedEvent.backoff_until = null;
        lockedEvent.processed_at = getDate();
      }

      eventOutcome = errored
        ? lockedEvent.errors === maxErrors
          ? TxOBTelemetryEventOutcome.MaxErrors
          : TxOBTelemetryEventOutcome.Error
        : TxOBTelemetryEventOutcome.Success;
      setTelemetrySpanAttributes(eventSpan, {
        [TxOBTelemetryAttributeKey.EventOutcome]: eventOutcome,
        [TxOBTelemetryAttributeKey.EventErrors]: lockedEvent.errors,
      });

      backoffUntil = lockedEvent.backoff_until ?? undefined;

      await txClient.updateEvent(lockedEvent);
    });
  } catch (error) {
    eventError = error;
    eventOutcome ??= TxOBTelemetryEventOutcome.Error;
    throw error;
  } finally {
    if (eventOutcome) {
      const eventAttributes = {
        ...eventMetricAttributes,
        [TxOBTelemetryAttributeKey.EventOutcome]: eventOutcome,
      };
      recordTelemetryCounter(
        telemetry?.eventCounter,
        telemetry,
        eventAttributes,
      );
      recordTelemetryDuration(
        telemetry?.eventDuration,
        telemetry,
        eventStartedAt,
        eventAttributes,
      );
    }
    endTelemetrySpan(eventSpan, eventError);
  }

  return { backoffUntil };
};
