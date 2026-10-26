import type { StandardSchemaV1 } from "@standard-schema/spec";

export type TxOBEventData = Record<string, unknown>;

export type TxOBStandardSchema = StandardSchemaV1<unknown, TxOBEventData>;

export type TxOBEventSchemaMap<TEventType extends string = string> = Record<
  TEventType,
  TxOBStandardSchema
>;

export type TxOBSchemaInput<TSchema> =
  TSchema extends StandardSchemaV1<infer TInput, unknown> ? TInput : never;

export type TxOBSchemaOutput<TSchema> =
  TSchema extends StandardSchemaV1<unknown, infer TOutput> ? TOutput : never;

export type TxOBEventTypeOf<TEventSchemas extends TxOBEventSchemaMap> =
  keyof TEventSchemas & string;

export type TxOBEvent<
  TEventType extends string = string,
  TData = TxOBEventData,
> = {
  id: string;
  type: TEventType;
  data: TData;
  correlation_id: string | null;
  created_at: Date;
};

export type TxOBEventFromSchemas<
  TEventSchemas extends TxOBEventSchemaMap,
  TEventType extends TxOBEventTypeOf<TEventSchemas> =
    TxOBEventTypeOf<TEventSchemas>,
> = {
  [TType in TEventType]: TxOBEvent<
    TType,
    TxOBSchemaOutput<TEventSchemas[TType]>
  >;
}[TEventType];

/**
 * A unit of work: one handler for one event. Created lazily by the processor
 * when an event is fanned out to the handlers registered for its type.
 */
export type TxOBDelivery = {
  event_id: string;
  event_type: string;
  handler: string;
  /** Number of times this delivery has been claimed, including the current claim. */
  attempts: number;
  priority: number;
  concurrency_key: string | null;
  /** Fencing token for the current lease. Writes with a stale token are ignored. */
  lease_token: string;
  event: TxOBEvent;
};

export type TxOBSerializedError = {
  message: string;
  name?: string;
  timestamp: string;
};

export interface Logger {
  debug(message?: unknown, ...optionalParams: unknown[]): void;
  info(message?: unknown, ...optionalParams: unknown[]): void;
  warn(message?: unknown, ...optionalParams: unknown[]): void;
  error(message?: unknown, ...optionalParams: unknown[]): void;
}

export interface WakeupEmitter {
  on(event: "wakeup", listener: () => void): void;
  off(event: "wakeup", listener: () => void): void;
  close(): Promise<void>;
}
