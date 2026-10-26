import { randomUUID } from "node:crypto";
import type { StandardSchemaV1 } from "@standard-schema/spec";
import { TxOBValidationError } from "./error.js";
import type { TxOBStore } from "./store.js";
import type {
  TxOBEvent,
  TxOBEventSchemaMap,
  TxOBEventTypeOf,
  TxOBSchemaInput,
  TxOBSchemaOutput,
} from "./types.js";

export type TxOBPublishOpts = {
  /** Defaults to a random UUID. */
  id?: string;
  correlation_id?: string | null;
};

export type TxOBPublish<TEventSchemas extends TxOBEventSchemaMap, TTx> = <
  TType extends TxOBEventTypeOf<TEventSchemas>,
>(
  tx: TTx,
  type: TType,
  data: TxOBSchemaInput<TEventSchemas[TType]>,
  opts?: TxOBPublishOpts,
) => Promise<TxOBEvent<TType, TxOBSchemaOutput<TEventSchemas[TType]>>>;

/** `publish` with the transaction already bound. */
export type TxOBBoundPublish<TEventSchemas extends TxOBEventSchemaMap> = <
  TType extends TxOBEventTypeOf<TEventSchemas>,
>(
  type: TType,
  data: TxOBSchemaInput<TEventSchemas[TType]>,
  opts?: TxOBPublishOpts,
) => Promise<TxOBEvent<TType, TxOBSchemaOutput<TEventSchemas[TType]>>>;

export type TxOBOutbox<TEventSchemas extends TxOBEventSchemaMap, TTx> = {
  publish: TxOBPublish<TEventSchemas, TTx>;
};

export const validateEventData = async <TSchema extends StandardSchemaV1>(
  eventType: string,
  schema: TSchema,
  data: unknown,
): Promise<StandardSchemaV1.InferOutput<TSchema>> => {
  let result = schema["~standard"].validate(data);
  if (result instanceof Promise) result = await result;
  if (result.issues) {
    throw new TxOBValidationError(eventType, result.issues);
  }
  return result.value;
};

/**
 * Creates a typed outbox writer. `publish` validates the event data against its schema and
 * inserts the event using the caller's transaction so it commits atomically with business data.
 *
 * @example
 * ```typescript
 * const outbox = createOutbox({ eventSchemas, store });
 * await withTransaction(async (tx) => {
 *   await createUser(tx, user);
 *   await outbox.publish(tx, "UserCreated", { userId: user.id });
 * });
 * ```
 */
export const createOutbox = <
  const TEventSchemas extends TxOBEventSchemaMap,
  TTx,
>(opts: {
  eventSchemas: TEventSchemas;
  store: Pick<TxOBStore<TTx>, "insertEvent">;
}): TxOBOutbox<TEventSchemas, TTx> => {
  const { eventSchemas, store } = opts;

  const publish: TxOBPublish<TEventSchemas, TTx> = async (
    tx,
    type,
    data,
    publishOpts,
  ) => {
    const schema = eventSchemas[type];
    if (!schema) {
      throw new Error(`unknown event type '${type}'`);
    }
    const event: TxOBEvent<
      typeof type,
      TxOBSchemaOutput<TEventSchemas[typeof type]>
    > = {
      id: publishOpts?.id ?? randomUUID(),
      type,
      data: (await validateEventData(type, schema, data)) as TxOBSchemaOutput<
        TEventSchemas[typeof type]
      >,
      correlation_id: publishOpts?.correlation_id ?? null,
      created_at: new Date(),
    };
    await store.insertEvent(tx, event);
    return event;
  };

  return { publish };
};
