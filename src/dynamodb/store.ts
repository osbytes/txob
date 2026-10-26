import { randomUUID } from "node:crypto";
import {
  ConditionalCheckFailedException,
  TransactionCanceledException,
  type DynamoDBClient,
} from "@aws-sdk/client-dynamodb";
import {
  BatchGetCommand,
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
  TransactWriteCommand,
  UpdateCommand,
  type BatchGetCommandOutput,
  type TransactWriteCommandInput,
} from "@aws-sdk/lib-dynamodb";
import type { TxOBHandlerSlot, TxOBNewDelivery, TxOBStore } from "../store.js";
import type { TxOBDelivery, TxOBEvent } from "../types.js";
import {
  defaultTableName,
  fanoutIndex,
  queueIndex,
  type DynamoTableOpts,
} from "./schema.js";

/**
 * The items of a `TransactWriteItems` request, in DocumentClient format. Push your own writes
 * (or ones built by ElectroDB, DynamoDB Toolbox, ...), publish, then send them in one
 * `TransactWriteCommand`.
 */
export type DynamoTransactItems = NonNullable<
  TransactWriteCommandInput["TransactItems"]
>;

export type CreateDynamoStoreOpts = DynamoTableOpts & {
  client: DynamoDBClient | DynamoDBDocumentClient;
};

type Item = Record<string, any>;

/** A running delivery counted against its handler's limits. */
type Holder = {
  /** Lease expiry (ms). */
  e: number;
  /** Concurrency key. */
  k: string | null;
};

type Limiter = {
  version: number;
  holders: Record<string, Holder>;
  /** Holders whose lease expired, to prune. */
  stale: Record<string, Holder>;
  /** Claim times (ms) within the rate limit window. */
  claims: number[];
};

const maxPriority = 2 ** 31 - 1;

/** Lease tokens of deliveries counted by a limiter carry this suffix. */
const limitedSuffix = ".limited";

/** One transaction holds at most 100 items: the limiter plus the deliveries. */
const maxClaimPerTransaction = 99;

const pad = (value: number, width: number) =>
  String(value).padStart(width, "0");

const slotOf = (eventType: string, handler: string) =>
  `${eventType.length}:${eventType}:${handler}`;

const deliveryId = (d: Pick<TxOBDelivery, "event_id" | "handler">) =>
  `${d.event_id}:${d.handler}`;

/**
 * Pending deliveries are partitioned per handler and priority, and sorted by when they're due,
 * so a claim reads only due deliveries: leased ones and ones waiting out a backoff sort after
 * `now`.
 */
const queuePk = (eventType: string, handler: string, priority: number) =>
  `${slotOf(eventType, handler)}#${priority}`;
const queueSk = (runAfter: number, id: string) => `${pad(runAfter, 15)}#${id}`;

/** Claim order across partitions: priority (highest first), then due time. */
const claimOrder = (item: Item) =>
  `${pad(maxPriority - item.priority, 10)}#${item.queue_sk}`;

/** Pages read from the front of a queue partition before resuming from the saved cursor. */
const pagesFromFront = 2;
const pagesFromCursor = 3;
const pageSize = 500;

const isLimited = (slot: TxOBHandlerSlot) =>
  slot.concurrency !== undefined ||
  slot.concurrencyPerKey !== undefined ||
  slot.rateLimit !== undefined;

const toDocumentClient = (
  client: DynamoDBClient | DynamoDBDocumentClient,
): DynamoDBDocumentClient =>
  client instanceof DynamoDBDocumentClient ||
  client.constructor.name === "DynamoDBDocumentClient"
    ? (client as DynamoDBDocumentClient)
    : DynamoDBDocumentClient.from(client as DynamoDBClient);

const toEvent = (item: Item): TxOBEvent => ({
  id: item.pk.slice(2),
  type: item.type,
  data: JSON.parse(item.data),
  correlation_id: item.correlation_id ?? null,
  created_at: new Date(item.created_at),
});

const jitter = () =>
  new Promise((resolve) => setTimeout(resolve, 5 + Math.random() * 20));

/**
 * DynamoDB store. Create the table with `migrate` or from `getTableDefinition()`.
 *
 * Publish by adding the event to the same `TransactWriteItems` request as your writes:
 *
 * ```typescript
 * const tx: DynamoTransactItems = [{ Put: { TableName: "users", Item: user } }];
 * await outbox.publish(tx, "UserCreated", { userId: user.id });
 * await documentClient.send(new TransactWriteCommand({ TransactItems: tx }));
 * ```
 *
 * Handlers with limits are coordinated through one small item per handler, updated with
 * optimistic concurrency, so limits hold across processors. Times come from the processors'
 * clocks; keep them synchronized.
 */
export const createDynamoStore = (
  opts: CreateDynamoStoreOpts,
): TxOBStore<DynamoTransactItems> => {
  const TableName = opts.tableName ?? defaultTableName;
  const doc = toDocumentClient(opts.client);

  const limiterKey = (eventType: string, handler: string) => ({
    pk: `l#${slotOf(eventType, handler)}`,
  });
  /** Lists the priorities a handler's deliveries have, i.e. its queue partitions. */
  const prioritiesKey = (eventType: string, handler: string) => ({
    pk: `p#${slotOf(eventType, handler)}`,
  });
  /**
   * Where the last claim stopped reading each queue partition. When the front of a partition
   * is all deliveries of keys at their limit, later claims continue from here, so other keys
   * are reached eventually however large one key's backlog is.
   */
  const cursors = new Map<string, Item>();
  const deliveryKey = (d: Pick<TxOBDelivery, "event_id" | "handler">) => ({
    pk: `d#${deliveryId(d)}`,
  });

  /** Runs a transaction; `false` when one of the given items' conditions failed. */
  const transact = async (
    items: DynamoTransactItems,
    fencedItems: number[] = [0],
  ): Promise<boolean> => {
    for (let attempt = 0; ; attempt++) {
      try {
        await doc.send(new TransactWriteCommand({ TransactItems: items }));
        return true;
      } catch (error) {
        if (!(error instanceof TransactionCanceledException)) throw error;
        const codes = (error.CancellationReasons ?? []).map((r) => r.Code);
        if (fencedItems.some((i) => codes[i] === "ConditionalCheckFailed")) {
          return false;
        }
        // Conflicts with a concurrent transaction on the same items are transient
        if (attempt < 10 && codes.includes("TransactionConflict")) {
          await jitter();
          continue;
        }
        throw error;
      }
    }
  };

  /**
   * Runs a transaction, retrying conflicts with concurrent transactions. Returns the
   * cancellation codes if a condition failed, `undefined` on success.
   */
  const transactCodes = async (
    items: DynamoTransactItems,
  ): Promise<(string | undefined)[] | undefined> => {
    for (let attempt = 0; ; attempt++) {
      try {
        await doc.send(new TransactWriteCommand({ TransactItems: items }));
        return undefined;
      } catch (error) {
        if (!(error instanceof TransactionCanceledException)) throw error;
        const codes = (error.CancellationReasons ?? []).map((r) => r.Code);
        if (codes.includes("ConditionalCheckFailed")) return codes;
        if (attempt < 10 && codes.includes("TransactionConflict")) {
          await jitter();
          continue;
        }
        throw error;
      }
    }
  };

  const update = async (
    input: Omit<ConstructorParameters<typeof UpdateCommand>[0], "TableName">,
  ): Promise<boolean> => {
    try {
      await doc.send(new UpdateCommand({ TableName, ...input }));
      return true;
    } catch (error) {
      if (error instanceof ConditionalCheckFailedException) return false;
      throw error;
    }
  };

  /** Removes a delivery from its handler's limiter. */
  const releaseHolder = (d: TxOBDelivery): DynamoTransactItems[number] => ({
    Update: {
      TableName,
      Key: limiterKey(d.event_type, d.handler),
      // Only claims bump the version: removing a holder can only make a concurrent claim's
      // view of the running deliveries conservative
      UpdateExpression: "REMOVE holders.#id",
      ExpressionAttributeNames: { "#id": deliveryId(d) },
    },
  });

  /** Applies a fenced update to a delivery, together with its limiter if it's counted by one. */
  const fenced = async (
    d: TxOBDelivery,
    input: {
      UpdateExpression: string;
      ExpressionAttributeNames?: Record<string, string>;
      ExpressionAttributeValues: Record<string, unknown>;
    },
    limiterUpdate?: DynamoTransactItems[number],
    extra: DynamoTransactItems = [],
  ): Promise<boolean> => {
    const deliveryUpdate = {
      Key: deliveryKey(d),
      ConditionExpression: "lease_token = :token",
      ...input,
      ExpressionAttributeValues: {
        ...input.ExpressionAttributeValues,
        ":token": d.lease_token,
      },
    };
    const items: DynamoTransactItems = [
      { Update: { TableName, ...deliveryUpdate } },
      ...(d.lease_token.endsWith(limitedSuffix) && limiterUpdate
        ? [limiterUpdate]
        : []),
      ...extra,
    ];
    if (items.length === 1) return update(deliveryUpdate);
    return transact(items);
  };

  /** Due deliveries of a handler in claim order, skipping keys in `exclude`. */
  const queryQueue = async (
    slot: TxOBHandlerSlot,
    now: number,
    want: number,
    exclude: string[],
  ): Promise<Item[]> => {
    const { Item: stored } = await doc.send(
      new GetCommand({
        TableName,
        Key: prioritiesKey(slot.event_type, slot.handler),
        ConsistentRead: true,
      }),
    );
    const priorities = [...((stored?.priorities ?? []) as Iterable<number>)]
      .map(Number)
      .sort((a, b) => b - a);

    // DynamoDB allows 100 values in an IN list; any other excluded keys are dropped later
    const excluded = exclude.slice(0, 100);
    const filter =
      excluded.length > 0
        ? {
            FilterExpression: `NOT (concurrency_key IN (${excluded.map((_, i) => `:k${i}`).join(", ")}))`,
          }
        : {};
    const excludedValues = Object.fromEntries(
      excluded.map((key, i) => [`:k${i}`, key]),
    );

    const items: Item[] = [];
    for (const priority of priorities) {
      if (items.length >= want) break;
      const partition = queuePk(slot.event_type, slot.handler, priority);
      const read = async (
        from: Item | undefined,
        pages: number,
      ): Promise<Item | undefined> => {
        let ExclusiveStartKey = from;
        for (let page = 0; page < pages && items.length < want; page++) {
          const result = await doc.send(
            new QueryCommand({
              TableName,
              IndexName: queueIndex,
              KeyConditionExpression: "queue_pk = :q AND queue_sk < :due",
              ExpressionAttributeValues: {
                ":q": partition,
                ":due": queueSk(now + 1, ""),
                ...excludedValues,
              },
              ...filter,
              Limit: pageSize,
              ExclusiveStartKey,
            }),
          );
          items.push(...(result.Items ?? []));
          ExclusiveStartKey = result.LastEvaluatedKey;
          if (!ExclusiveStartKey) return undefined;
        }
        return ExclusiveStartKey;
      };
      const rest = await read(undefined, pagesFromFront);
      if (rest && items.length < want) {
        const next = await read(
          cursors.get(partition) ?? rest,
          pagesFromCursor,
        );
        if (next) cursors.set(partition, next);
        else cursors.delete(partition);
      }
    }
    return items;
  };

  type Plan = { slot: TxOBHandlerSlot; limiter?: Limiter; picks: Item[] };

  /** Finds claimable deliveries for one handler within its limits. */
  const plan = async (slot: TxOBHandlerSlot, now: number): Promise<Plan> => {
    let budget = slot.limit;
    let limiter: Limiter | undefined;
    const running = new Map<string, number>();
    if (isLimited(slot)) {
      const { Item: stored } = await doc.send(
        new GetCommand({
          TableName,
          Key: limiterKey(slot.event_type, slot.handler),
          ConsistentRead: true,
        }),
      );
      const stale = Object.entries(
        (stored?.holders ?? {}) as Record<string, Holder>,
      ).filter(([, holder]) => holder.e <= now);
      const holders = Object.fromEntries(
        Object.entries(
          (stored?.holders ?? {}) as Record<string, Holder>,
        ).filter(([, holder]) => holder.e > now),
      );
      const claims = ((stored?.claims ?? []) as number[]).filter(
        (at) => slot.rateLimit && at > now - slot.rateLimit.durationMs,
      );
      limiter = {
        version: stored?.version ?? 0,
        holders,
        claims,
        stale: Object.fromEntries(stale),
      };
      if (slot.concurrency !== undefined) {
        budget = Math.min(
          budget,
          slot.concurrency - Object.keys(holders).length,
        );
      }
      if (slot.rateLimit) {
        budget = Math.min(budget, slot.rateLimit.max - claims.length);
      }
      for (const holder of Object.values(holders)) {
        if (holder.k !== null) {
          running.set(holder.k, (running.get(holder.k) ?? 0) + 1);
        }
      }
    }
    budget = Math.min(budget, maxClaimPerTransaction);
    if (budget <= 0) return { slot, limiter, picks: [] };

    const keyLimit = slot.concurrencyPerKey;
    const saturated =
      keyLimit === undefined
        ? []
        : [...running].filter(([, n]) => n >= keyLimit).map(([key]) => key);
    // With a per-key limit some candidates are skipped, so look further ahead
    const candidates = await queryQueue(
      slot,
      now,
      keyLimit === undefined ? budget : budget * 10,
      saturated,
    );
    const picks: Item[] = [];
    for (const item of candidates) {
      if (picks.length >= budget) break;
      const key: string | null = item.concurrency_key ?? null;
      if (keyLimit !== undefined && key !== null) {
        const count = running.get(key) ?? 0;
        if (count >= keyLimit) continue;
        running.set(key, count + 1);
      }
      picks.push(item);
    }
    return { slot, limiter, picks };
  };

  const leaseUpdate = (
    item: Item,
    token: string,
    expires: number,
  ): ConstructorParameters<typeof UpdateCommand>[0] & {
    UpdateExpression: string;
  } => ({
    TableName,
    Key: { pk: item.pk },
    // queue_sk changes with every state change, so this fails if anything moved the delivery
    ConditionExpression: "queue_sk = :seen",
    UpdateExpression:
      "SET lease_token = :token, run_after = :expires, queue_sk = :sk ADD attempts :one",
    ExpressionAttributeValues: {
      ":seen": item.queue_sk,
      ":token": token,
      ":expires": expires,
      ":sk": queueSk(expires, item.pk.slice(2)),
      ":one": 1,
    },
  });

  /** Leases a plan's picks. Returns the leased items, or `undefined` to plan again. */
  const lease = async (
    { slot, limiter, picks }: Plan,
    now: number,
    leaseMs: number,
  ): Promise<{ items: Item[]; token: string } | undefined> => {
    const expires = now + leaseMs;
    if (!limiter) {
      const token = randomUUID();
      const leased = await Promise.all(
        picks.map(async (item) =>
          (await update(leaseUpdate(item, token, expires))) ? item : undefined,
        ),
      );
      return { items: leased.filter((item) => item !== undefined), token };
    }

    // The limiter and the deliveries change in one transaction, guarded by the limiter's
    // version, so concurrent claims can't both spend the same capacity
    const token = `${randomUUID()}${limitedSuffix}`;
    const claims = slot.rateLimit
      ? [...limiter.claims, ...picks.map(() => now)]
      : [];
    const picked = new Set(picks.map((item) => item.pk.slice(2)));
    let limiterUpdate: NonNullable<DynamoTransactItems[number]["Update"]>;
    if (limiter.version === 0) {
      limiterUpdate = {
        TableName,
        Key: limiterKey(slot.event_type, slot.handler),
        ConditionExpression: "attribute_not_exists(version)",
        UpdateExpression:
          "SET holders = :holders, claims = :claims, version = :next",
        ExpressionAttributeValues: {
          ":holders": Object.fromEntries(
            picks.map((item) => [
              item.pk.slice(2),
              { e: expires, k: item.concurrency_key ?? null },
            ]),
          ),
          ":claims": claims,
          ":next": 1,
        },
      };
    } else {
      // Holders are updated by path, so renewals and completions that happen meanwhile are
      // kept. Expired holders are pruned only if nobody renewed them since they were read.
      const names: Record<string, string> = {};
      const values: Record<string, unknown> = {
        ":version": limiter.version,
        ":next": limiter.version + 1,
        ":claims": claims,
      };
      const sets = ["version = :next", "claims = :claims"];
      const conditions = ["version = :version"];
      picks.forEach((item, i) => {
        names[`#n${i}`] = item.pk.slice(2);
        values[`:n${i}`] = { e: expires, k: item.concurrency_key ?? null };
        sets.push(`holders.#n${i} = :n${i}`);
      });
      const removes: string[] = [];
      Object.entries(limiter.stale)
        .filter(([id]) => !picked.has(id))
        .slice(0, 25)
        .forEach(([id, holder], i) => {
          names[`#s${i}`] = id;
          values[`:s${i}`] = holder.e;
          removes.push(`holders.#s${i}`);
          conditions.push(`holders.#s${i}.e = :s${i}`);
        });
      limiterUpdate = {
        TableName,
        Key: limiterKey(slot.event_type, slot.handler),
        ConditionExpression: conditions.join(" AND "),
        UpdateExpression: `SET ${sets.join(", ")}${removes.length > 0 ? ` REMOVE ${removes.join(", ")}` : ""}`,
        ExpressionAttributeNames: names,
        ExpressionAttributeValues: values,
      };
    }
    try {
      await doc.send(
        new TransactWriteCommand({
          TransactItems: [
            { Update: limiterUpdate },
            ...picks.map((item) => ({
              Update: leaseUpdate(item, token, expires),
            })),
          ],
        }),
      );
      return { items: picks, token };
    } catch (error) {
      if (error instanceof TransactionCanceledException) return undefined;
      throw error;
    }
  };

  const getEvents = async (ids: string[]): Promise<Map<string, TxOBEvent>> => {
    const events = new Map<string, TxOBEvent>();
    const unique = [...new Set(ids)];
    for (let i = 0; i < unique.length; i += 100) {
      let keys: Item[] | undefined = unique
        .slice(i, i + 100)
        .map((id) => ({ pk: `e#${id}` }));
      while (keys && keys.length > 0) {
        const result: BatchGetCommandOutput = await doc.send(
          new BatchGetCommand({
            RequestItems: { [TableName]: { Keys: keys, ConsistentRead: true } },
          }),
        );
        for (const item of result.Responses?.[TableName] ?? []) {
          const event = toEvent(item);
          events.set(event.id, event);
        }
        keys = result.UnprocessedKeys?.[TableName]?.Keys;
      }
    }
    return events;
  };

  return {
    insertEvent: async (tx, event) => {
      tx.push({
        Put: {
          TableName,
          Item: {
            pk: `e#${event.id}`,
            type: event.type,
            data: JSON.stringify(event.data),
            correlation_id: event.correlation_id,
            created_at: event.created_at.getTime(),
            fanout_type: event.type,
            fanout_run_after: event.created_at.getTime(),
            fanout_attempts: 0,
          },
          ConditionExpression: "attribute_not_exists(pk)",
        },
      });
    },

    claimEventsForFanout: async ({ types, limit, leaseMs }) => {
      const now = Date.now();
      const found = (
        await Promise.all(
          types.map((type) =>
            doc.send(
              new QueryCommand({
                TableName,
                IndexName: fanoutIndex,
                KeyConditionExpression:
                  "fanout_type = :type AND fanout_run_after <= :now",
                ExpressionAttributeValues: { ":type": type, ":now": now },
                Limit: limit,
              }),
            ),
          ),
        )
      )
        .flatMap((result) => result.Items ?? [])
        .sort((a, b) => a.created_at - b.created_at)
        .slice(0, limit);

      const token = randomUUID();
      const claimed = await Promise.all(
        found.map(async (item) => {
          try {
            const { Attributes } = await doc.send(
              new UpdateCommand({
                TableName,
                Key: { pk: item.pk },
                // Fails if another processor leased or fanned out the event since the query
                ConditionExpression: "fanout_run_after = :seen",
                UpdateExpression:
                  "SET fanout_run_after = :expires, fanout_lease_token = :token ADD fanout_attempts :one",
                ExpressionAttributeValues: {
                  ":seen": item.fanout_run_after,
                  ":expires": now + leaseMs,
                  ":token": token,
                  ":one": 1,
                },
                ReturnValues: "ALL_NEW",
              }),
            );
            return Attributes;
          } catch (error) {
            if (error instanceof ConditionalCheckFailedException) return;
            throw error;
          }
        }),
      );
      return claimed
        .filter((item) => item !== undefined)
        .map((item) => ({
          ...toEvent(item),
          fanout_attempts: item.fanout_attempts,
          fanout_lease_token: token,
        }));
    },

    completeFanout: async ({ event_ids, deliveries }) => {
      // Not one transaction, but safe to repeat: existing deliveries are kept, so a crash
      // between the writes just re-runs fan-out for the event
      const now = Date.now();
      // Register each handler's priorities first, so claims look in the new partitions
      const priorities = new Map<
        string,
        { d: TxOBNewDelivery; set: Set<number> }
      >();
      for (const d of deliveries) {
        if (
          !Number.isInteger(d.priority) ||
          Math.abs(d.priority) > maxPriority
        ) {
          throw new Error(
            `priority must be a 32-bit integer, got ${d.priority}`,
          );
        }
        const slot = slotOf(d.event_type, d.handler);
        const entry = priorities.get(slot) ?? { d, set: new Set<number>() };
        entry.set.add(d.priority);
        priorities.set(slot, entry);
      }
      await Promise.all(
        [...priorities.values()].map(({ d, set }) =>
          doc.send(
            new UpdateCommand({
              TableName,
              Key: prioritiesKey(d.event_type, d.handler),
              UpdateExpression: "ADD priorities :priorities",
              ExpressionAttributeValues: { ":priorities": set },
            }),
          ),
        ),
      );
      await Promise.all(
        deliveries.map(async (d) => {
          const id = deliveryId(d);
          try {
            await doc.send(
              new PutCommand({
                TableName,
                Item: {
                  pk: `d#${id}`,
                  ...d,
                  status: "pending",
                  attempts: 0,
                  run_after: now,
                  created_at: now,
                  queue_pk: queuePk(d.event_type, d.handler, d.priority),
                  queue_sk: queueSk(now, id),
                },
                ConditionExpression: "attribute_not_exists(pk)",
              }),
            );
          } catch (error) {
            if (!(error instanceof ConditionalCheckFailedException))
              throw error;
          }
        }),
      );
      await Promise.all(
        event_ids.map((id) =>
          update({
            Key: { pk: `e#${id}` },
            ConditionExpression: "attribute_exists(pk)",
            UpdateExpression:
              "SET fanned_out_at = :now REMOVE fanout_type, fanout_run_after, fanout_lease_token",
            ExpressionAttributeValues: { ":now": now },
          }),
        ),
      );
    },

    claimDeliveries: async ({ slots, limit, leaseMs }) => {
      const now = Date.now();
      const plans = await Promise.all(slots.map((slot) => plan(slot, now)));

      // Highest priority first across handlers, `limit` overall
      const allowed = new Map<TxOBHandlerSlot, number>();
      for (const { slot, item } of plans
        .flatMap((p) => p.picks.map((item) => ({ slot: p.slot, item })))
        .sort((a, b) => (claimOrder(a.item) < claimOrder(b.item) ? -1 : 1))
        .slice(0, limit)) {
        allowed.set(slot, (allowed.get(slot) ?? 0) + 1);
      }

      const leased = await Promise.all(
        plans.map(async (first) => {
          const slotLimit = allowed.get(first.slot);
          if (!slotLimit) return [];
          let current: Plan = {
            ...first,
            picks: first.picks.slice(0, slotLimit),
          };
          let at = now;
          for (let attempt = 0; attempt < 10; attempt++) {
            const result = await lease(current, at, leaseMs);
            if (result) {
              return result.items.map((item) => ({
                item,
                token: result.token,
              }));
            }
            // Another processor changed the limiter or one of the deliveries; look again
            await jitter();
            at = Date.now();
            current = await plan({ ...first.slot, limit: slotLimit }, at);
            if (current.picks.length === 0) return [];
          }
          return [];
        }),
      );

      const flat = leased.flat();
      const events = await getEvents(flat.map(({ item }) => item.event_id));
      return flat
        .filter(({ item }) => events.has(item.event_id))
        .sort((a, b) => (claimOrder(a.item) < claimOrder(b.item) ? -1 : 1))
        .map(
          ({ item, token }): TxOBDelivery => ({
            event_id: item.event_id,
            event_type: item.event_type,
            handler: item.handler,
            attempts: item.attempts + 1,
            priority: item.priority,
            concurrency_key: item.concurrency_key ?? null,
            lease_token: token,
            event: events.get(item.event_id)!,
          }),
        );
    },

    completeDelivery: (d) =>
      fenced(
        d,
        {
          UpdateExpression:
            "SET #status = :completed, completed_at = :now REMOVE lease_token, queue_pk, queue_sk",
          ExpressionAttributeNames: { "#status": "status" },
          ExpressionAttributeValues: {
            ":completed": "completed",
            ":now": Date.now(),
          },
        },
        releaseHolder(d),
      ),

    failDelivery: async (d, failure, failOpts) => {
      const extra: DynamoTransactItems = [];
      if (failOpts?.within) {
        // Don't run the hook for a delivery this processor no longer holds. The transaction
        // below is fenced too, in case the lease is lost in between.
        const { Item: current } = await doc.send(
          new GetCommand({
            TableName,
            Key: deliveryKey(d),
            ConsistentRead: true,
          }),
        );
        if (current?.lease_token !== d.lease_token) return false;
        await failOpts.within(extra);
      }
      const error = JSON.stringify(failure.error);
      const input = failure.run_after
        ? {
            UpdateExpression:
              "SET run_after = :run_after, queue_sk = :sk, last_error = :error REMOVE lease_token",
            ExpressionAttributeValues: {
              ":run_after": failure.run_after.getTime(),
              ":sk": queueSk(failure.run_after.getTime(), deliveryId(d)),
              ":error": error,
            },
          }
        : {
            UpdateExpression:
              "SET #status = :dead, completed_at = :now, last_error = :error REMOVE lease_token, queue_pk, queue_sk",
            ExpressionAttributeNames: { "#status": "status" },
            ExpressionAttributeValues: {
              ":dead": "dead",
              ":now": Date.now(),
              ":error": error,
            },
          };
      return fenced(d, input, releaseHolder(d), extra);
    },

    extendLeases: async (deliveries, { leaseMs }) => {
      const renew = (d: TxOBDelivery, expires: number) => ({
        TableName,
        Key: deliveryKey(d),
        ConditionExpression: "lease_token = :token",
        UpdateExpression: "SET run_after = :expires, queue_sk = :sk",
        ExpressionAttributeValues: {
          ":token": d.lease_token,
          ":expires": expires,
          ":sk": queueSk(expires, deliveryId(d)),
        },
      });

      const lost: TxOBDelivery[] = [];
      const limited = new Map<string, TxOBDelivery[]>();
      const unlimited: TxOBDelivery[] = [];
      for (const d of deliveries) {
        if (d.lease_token.endsWith(limitedSuffix)) {
          const key = slotOf(d.event_type, d.handler);
          limited.set(key, [...(limited.get(key) ?? []), d]);
        } else {
          unlimited.push(d);
        }
      }

      const expires = Date.now() + leaseMs;
      await Promise.all([
        ...unlimited.map(async (d) => {
          if (!(await update(renew(d, expires)))) lost.push(d);
        }),
        // One transaction per limiter (in chunks), rather than one per delivery, so renewals
        // don't conflict with each other on the limiter item
        ...[...limited.values()].flatMap((group) => {
          const chunks: TxOBDelivery[][] = [];
          for (let i = 0; i < group.length; i += maxClaimPerTransaction) {
            chunks.push(group.slice(i, i + maxClaimPerTransaction));
          }
          return chunks.map(async (chunk) => {
            let pending = chunk;
            while (pending.length > 0) {
              const names: Record<string, string> = {};
              const values: Record<string, unknown> = {};
              const sets = pending.map((d, i) => {
                names[`#h${i}`] = deliveryId(d);
                values[`:h${i}`] = { e: expires, k: d.concurrency_key };
                return `holders.#h${i} = :h${i}`;
              });
              const codes = await transactCodes([
                ...pending.map((d) => ({ Update: renew(d, expires) })),
                {
                  Update: {
                    TableName,
                    Key: limiterKey(pending[0].event_type, pending[0].handler),
                    UpdateExpression: `SET ${sets.join(", ")}`,
                    ExpressionAttributeNames: names,
                    ExpressionAttributeValues: values,
                  },
                },
              ]);
              if (!codes) return;
              // Drop the deliveries whose lease is gone and renew the rest
              const failed = new Set(
                pending.filter((_, i) => codes[i] === "ConditionalCheckFailed"),
              );
              if (failed.size === 0) {
                throw new Error("renewing leases failed on the limiter item");
              }
              lost.push(...failed);
              pending = pending.filter((d) => !failed.has(d));
            }
          });
        }),
      ]);
      return deliveries.filter((d) => lost.includes(d));
    },

    releaseDeliveries: async (deliveries) => {
      await Promise.all(
        deliveries.map((d) => {
          const now = Date.now();
          return fenced(
            d,
            {
              UpdateExpression:
                "SET run_after = :now, queue_sk = :sk ADD attempts :minus REMOVE lease_token",
              ExpressionAttributeValues: {
                ":now": now,
                ":sk": queueSk(now, deliveryId(d)),
                ":minus": -1,
              },
            },
            releaseHolder(d),
          );
        }),
      );
    },
  };
};
