import type { TransactWriteItem } from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";
import type { DynamoTransactItems } from "./store.js";

const toAttributes = (value: Record<string, unknown> | undefined) =>
  value === undefined
    ? undefined
    : marshall(value, { removeUndefinedValues: true });

/**
 * Converts DocumentClient-style transaction items (what `outbox.publish` adds) to the
 * low-level `AttributeValue` format, for libraries that send `TransactWriteItems` with the
 * plain `DynamoDBClient`, such as Dynamoose.
 */
export const marshallTransactItems = (
  items: DynamoTransactItems,
): TransactWriteItem[] =>
  items.map((item) => {
    const operation =
      item.Put ?? item.Update ?? item.Delete ?? item.ConditionCheck;
    if (!operation) throw new Error("empty transaction item");
    const converted = {
      ...operation,
      ...("Item" in operation ? { Item: toAttributes(operation.Item) } : {}),
      ...("Key" in operation ? { Key: toAttributes(operation.Key) } : {}),
      ExpressionAttributeValues: toAttributes(
        operation.ExpressionAttributeValues,
      ),
    };
    if (item.Put) return { Put: converted } as TransactWriteItem;
    if (item.Update) return { Update: converted } as TransactWriteItem;
    if (item.Delete) return { Delete: converted } as TransactWriteItem;
    return { ConditionCheck: converted } as TransactWriteItem;
  });
