import { randomBytes } from "node:crypto";
import { DeleteTableCommand, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  ScanCommand,
  TransactWriteCommand,
} from "@aws-sdk/lib-dynamodb";
import { describe } from "vitest";
import {
  createDynamoStore,
  migrate,
  type DynamoTransactItems,
} from "../src/dynamodb/index.js";
import { runStoreContract, type ContractDelivery } from "./contract.js";
import { createDynamoTestClient } from "./helpers.js";

/**
 * Integration tests against DynamoDB (or DynamoDB Local). Skipped unless
 * TXOB_TEST_DYNAMODB_URL is set, e.g.
 *   docker run --rm -p 18000:8000 amazon/dynamodb-local
 *   TXOB_TEST_DYNAMODB_URL=http://localhost:18000 yarn test
 */
const url = process.env.TXOB_TEST_DYNAMODB_URL;

describe.skipIf(!url)("dynamodb", () => {
  runStoreContract("dynamodb", async () => {
    const client = createDynamoTestClient();
    const doc = DynamoDBDocumentClient.from(client);
    const tableName = `txob_test_${randomBytes(4).toString("hex")}`;
    await migrate({ client, tableName });
    const store = createDynamoStore({ client, tableName });
    const scan = async (prefix: string) => {
      const items: Record<string, any>[] = [];
      let ExclusiveStartKey: Record<string, any> | undefined;
      do {
        const result = await doc.send(
          new ScanCommand({
            TableName: tableName,
            ConsistentRead: true,
            FilterExpression: "begins_with(pk, :prefix)",
            ExpressionAttributeValues: { ":prefix": prefix },
            ExclusiveStartKey,
          }),
        );
        items.push(...(result.Items ?? []));
        ExclusiveStartKey = result.LastEvaluatedKey;
      } while (ExclusiveStartKey);
      return items;
    };
    return {
      store,
      transaction: async <T>(fn: (tx: DynamoTransactItems) => Promise<T>) => {
        const tx: DynamoTransactItems = [];
        const result = await fn(tx);
        // TransactWriteItems takes at most 100 items
        for (let i = 0; i < tx.length; i += 100) {
          await doc.send(
            new TransactWriteCommand({ TransactItems: tx.slice(i, i + 100) }),
          );
        }
        return result;
      },
      getDeliveries: async () =>
        (await scan("d#")) as unknown as ContractDelivery[],
      getEvents: async () =>
        (await scan("e#"))
          .sort((a, b) => a.created_at - b.created_at)
          .map((item) => ({
            id: item.pk.slice(2),
            type: item.type,
            fanned_out_at:
              item.fanned_out_at === undefined
                ? null
                : new Date(item.fanned_out_at),
          })),
      teardown: async () => {
        await client.send(new DeleteTableCommand({ TableName: tableName }));
        client.destroy();
      },
    };
  });
});
