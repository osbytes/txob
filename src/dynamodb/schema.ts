import {
  CreateTableCommand,
  DescribeTableCommand,
  ResourceInUseException,
  ResourceNotFoundException,
  type CreateTableCommandInput,
  type DynamoDBClient,
} from "@aws-sdk/client-dynamodb";
import type { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

export type DynamoTableOpts = {
  /** Table for txob's items. Default: "txob". */
  tableName?: string;
};

export const defaultTableName = "txob";
export const fanoutIndex = "txob_fanout";
export const queueIndex = "txob_queue";

/**
 * The table txob needs, as `CreateTable` input: string partition key `pk` and two sparse
 * global secondary indexes. Use it with CDK, Terraform or CloudFormation, or call `migrate`.
 */
export const getTableDefinition = (
  opts: DynamoTableOpts = {},
): CreateTableCommandInput => ({
  TableName: opts.tableName ?? defaultTableName,
  BillingMode: "PAY_PER_REQUEST",
  AttributeDefinitions: [
    { AttributeName: "pk", AttributeType: "S" },
    { AttributeName: "fanout_type", AttributeType: "S" },
    { AttributeName: "fanout_run_after", AttributeType: "N" },
    { AttributeName: "queue_pk", AttributeType: "S" },
    { AttributeName: "queue_sk", AttributeType: "S" },
  ],
  KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }],
  GlobalSecondaryIndexes: [
    {
      // Events not yet fanned out, per type
      IndexName: fanoutIndex,
      KeySchema: [
        { AttributeName: "fanout_type", KeyType: "HASH" },
        { AttributeName: "fanout_run_after", KeyType: "RANGE" },
      ],
      Projection: { ProjectionType: "ALL" },
    },
    {
      // Pending deliveries, per handler, in claim order
      IndexName: queueIndex,
      KeySchema: [
        { AttributeName: "queue_pk", KeyType: "HASH" },
        { AttributeName: "queue_sk", KeyType: "RANGE" },
      ],
      Projection: { ProjectionType: "ALL" },
    },
  ],
});

/**
 * Creates the table if it doesn't exist and waits until it's active. Safe to run from many
 * processes at once.
 */
export const migrate = async (
  opts: DynamoTableOpts & {
    client: DynamoDBClient | DynamoDBDocumentClient;
  },
): Promise<void> => {
  const definition = getTableDefinition(opts);
  const client = opts.client as DynamoDBClient;
  try {
    await client.send(new CreateTableCommand(definition));
  } catch (error) {
    if (!(error instanceof ResourceInUseException)) throw error;
  }
  for (let attempt = 0; attempt < 120; attempt++) {
    try {
      const { Table } = await client.send(
        new DescribeTableCommand({ TableName: definition.TableName }),
      );
      if (
        Table?.TableStatus === "ACTIVE" &&
        (Table.GlobalSecondaryIndexes ?? []).every(
          (index) => index.IndexStatus === "ACTIVE",
        )
      ) {
        return;
      }
    } catch (error) {
      if (!(error instanceof ResourceNotFoundException)) throw error;
    }
    await new Promise((resolve) => setTimeout(resolve, 1_000));
  }
  throw new Error(`table ${definition.TableName} did not become active`);
};
