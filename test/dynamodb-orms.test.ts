import { randomBytes } from "node:crypto";
import {
  CreateTableCommand,
  DeleteTableCommand,
  DynamoDB,
  TransactWriteItemsCommand,
} from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  ScanCommand,
  TransactWriteCommand,
} from "@aws-sdk/lib-dynamodb";
import * as toolbox from "dynamodb-toolbox";
import * as dynamoose from "dynamoose";
import { Entity as ElectroEntity, Service } from "electrodb";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createOutbox } from "../src/index.js";
import {
  createDynamoStore,
  marshallTransactItems,
  migrate,
  type DynamoTransactItems,
} from "../src/dynamodb/index.js";
import {
  createDynamoTestClient as createTestClient,
  schema,
} from "./helpers.js";

/**
 * Publishing atomically with writes made through DynamoDB ORMs: each builds its transaction
 * items, txob adds the event, and everything is sent as one TransactWriteItems request.
 */
const url = process.env.TXOB_TEST_DYNAMODB_URL;

describe.skipIf(!url)("dynamodb ORMs", () => {
  const eventSchemas = { UserCreated: schema<{ userId: string }>() };
  let client: ReturnType<typeof createTestClient>;
  let doc: DynamoDBDocumentClient;
  let txobTable: string;
  let usersTable: string;
  let outbox: ReturnType<
    typeof createOutbox<typeof eventSchemas, DynamoTransactItems>
  >;

  beforeEach(async () => {
    client = createTestClient();
    doc = DynamoDBDocumentClient.from(client);
    const suffix = randomBytes(4).toString("hex");
    txobTable = `txob_orm_${suffix}`;
    usersTable = `users_${suffix}`;
    await migrate({ client, tableName: txobTable });
    await client.send(
      new CreateTableCommand({
        TableName: usersTable,
        BillingMode: "PAY_PER_REQUEST",
        AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }],
        KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }],
      }),
    );
    outbox = createOutbox({
      eventSchemas,
      store: createDynamoStore({ client, tableName: txobTable }),
    });
  });

  afterEach(async () => {
    for (const TableName of [txobTable, usersTable]) {
      await client.send(new DeleteTableCommand({ TableName }));
    }
    client.destroy();
  });

  const eventIds = async () =>
    (
      (
        await doc.send(
          new ScanCommand({ TableName: txobTable, ConsistentRead: true }),
        )
      ).Items ?? []
    )
      .filter((item) => item.pk.startsWith("e#"))
      .map((item) => item.pk.slice(2) as string)
      .sort();

  /**
   * Writing the same user twice fails the ORM's create condition on the second write, which
   * must roll back that write's event too.
   */
  const expectAtomic = async (
    write: (userId: string) => Promise<string>,
    readUser: (userId: string) => Promise<unknown>,
  ) => {
    const first = await write("u1");
    expect(await readUser("u1")).toBeTruthy();
    expect(await eventIds()).toEqual([first]);

    await expect(write("u1")).rejects.toThrow();
    expect(await eventIds()).toEqual([first]);

    const second = await write("u2");
    expect(await eventIds()).toEqual([first, second].sort());
  };

  it("ElectroDB", async () => {
    const users = new ElectroEntity(
      {
        model: { entity: "user", version: "1", service: "app" },
        attributes: { userId: { type: "string", required: true } },
        indexes: {
          primary: { pk: { field: "pk", composite: ["userId"] } },
        },
      },
      { table: usersTable, client: doc },
    );
    const app = new Service({ users });

    await expectAtomic(
      async (userId) => {
        const { TransactItems = [] } = app.transaction
          .write(({ users }) => [users.create({ userId }).commit()])
          .params();
        // ElectroDB types its items loosely; they are DocumentClient items
        const tx = [...TransactItems] as DynamoTransactItems;
        const event = await outbox.publish(tx, "UserCreated", { userId });
        await doc.send(new TransactWriteCommand({ TransactItems: tx }));
        return event.id;
      },
      async (userId) => (await users.get({ userId }).go()).data,
    );
  });

  it("DynamoDB Toolbox", async () => {
    const table = new toolbox.Table({
      name: usersTable,
      partitionKey: { name: "pk", type: "string" },
      documentClient: doc,
    });
    const users = new toolbox.Entity({
      name: "user",
      table,
      schema: toolbox.item({ pk: toolbox.string().key() }),
    });

    await expectAtomic(
      async (userId) => {
        const tx: DynamoTransactItems = [
          users
            .build(toolbox.PutTransaction)
            .item({ pk: userId })
            .options({ condition: { attr: "pk", exists: false } })
            .params(),
        ];
        const event = await outbox.publish(tx, "UserCreated", { userId });
        await doc.send(new TransactWriteCommand({ TransactItems: tx }));
        return event.id;
      },
      async (userId) =>
        (await users.build(toolbox.GetItemCommand).key({ pk: userId }).send())
          .Item,
    );
  });

  it("Dynamoose", async () => {
    dynamoose.aws.ddb.set(
      new DynamoDB({
        endpoint: url,
        region: "local",
        credentials: { accessKeyId: "local", secretAccessKey: "local" },
      }),
    );
    const User = dynamoose.model(
      `User${randomBytes(4).toString("hex")}`,
      new dynamoose.Schema({ pk: { type: String, hashKey: true } }),
      { tableName: usersTable, create: false, waitForActive: false },
    );

    await expectAtomic(
      async (userId) => {
        // Build Dynamoose's request without sending it, add the event (converted to the
        // low-level format Dynamoose uses) and send both together
        const request = await dynamoose.transaction(
          [User.transaction.create({ pk: userId })],
          { return: "request" as any },
        );
        const tx: DynamoTransactItems = [];
        const event = await outbox.publish(tx, "UserCreated", { userId });
        await client.send(
          new TransactWriteItemsCommand({
            TransactItems: [
              ...request.TransactItems,
              ...marshallTransactItems(tx),
            ],
          }),
        );
        return event.id;
      },
      async (userId) => User.get(userId),
    );
  });
});
