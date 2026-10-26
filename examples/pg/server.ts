import http from "node:http";
import { randomUUID } from "node:crypto";
import gracefulShutdown from "http-graceful-shutdown";
import { migrate } from "../../src/pg/index.js";
import { outbox, pool } from "./events.js";

const main = async (): Promise<void> => {
  await migrate(pool);
  await pool.query(`CREATE TABLE IF NOT EXISTS activity (
    id UUID PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    ip TEXT,
    ua TEXT,
    method TEXT,
    path TEXT,
    correlation_id UUID
  )`);

  const server = http.createServer(async (req, res) => {
    const correlationId = randomUUID();
    const client = await pool.connect();
    try {
      const activityId = randomUUID();
      await client.query("BEGIN");
      await client.query(
        `INSERT INTO activity (id, ip, ua, method, path, correlation_id) VALUES ($1, $2, $3, $4, $5, $6)`,
        [
          activityId,
          req.socket.remoteAddress,
          req.headers["user-agent"],
          req.method,
          req.url,
          correlationId,
        ],
      );
      // Committed atomically with the activity row, or not at all
      await outbox.publish(
        client,
        "ResourceSaved",
        {
          type: "activity",
          id: activityId,
          accountId: `acct-${Math.floor(Math.random() * 5)}`,
        },
        { correlation_id: correlationId },
      );
      await client.query("COMMIT");
      res.statusCode = 201;
    } catch (e) {
      await client.query("ROLLBACK");
      console.log(e);
      res.statusCode = 500;
    } finally {
      client.release();
    }
    res.end();
  });
  const port = process.env.PORT || 3003;
  server.listen(port, () => console.log(`listening on ${port}`));

  gracefulShutdown(server, { finally: () => void pool.end() });
};

await main().catch((err) => {
  console.error(err);
  process.exit(1);
});
