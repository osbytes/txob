import { createHash } from "node:crypto";

/**
 * Minimal surface txob needs from a Redis client. ioredis (and its forks such as iovalkey) and
 * node-redis (`redis`, `@redis/client`) are detected automatically.
 */
export type RedisClientLike = {
  multi(...args: any[]): object;
  duplicate(...args: any[]): unknown;
};

/** A dedicated connection, for WATCH and SUBSCRIBE which can't share a multiplexed one. */
export type RedisConnection = {
  command(args: string[]): Promise<unknown>;
  watch(key: string): Promise<void>;
  multi(): object;
  /** Executes a MULTI; `null` if a watched key changed. */
  exec(multi: object): Promise<unknown[] | null>;
  subscribe(channel: string, onMessage: () => void): Promise<void>;
  onError(listener: (error: Error) => void): void;
  close(): Promise<void>;
};

export type RedisDriver = {
  command(args: string[]): Promise<unknown>;
  /** Appends a command to the caller's MULTI. */
  queue(multi: object, args: string[]): void;
  connect(): Promise<RedisConnection>;
};

const isIORedis = (client: any): boolean => typeof client.call === "function";
const isNodeRedis = (client: any): boolean =>
  typeof client.sendCommand === "function" &&
  typeof client.connect === "function";

const ioredisConnection = async (client: any): Promise<RedisConnection> => {
  const conn = client.duplicate();
  return {
    command: (args) => conn.call(...args),
    watch: async (key) => void (await conn.watch(key)),
    multi: () => conn.multi(),
    exec: async (multi: any) => {
      const results: [Error | null, unknown][] | null = await multi.exec();
      if (results === null) return null;
      const failed = results.find(([error]) => error);
      if (failed) throw failed[0];
      return results.map(([, result]) => result);
    },
    subscribe: async (channel, onMessage) => {
      conn.on("message", (received: string) => {
        if (received === channel) onMessage();
      });
      await conn.subscribe(channel);
    },
    onError: (listener) => conn.on("error", listener),
    close: async () => {
      await conn.quit().catch(() => conn.disconnect());
    },
  };
};

const nodeRedisConnection = async (client: any): Promise<RedisConnection> => {
  const conn = client.duplicate();
  const errorListeners: ((error: Error) => void)[] = [];
  // node-redis throws on an unhandled `error` event; only report errors if someone listens
  conn.on("error", (error: Error) => {
    for (const listener of errorListeners) listener(error);
  });
  await conn.connect();
  return {
    command: (args) => conn.sendCommand(args),
    watch: async (key) => void (await conn.watch(key)),
    multi: () => conn.multi(),
    exec: async (multi: any) => {
      try {
        return await multi.exec();
      } catch (error) {
        if (error instanceof Error && error.name === "WatchError") return null;
        throw error;
      }
    },
    subscribe: async (channel, onMessage) => {
      await conn.subscribe(channel, () => onMessage());
    },
    onError: (listener) => void errorListeners.push(listener),
    close: async () => {
      await (typeof conn.close === "function" ? conn.close() : conn.quit());
    },
  };
};

export const createRedisDriver = (client: RedisClientLike): RedisDriver => {
  if (isIORedis(client)) {
    const io = client as any;
    return {
      command: (args) => io.call(...args),
      queue: (multi: any, args) => void multi.call(...args),
      connect: () => ioredisConnection(io),
    };
  }
  if (isNodeRedis(client)) {
    const nr = client as any;
    return {
      command: (args) => nr.sendCommand(args),
      queue: (multi: any, args) => void multi.addCommand(args),
      connect: () => nodeRedisConnection(nr),
    };
  }
  throw new Error(
    "unsupported Redis client: pass an ioredis/iovalkey client or a connected node-redis client",
  );
};

/**
 * Runs a script by SHA, loading it on first use (or after a SCRIPT FLUSH / failover).
 * `routingKey` is declared as the script's key so cluster clients route it to the slot that
 * holds txob's keys.
 */
export const createScriptRunner = (driver: RedisDriver, routingKey: string) => {
  const shas = new Map<string, string>();
  return async (script: string, args: (string | number)[]): Promise<any> => {
    let sha = shas.get(script);
    if (!sha) {
      sha = createHash("sha1").update(script).digest("hex");
      shas.set(script, sha);
    }
    const argv = args.map(String);
    try {
      return await driver.command(["EVALSHA", sha, "1", routingKey, ...argv]);
    } catch (error) {
      if (!(error instanceof Error) || !error.message.startsWith("NOSCRIPT")) {
        throw error;
      }
      return driver.command(["EVAL", script, "1", routingKey, ...argv]);
    }
  };
};
