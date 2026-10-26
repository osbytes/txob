import type { PgAdapter } from "./store.js";

type PrismaQueryable = {
  $queryRawUnsafe<T = unknown>(query: string, ...values: any[]): Promise<T>;
};

type PrismaClientLike = PrismaQueryable & {
  $transaction(...args: any[]): Promise<any>;
};

/** What `prisma.$transaction(async (tx) => ...)` passes as `tx`, as Prisma itself defines it. */
export type PrismaTransactionOf<TClient> = Omit<
  TClient,
  "$connect" | "$disconnect" | "$on" | "$transaction" | "$use" | "$extends"
>;

export type PrismaAdapterOpts = {
  /**
   * Options for the interactive transactions txob opens: claims for limited handlers, and
   * marking a delivery dead with `onDeliveryDead`. Raise `timeout` (Prisma's default is 5s)
   * if your `onDeliveryDead` hook does slow work.
   */
  transactionOptions?: {
    maxWait?: number;
    timeout?: number;
    isolationLevel?: string;
  };
};

/**
 * Runs txob's SQL through Prisma Client on Postgres. Publish with the `tx` from an
 * interactive `prisma.$transaction(async (tx) => ...)`.
 */
export const prismaAdapter = <TClient extends PrismaClientLike>(
  prisma: TClient,
  opts: PrismaAdapterOpts = {},
): PgAdapter<PrismaTransactionOf<TClient>> => ({
  query: (text, values) => prisma.$queryRawUnsafe<any[]>(text, ...values),
  queryIn: (tx, text, values) =>
    (tx as unknown as PrismaQueryable).$queryRawUnsafe<any[]>(text, ...values),
  transaction: (fn) => prisma.$transaction(fn, opts.transactionOptions),
});
