import { defineConfig } from "prisma/config";

export default defineConfig({
  schema: "schema.prisma",
  datasource: { url: process.env.TXOB_TEST_PRISMA_DIFF_URL ?? "" },
});
