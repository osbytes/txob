import { createMemoryStore } from "../src/memory/index.js";
import { runStoreContract } from "./contract.js";

runStoreContract("memory", async () => {
  const store = createMemoryStore();
  return {
    store,
    transaction: store.transaction,
    getDeliveries: async () => store.getDeliveries(),
    getEvents: async () => store.getEvents(),
    createWakeupEmitter: async () => ({
      ...store.wakeupEmitter,
      close: async () => {},
    }),
    teardown: async () => {},
  };
});
