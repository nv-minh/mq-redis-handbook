import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    alias: {
      "@handbook/testkit": fileURLToPath(
        new URL("./packages/testkit-ts/src/index.ts", import.meta.url),
      ),
    },
  },
  test: {
    // Test files share one Redis. 02-redis-core/lab-03-eviction changes its maxmemory settings
    // for a few seconds, which would make writes of other files fail with OOM, so files run one at a time.
    fileParallelism: false,
    include: ["packages/**/*.test.ts", "[0-9][0-9]-*/**/*.test.ts"],
    // Lab tests talk to real brokers: generous timeouts, no fixed sleeps (use `eventually`).
    testTimeout: 30_000,
    hookTimeout: 30_000,
  },
});
