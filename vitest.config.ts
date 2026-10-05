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
    include: ["packages/**/*.test.ts", "[0-9][0-9]-*/**/*.test.ts"],
    // Lab tests talk to real brokers: generous timeouts, no fixed sleeps (use `eventually`).
    testTimeout: 30_000,
    hookTimeout: 30_000,
  },
});
