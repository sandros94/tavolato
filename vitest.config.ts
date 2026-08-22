import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    slowTestThreshold: 2000,
    coverage: {
      exclude: ["dist/**", "docs/**", "test/**", "build.config.ts", "vitest.config.ts"],
    },
  },
});
