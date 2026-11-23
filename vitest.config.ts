import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    slowTestThreshold: 2000,
    coverage: {
      exclude: ["dist/**", "docs/**", "test/**", "build.config.ts", "vitest.config.ts"],
    },
    projects: [
      {
        extends: true,
        test: {
          name: "unit",
          include: ["test/**/*.test.ts"],
          exclude: ["test/integration/**"],
        },
      },
      {
        extends: true,
        test: {
          name: "integration",
          include: ["test/integration/**/*.test.ts"],
          // One container for the whole run, started before any file and
          // stopped after the last.
          globalSetup: ["test/integration/_setup.ts"],
          // Pulling and booting a store takes minutes, not milliseconds.
          hookTimeout: 300_000,
          testTimeout: 60_000,
        },
      },
    ],
  },
});
