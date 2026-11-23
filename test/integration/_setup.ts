import type { TestProject } from "vitest/node";
import { type RunningStore, type StartedStore, STORES, startStore } from "./_stores.ts";

declare module "vitest" {
  interface ProvidedContext {
    /** Every store the run started, for the test files to `inject`. */
    stores: StartedStore[];
  }
}

/**
 * Starts every store once for the whole run and hands them to the test files.
 *
 * Without a container runtime this fails, loudly, rather than skipping: a run
 * that tested no store and reported success would be the worst answer here.
 */
export default async function setup(project: TestProject): Promise<() => Promise<void>> {
  const running: RunningStore[] = [];
  try {
    for (const spec of STORES) running.push(await startStore(spec));
  } catch (error) {
    await Promise.all(running.map(async ({ stop }) => await stop()));
    throw error;
  }
  project.provide(
    "stores",
    running.map(({ store }) => store),
  );
  return async () => {
    await Promise.all(running.map(async ({ stop }) => await stop()));
  };
}
