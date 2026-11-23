import { GenericContainer, Wait } from "testcontainers";
import type { StartedTestContainer, WaitStrategy } from "testcontainers";
import type { Credentials } from "uns3";
import { signRequest } from "uns3/signer";

/*
 * ---------------------------------------------------------------------------
 * The stores the integration suite runs against.
 *
 * Each one is a real S3 implementation in a container, described by a spec:
 * the image, how it signals that it is serving, and what it signs with. The
 * suite itself is written once and handed every started store, so a second
 * implementation is one more entry in `STORES` rather than a second suite.
 * ---------------------------------------------------------------------------
 */

/** Signing region of the containers. None cares which it is, all care that it is stable. */
export const REGION = "us-east-1";

/** The bucket the suite reads and writes. */
export const BUCKET = "tavolato";

/** A second bucket, for the calls that name a bucket of their own. */
export const OTHER_BUCKET = "tavolato-other";

/** The bucket the README's object-store example writes to. */
export const README_BUCKET = "analytics";

/** Pulling and booting a store takes minutes, not milliseconds. */
const STARTUP_TIMEOUT_MS = 180_000;

/** How long a store gets to become willing to create a bucket after it boots. */
const BUCKET_TIMEOUT_MS = 120_000;

/** How long one warm-up write may take before it is abandoned and tried again. */
const WARMUP_WRITE_TIMEOUT_MS = 10_000;

/** How many warm-up writes a store gets before the run fails. */
const WARMUP_ATTEMPTS = 12;

const POLL_MS = 500;

/** A store the suite starts in a container. */
export interface StoreSpec {
  /** The name the suite reports under. */
  readonly name: string;
  readonly image: string;
  /** Port the S3 API listens on inside the container. */
  readonly port: number;
  /** Overrides the image's own command. */
  readonly command?: readonly string[];
  /** Environment the image reads its credentials (or anything else) from. */
  readonly env?: Readonly<Record<string, string>>;
  /**
   * Built fresh on every start: a wait strategy holds the container it
   * watches, so one instance cannot be shared between two starts.
   */
  readonly wait: () => WaitStrategy;
  /** What the client signs with, and what the store accepts. */
  readonly credentials: Credentials;
}

export const STORES: readonly StoreSpec[] = [
  {
    name: "seaweedfs",
    image: "chrislusf/seaweedfs:4.54",
    port: 8333,
    command: ["server", "-s3"],
    // The S3 gateway binds its port well before it serves on it, so a port
    // wait returns too early. This log line is the readiness signal.
    wait: () => Wait.forLogMessage(/Start Seaweed S3 API Server/),
    credentials: { accessKeyId: "tavolato", secretAccessKey: "tavolato-secret" },
  },
];

/**
 * A store that is serving, and where the test process reaches it.
 *
 * Plain data on purpose: the global setup hands it to the test files, which
 * run in other workers.
 */
export interface StartedStore {
  readonly name: string;
  /** Host-side endpoint, on whatever port the runtime published. */
  readonly endpoint: string;
  readonly region: string;
  readonly bucket: string;
  readonly otherBucket: string;
  readonly credentials: Credentials;
}

/** A started store, and how to stop it. */
export interface RunningStore {
  readonly store: StartedStore;
  readonly stop: () => Promise<void>;
}

/** Starts a store's container and readies its buckets. */
export async function startStore(spec: StoreSpec): Promise<RunningStore> {
  let builder = new GenericContainer(spec.image)
    .withExposedPorts(spec.port)
    .withWaitStrategy(spec.wait())
    .withStartupTimeout(STARTUP_TIMEOUT_MS);
  if (spec.command) builder = builder.withCommand([...spec.command]);
  if (spec.env) builder = builder.withEnvironment({ ...spec.env });

  const container: StartedTestContainer = await builder.start();
  const store: StartedStore = {
    name: spec.name,
    endpoint: `http://${container.getHost()}:${String(container.getMappedPort(spec.port))}`,
    region: REGION,
    bucket: BUCKET,
    otherBucket: OTHER_BUCKET,
    credentials: spec.credentials,
  };

  try {
    for (const bucket of [BUCKET, OTHER_BUCKET, README_BUCKET]) await createBucket(store, bucket);
    await awaitFirstWrite(store);
  } catch (error) {
    await container.stop();
    throw error;
  }

  return {
    store,
    stop: async () => {
      await container.stop();
    },
  };
}

/**
 * Creates a bucket, retrying until the store agrees to.
 *
 * A store can answer on its S3 port before it serves bucket operations on it,
 * so the first attempt can be refused by a store that is healthy a moment
 * later.
 */
async function createBucket(store: StartedStore, bucket: string): Promise<void> {
  const url = new URL(`${store.endpoint}/${bucket}`);
  const giveUp = Date.now() + BUCKET_TIMEOUT_MS;
  let last = "no attempt was made";

  for (;;) {
    try {
      const response = await setupRequest(store, "PUT", url);
      // 409 is `BucketAlreadyOwnedByYou`: the goal, already met.
      if (response.ok || response.status === 409) {
        await response.body?.cancel();
        return;
      }
      last = `answered ${String(response.status)}: ${(await response.text()).slice(0, 200)}`;
    } catch (error) {
      last = describeFailure(error);
    }
    if (Date.now() >= giveUp) {
      throw new Error(`could not create bucket "${bucket}" at ${store.endpoint}; last: ${last}`);
    }
    await delay(POLL_MS);
  }
}

/**
 * Writes and deletes one object, retrying until the store takes the write.
 *
 * A fresh bucket can hold its first write for seconds while the store lays out
 * space for it. That is start-up cost, and the first test to write must not be
 * the one to pay it.
 */
async function awaitFirstWrite(store: StartedStore): Promise<void> {
  const url = new URL(`${store.endpoint}/${store.bucket}/.ready-${crypto.randomUUID()}`);
  let last = "no attempt was made";

  for (let attempt = 0; attempt < WARMUP_ATTEMPTS; attempt += 1) {
    try {
      const written = await setupRequest(
        store,
        "PUT",
        url,
        "ready",
        AbortSignal.timeout(WARMUP_WRITE_TIMEOUT_MS),
      );
      if (written.ok) {
        await written.body?.cancel();
        await (await setupRequest(store, "DELETE", url)).body?.cancel();
        return;
      }
      last = `answered ${String(written.status)}: ${(await written.text()).slice(0, 200)}`;
    } catch (error) {
      last = describeFailure(error);
    }
    await delay(POLL_MS);
  }

  throw new Error(`${store.endpoint} never accepted a first write; last: ${last}`);
}

/**
 * One signed set-up request, sent before any client exists.
 *
 * Signed per call rather than once per loop: a signature commits to the minute
 * it was made in, and the set-up loops may run for longer than that.
 */
async function setupRequest(
  store: StartedStore,
  method: "PUT" | "DELETE",
  url: URL,
  body?: string,
  signal?: AbortSignal,
): Promise<Response> {
  const { headers } = await signRequest({
    method,
    url,
    region: store.region,
    credentials: store.credentials,
    headers: new Headers(),
    body: body ?? null,
  });
  return await fetch(url, { method, headers, body, signal });
}

/** A failure in one line, with the cause `fetch` hides behind "fetch failed". */
function describeFailure(error: unknown): string {
  if (!(error instanceof Error)) return String(error);
  return error.cause instanceof Error ? `${error.message}: ${error.cause.message}` : error.message;
}

function delay(ms: number): Promise<void> {
  return new Promise((settle) => setTimeout(settle, ms));
}
