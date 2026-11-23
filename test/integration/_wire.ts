import { S3Client, S3Error } from "uns3";
import type { S3ClientConfig } from "uns3";
import type { ParquetStoreClient } from "../../src/uns3.ts";
import type { StartedStore } from "./_stores.ts";

/*
 * ---------------------------------------------------------------------------
 * The wire between a real `uns3` client and a real store, recorded and, where
 * a test asks for it, tampered with.
 *
 * Every request the client sends goes to the store for real. The recording is
 * what lets a test count what a ranged read transferred, in bytes the store
 * actually served. The faults are what lets a test meet the stores that
 * misbehave: each one answers from what the store really said, and changes
 * only the part of the answer the fault is about.
 *
 * Faults never touch a request. `uns3` signs every header, so a request edited
 * after signing would be refused by any store that checks signatures. A fault
 * that needs a different answer asks the store for it with a client of its
 * own, signed properly, and hands that answer back instead.
 * ---------------------------------------------------------------------------
 */

/** One request the client sent, as the store received it. */
export interface Exchange {
  /** How many requests the client sent before this one. */
  readonly index: number;
  readonly method: string;
  readonly bucket: string;
  readonly key: string;
  /** The `Range` header the client sent, verbatim. */
  readonly range: string | null;
  /** The `If-Match` header the client sent, verbatim. */
  readonly ifMatch: string | null;
}

/** One request, and what the client was answered. */
export interface Recorded extends Exchange {
  readonly status: number;
  /** Bytes of body the client received. */
  readonly bytes: number;
}

/**
 * Stands between the client and the store for one request. `answer` sends the
 * request to the store, and `objects` asks the store anything else.
 */
export type Fault = (
  exchange: Exchange,
  answer: () => Promise<Response>,
  objects: Objects,
) => Promise<Response>;

/** A real store, wired up for one test. */
export interface Wire {
  /** The recorded client, for the store under test. */
  readonly client: S3Client;
  /** The test's own objects, through a client nothing records or tampers with. */
  readonly objects: Objects;
  /** Every request the recorded client sent that was answered, in order. */
  readonly requests: readonly Recorded[];
  readonly requestCount: number;
  /** Bytes of body the recorded client received. */
  readonly bytesServed: number;
  /** The `Range` header of every GET, in order. */
  readonly ranges: readonly (string | null)[];
  /** Forgets the recorded requests. */
  reset(): void;
}

export interface WireOptions {
  readonly fault?: Fault;
  readonly client?: Partial<S3ClientConfig>;
}

/** Wires a recorded client to a store, with a fresh key prefix for the test. */
export function connect(store: StartedStore, options: WireOptions = {}): Wire {
  const log: Recorded[] = [];
  const plain = clientOf(store);
  const objects = new Objects(plain, store, randomPrefix());
  let sent = 0;

  const recording = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const request = new Request(input, init);
    const url = new URL(request.url);
    const [, bucket = "", ...rest] = url.pathname.split("/");
    const exchange: Exchange = {
      index: sent++,
      method: request.method,
      bucket,
      key: decodeURIComponent(rest.join("/")),
      range: request.headers.get("range"),
      ifMatch: request.headers.get("if-match"),
    };
    const answer = async (): Promise<Response> => await fetch(request);
    const response =
      options.fault === undefined ? await answer() : await options.fault(exchange, answer, objects);
    const body = await bodyOf(request.method, response);
    log.push({ ...exchange, status: response.status, bytes: body?.length ?? 0 });
    return new Response(body, {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    });
  };

  return {
    client: clientOf(store, { ...options.client, fetch: recording }),
    objects,
    requests: log,
    get requestCount() {
      return log.length;
    },
    get bytesServed() {
      let total = 0;
      for (const request of log) total += request.bytes;
      return total;
    },
    get ranges() {
      return log.filter((request) => request.method === "GET").map((request) => request.range);
    },
    reset() {
      log.length = 0;
    },
  };
}

/** The objects one test owns: every key under a prefix no other test writes under. */
export class Objects {
  readonly client: S3Client;
  readonly store: StartedStore;
  readonly prefix: string;

  constructor(client: S3Client, store: StartedStore, prefix: string) {
    this.client = client;
    this.store = store;
    this.prefix = prefix;
  }

  /** The full key of one of this test's objects. */
  at(name: string): string {
    return `${this.prefix}${name}`;
  }

  /** Writes an object, and answers its entity tag. */
  async seed(key: string, bytes: Uint8Array<ArrayBuffer>, bucket?: string): Promise<string> {
    const response = await this.client.put({ bucket, key, body: bytes, contentType: false });
    await response.body?.cancel();
    const etag = response.headers.get("etag");
    if (etag === null) throw new Error(`the store answered a put of "${key}" with no ETag`);
    return etag;
  }

  /** The bytes an object holds, or `undefined` when there is no such object. */
  async stored(key: string, bucket?: string): Promise<Uint8Array<ArrayBuffer> | undefined> {
    try {
      const response = await this.client.get({ bucket, key });
      return new Uint8Array(await response.arrayBuffer());
    } catch (error) {
      if (error instanceof S3Error && error.status === 404) return undefined;
      throw error;
    }
  }

  /** An object's current entity tag. */
  async etag(key: string): Promise<string | null> {
    const response = await this.client.head({ key });
    return response.headers.get("etag");
  }

  /** The stored `Content-Type` of an object. */
  async contentType(key: string, bucket?: string): Promise<string | null> {
    const response = await this.client.head({ bucket, key });
    return response.headers.get("content-type");
  }
}

/*
 * ---------------------------------------------------------------------------
 * Faults
 * ---------------------------------------------------------------------------
 */

/** Applies a fault from the request with this index on, and none before it. */
export function from(index: number, fault: Fault): Fault {
  return async (exchange, answer, objects) =>
    exchange.index >= index ? await fault(exchange, answer, objects) : await answer();
}

/** Runs `action` once the request with this index has been answered, and before the client sees it. */
export function after(index: number, action: (objects: Objects) => Promise<void> | void): Fault {
  return async (exchange, answer, objects) => {
    const response = await answer();
    if (exchange.index === index) await action(objects);
    return response;
  };
}

/** Applies several faults to every request, the first one outermost. */
export function chain(...faults: Fault[]): Fault {
  return async (exchange, answer, objects) => {
    let next = answer;
    for (const fault of faults.toReversed()) {
      const inner = next;
      next = async () => await fault(exchange, inner, objects);
    }
    return await next();
  };
}

/** The store ignores `Range`: a ranged GET is answered with the whole object and a 200. */
export const ignoreRange: Fault = async (exchange, answer, objects) => {
  if (exchange.method !== "GET" || exchange.range === null) return await answer();
  return await objects.client.get({
    bucket: exchange.bucket,
    key: exchange.key,
    ...(exchange.ifMatch === null ? {} : { ifMatch: exchange.ifMatch }),
    expectedStatus: [200, 412],
  });
};

/** Explicit ranges are served from this many bytes after the offset asked for. */
export function shiftRanges(by: number): Fault {
  return async (exchange, answer, objects) => {
    const range = explicitRange(exchange);
    if (range === undefined) return await answer();
    return await rangedAnswer(objects, exchange, { start: range.start + by, end: range.end + by });
  };
}

/** Explicit ranges are answered one byte short. */
export const truncateRanges: Fault = async (exchange, answer, objects) => {
  const range = explicitRange(exchange);
  if (range === undefined) return await answer();
  return await rangedAnswer(objects, exchange, {
    start: range.start,
    end: Math.max(range.start, range.end - 1),
  });
};

/** A suffix range is answered with the last byte alone. */
export const truncateTail: Fault = async (exchange, answer, objects) => {
  if (exchange.method !== "GET" || !exchange.range?.startsWith("bytes=-")) return await answer();
  return await rangedAnswer(objects, exchange, { suffix: 1 });
};

/**
 * Explicit ranges are refused as unsatisfiable: the store's own `416`, to a
 * range past the end of the object.
 */
export const unsatisfiableRanges: Fault = async (exchange, answer, objects) => {
  if (explicitRange(exchange) === undefined) return await answer();
  const past = Number.MAX_SAFE_INTEGER - 1;
  return await objects.client.get({
    bucket: exchange.bucket,
    key: exchange.key,
    range: { start: past, end: past },
    expectedStatus: [416],
  });
};

/** A partial answer does not say what it is partial of. */
export const omitContentRange: Fault = async (_exchange, answer) =>
  await partial(await answer(), (headers) => headers.delete("content-range"));

/** A partial answer states this `Content-Range` in place of the true one. */
export function contentRange(value: string): Fault {
  return async (_exchange, answer) =>
    await partial(await answer(), (headers) => headers.set("content-range", value));
}

/** A partial answer states this total in an otherwise truthful `Content-Range`. */
export function contentRangeTotal(total: number | "*"): Fault {
  return async (_exchange, answer) =>
    await partial(await answer(), (headers) => {
      const header = headers.get("content-range");
      if (header !== null) headers.set("content-range", header.replace(/\/\S+$/, `/${total}`));
    });
}

/** A HEAD is answered without a `Content-Length`. */
export const omitContentLength: Fault = async (exchange, answer) => {
  const response = await answer();
  if (exchange.method !== "HEAD") return response;
  return await rewrite(response, (headers) => headers.delete("content-length"));
};

/** Every answer comes without an `ETag`, as from a store with no entity tags to give. */
export const omitEtag: Fault = async (_exchange, answer) =>
  await rewrite(await answer(), (headers) => headers.delete("etag"));

/**
 * The slice of a client a store drives, answering `statuses` rather than
 * throwing for them on `get` and `head`.
 */
export function returning(client: S3Client, statuses: readonly number[]): ParquetStoreClient {
  return {
    get: async (params) => await client.get({ ...params, expectedStatus: [...statuses] }),
    head: async (params) => await client.head({ ...params, expectedStatus: [...statuses] }),
    put: async (params) => await client.put(params),
    del: async (params) => await client.del(params),
    list: async (params) => await client.list(params),
  };
}

/*
 * ---------------------------------------------------------------------------
 * Internals
 * ---------------------------------------------------------------------------
 */

function clientOf(store: StartedStore, overrides: Partial<S3ClientConfig> = {}): S3Client {
  return new S3Client({
    endpoint: store.endpoint,
    region: store.region,
    credentials: store.credentials,
    // A container answers on an address with no wildcard DNS in front of it.
    bucketStyle: "path",
    defaultBucket: store.bucket,
    ...overrides,
  });
}

/** A key prefix no other test writes under. */
function randomPrefix(): string {
  return `it/${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}/`;
}

/** The explicit `bytes=a-b` range a GET asked for, if that is what it asked for. */
function explicitRange(exchange: Exchange): { start: number; end: number } | undefined {
  if (exchange.method !== "GET" || exchange.range === null) return undefined;
  const match = /^bytes=(\d+)-(\d+)$/.exec(exchange.range);
  if (match === null) return undefined;
  return { start: Number(match[1]), end: Number(match[2]) };
}

/** The store's own answer to a range other than the one the client asked for. */
async function rangedAnswer(
  objects: Objects,
  exchange: Exchange,
  range: { start: number; end: number } | { suffix: number },
): Promise<Response> {
  return await objects.client.get({
    bucket: exchange.bucket,
    key: exchange.key,
    range,
    ...(exchange.ifMatch === null ? {} : { ifMatch: exchange.ifMatch }),
    expectedStatus: [206, 412],
  });
}

/** Rewrites the headers of a `206`, and leaves every other answer alone. */
async function partial(response: Response, edit: (headers: Headers) => void): Promise<Response> {
  return response.status === 206 ? await rewrite(response, edit) : response;
}

/** The same answer, with its headers edited. */
async function rewrite(response: Response, edit: (headers: Headers) => void): Promise<Response> {
  const headers = new Headers(response.headers);
  edit(headers);
  const body = response.body === null ? null : new Uint8Array(await response.arrayBuffer());
  return new Response(body, { status: response.status, statusText: response.statusText, headers });
}

/** A response body as bytes, or `null` for the answers that cannot carry one. */
async function bodyOf(method: string, response: Response): Promise<Uint8Array<ArrayBuffer> | null> {
  if (method === "HEAD" || [101, 204, 205, 304].includes(response.status)) {
    await response.body?.cancel();
    return null;
  }
  return new Uint8Array(await response.arrayBuffer());
}
