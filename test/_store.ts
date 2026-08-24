import type {
  ByteRange,
  DeleteObjectParams,
  GetObjectParams,
  HeadObjectParams,
  ListObjectsV2Params,
  ListObjectsV2Response,
  PutObjectParams,
} from "uns3";
import type { ParquetStoreClient } from "../src/uns3.ts";

/*
 * ---------------------------------------------------------------------------
 * An in-memory S3, for the store to talk to.
 *
 * Objects are byte arrays in a `Map`, and the five methods the store drives are
 * implemented to the letter of what they promise: `get` honours `Range` with
 * 206 semantics and a `Content-Range`, `If-Match` fails with a 412, `head`
 * answers with sizes and etags and no body at all.
 *
 * Every request is recorded, with the bytes it served. That is the point of the
 * whole file: a ranged read is only worth writing if the bytes it does *not*
 * transfer can be counted, and this is what counts them.
 *
 * One deliberate difference from `uns3` 0.0.7, and it matters: this fake
 * honours `params.expectedStatus`, which `uns3`'s own client currently accepts
 * in its types and then ignores — its `get` checks against a hard-coded
 * `[200, 304]`, so a 206 from a real S3 is thrown as an `S3Error` before the
 * store ever sees it. The fake models `uns3`'s *documented* contract rather
 * than that bug; `store-integration.test.ts` pins the bug itself against the
 * real client, so the day it is fixed both sides say so.
 * ---------------------------------------------------------------------------
 */

/**
 * The fake, wrapped so that something happens between two of a read's requests.
 *
 * Every interesting failure of a multi-request read is a *race*: the object is
 * replaced, or the store starts ignoring ranges, after the read has already
 * committed to a footer. `hook` runs once each GET has been answered, with the
 * number of GETs that came before it, which is enough to say "and now, in the
 * middle of this read, that".
 */
export function racing(s3: FakeS3, hook: (answered: number) => void): ParquetStoreClient {
  let answered = 0;
  return wrap(s3, {
    get: async (params) => {
      const response = await s3.get(params);
      hook(answered++);
      return response;
    },
  });
}

/**
 * The fake as a plain client object, with any of its five methods replaced.
 *
 * The store is handed a client, never the class, so this is also what keeps the
 * tests honest about the surface it actually drives.
 */
export function wrap(s3: FakeS3, overrides: Partial<ParquetStoreClient> = {}): ParquetStoreClient {
  return {
    get: async (params) => await s3.get(params),
    head: async (params) => await s3.head(params),
    put: async (params) => await s3.put(params),
    del: async (params) => await s3.del(params),
    list: async (params) => await s3.list(params),
    ...overrides,
  };
}

/** An `S3Error` as far as anything outside `uns3` can tell: a status, and a name. */
export class FakeS3Error extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string) {
    super(`${status} ${code}`);
    this.name = "S3Error";
    this.status = status;
    this.code = code;
  }
}

/** One request the fake answered. */
export interface FakeRequest {
  readonly method: "GET" | "HEAD" | "PUT" | "DELETE" | "LIST";
  readonly key: string | undefined;
  readonly range: ByteRange | undefined;
  readonly ifMatch: string | undefined;
  readonly status: number;
  /** Bytes of body handed back. */
  readonly bytes: number;
}

/** The ways a store can misbehave, each one a switch a test can flip. */
export interface FakeQuirks {
  /** Answer a ranged GET with the whole object and a 200, as stores that ignore `Range` do. */
  ignoreRange?: boolean;
  /** Answer a partial GET without saying what it was partial *of*. */
  omitContentRange?: boolean;
  /** Answer a HEAD without a `Content-Length`. */
  omitContentLength?: boolean;
  /**
   * Hand back fewer bytes than an explicit range asked for.
   *
   * Suffix ranges (`bytes=-n`) are left alone: their length is legitimately
   * "whichever is smaller, `n` or the object", so a short answer to one is not
   * a store misbehaving and the read has nothing to hold it to.
   */
  truncateRanges?: boolean;
  /** Hand back a single byte for a suffix range, which nothing can hold a store to. */
  truncateTail?: boolean;
  /** Answer a partial GET with this `Content-Range` in place of the true one. */
  contentRange?: string;
  /** Answer without an `ETag`, as a store with no entity tags to give does. */
  omitEtag?: boolean;
  /** Return an unexpected status as a response instead of throwing, unlike `uns3`. */
  neverThrow?: boolean;
}

/** What `uns3`'s client accepts per method when a call does not say otherwise. */
const DEFAULT_EXPECTED: Record<FakeRequest["method"], readonly number[]> = {
  GET: [200, 304],
  HEAD: [200, 304],
  PUT: [200, 412],
  DELETE: [200, 204],
  LIST: [200],
};

interface StoredObject {
  bytes: Uint8Array;
  etag: string;
  lastModified: string;
}

export class FakeS3 implements ParquetStoreClient {
  readonly requests: FakeRequest[] = [];
  readonly quirks: FakeQuirks = {};

  readonly #objects = new Map<string, StoredObject>();
  #etags = 0;

  /** Total bytes of response body served since the last {@link reset}. */
  get bytesServed(): number {
    let total = 0;
    for (const request of this.requests) total += request.bytes;
    return total;
  }

  /** Requests answered since the last {@link reset}. */
  get requestCount(): number {
    return this.requests.length;
  }

  /** Every range a GET was asked for, in order. */
  get ranges(): (ByteRange | undefined)[] {
    return this.requests
      .filter((request) => request.method === "GET")
      .map((request) => request.range);
  }

  /** Forgets the recorded requests; the objects stay. */
  reset(): void {
    this.requests.length = 0;
  }

  /** Puts an object without going through the store, for a read-only fixture. */
  seed(key: string, bytes: Uint8Array, bucket = "b"): string {
    const etag = `"etag-${++this.#etags}"`;
    this.#objects.set(this.#id(bucket, key), {
      bytes,
      etag,
      lastModified: new Date(1_700_000_000_000).toISOString(),
    });
    return etag;
  }

  /** The bytes an object holds, as the store left them. */
  stored(key: string, bucket = "b"): Uint8Array | undefined {
    return this.#objects.get(this.#id(bucket, key))?.bytes;
  }

  /** The object's current etag. */
  etag(key: string, bucket = "b"): string | undefined {
    return this.#objects.get(this.#id(bucket, key))?.etag;
  }

  /** Replaces an object's bytes and its etag, as a concurrent writer would. */
  replace(key: string, bytes: Uint8Array, bucket = "b"): void {
    this.seed(key, bytes, bucket);
  }

  async get(params: GetObjectParams): Promise<Response> {
    const object = this.#objects.get(this.#id(params.bucket, params.key));
    if (object === undefined) return this.#answer("GET", params, 404, null, {});
    if (this.#mismatched(params.ifMatch, object.etag)) {
      return this.#answer("GET", params, 412, null, {});
    }

    const headers: Record<string, string> =
      this.quirks.omitEtag === true ? {} : { etag: object.etag };
    if (params.range === undefined || this.quirks.ignoreRange === true) {
      return this.#answer("GET", params, 200, object.bytes, headers);
    }

    const size = object.bytes.length;
    const start = params.range.start ?? Math.max(0, size - (params.range.end ?? size));
    const end =
      params.range.start === undefined
        ? size - 1
        : Math.min(params.range.end ?? size - 1, size - 1);
    if (start >= size || end < start) return this.#answer("GET", params, 416, null, headers);

    const suffix = params.range.start === undefined;
    const truncate = suffix
      ? this.quirks.truncateTail === true
      : this.quirks.truncateRanges === true;
    // A truncated suffix comes back as a single byte, a truncated explicit
    // range one byte short: both are less than what was asked for.
    const last = truncate ? (suffix ? start : Math.max(start, end - 1)) : end;
    if (this.quirks.omitContentRange !== true) {
      headers["content-range"] = this.quirks.contentRange ?? `bytes ${start}-${last}/${size}`;
    }
    return this.#answer("GET", params, 206, object.bytes.subarray(start, last + 1), headers);
  }

  async head(params: HeadObjectParams): Promise<Response> {
    const object = this.#objects.get(this.#id(params.bucket, params.key));
    if (object === undefined) return this.#answer("HEAD", params, 404, null, {});
    const headers: Record<string, string> = {
      "etag": object.etag,
      "last-modified": object.lastModified,
    };
    if (this.quirks.omitContentLength !== true) {
      headers["content-length"] = String(object.bytes.length);
    }
    // A HEAD never has a body, so nothing is served and nothing is counted.
    return this.#answer("HEAD", params, 200, null, headers);
  }

  async put(params: PutObjectParams): Promise<Response> {
    const body = params.body;
    if (!(body instanceof Uint8Array)) {
      throw new TypeError("the fake only stores Uint8Array bodies");
    }
    const id = this.#id(params.bucket, params.key);
    const existing = this.#objects.get(id);
    if (this.#mismatched(params.ifMatch, existing?.etag)) {
      return this.#answer("PUT", params, 412, null, {});
    }
    const etag = `"etag-${++this.#etags}"`;
    this.#objects.set(id, {
      bytes: body,
      etag,
      lastModified: new Date(1_700_000_000_000).toISOString(),
    });
    return this.#answer("PUT", params, 200, null, { etag });
  }

  async del(params: DeleteObjectParams): Promise<Response> {
    this.#objects.delete(this.#id(params.bucket, params.key));
    return this.#answer("DELETE", params, 204, null, {});
  }

  async list(params: ListObjectsV2Params = {}): Promise<ListObjectsV2Response> {
    const prefix = `${params.bucket ?? "b"}\u0000${params.prefix ?? ""}`;
    const keys = [...this.#objects.keys()].filter((id) => id.startsWith(prefix)).sort();

    const contents: ListObjectsV2Response["contents"] = [];
    const commonPrefixes = new Set<string>();
    for (const id of keys) {
      const key = id.slice(id.indexOf("\u0000") + 1);
      const object = this.#objects.get(id);
      if (object === undefined) continue;
      if (params.delimiter !== undefined) {
        const rest = key.slice((params.prefix ?? "").length);
        const cut = rest.indexOf(params.delimiter);
        if (cut >= 0) {
          commonPrefixes.add(
            `${params.prefix ?? ""}${rest.slice(0, cut + params.delimiter.length)}`,
          );
          continue;
        }
      }
      contents.push({
        key,
        size: object.bytes.length,
        etag: object.etag,
        lastModified: object.lastModified,
        storageClass: "STANDARD",
      });
    }

    const max = params.maxKeys ?? contents.length;
    const page = contents.slice(0, max);
    this.requests.push({
      method: "LIST",
      key: undefined,
      range: undefined,
      ifMatch: undefined,
      status: 200,
      bytes: 0,
    });
    return {
      contents: page,
      commonPrefixes: [...commonPrefixes],
      isTruncated: page.length < contents.length,
      ...(page.length < contents.length ? { nextContinuationToken: String(page.length) } : {}),
    };
  }

  #id(bucket: string | undefined, key: string | undefined): string {
    return `${bucket ?? "b"}\u0000${key ?? ""}`;
  }

  #mismatched(ifMatch: string | string[] | undefined, etag: string | undefined): boolean {
    if (ifMatch === undefined) return false;
    const wanted = Array.isArray(ifMatch) ? ifMatch : [ifMatch];
    return etag === undefined || !wanted.includes(etag);
  }

  #answer(
    method: FakeRequest["method"],
    params: {
      key?: string;
      range?: ByteRange;
      ifMatch?: string | string[];
      expectedStatus?: number | number[];
    },
    status: number,
    body: Uint8Array | null,
    headers: Record<string, string>,
  ): Response {
    this.requests.push({
      method,
      key: params.key,
      range: params.range,
      ifMatch: Array.isArray(params.ifMatch) ? params.ifMatch[0] : params.ifMatch,
      status,
      bytes: body?.length ?? 0,
    });

    const expected = params.expectedStatus ?? DEFAULT_EXPECTED[method];
    const accepted = Array.isArray(expected) ? expected : [expected];
    if (!accepted.includes(status) && this.quirks.neverThrow !== true) {
      throw new FakeS3Error(status, status === 412 ? "PreconditionFailed" : "Unexpected");
    }
    // `BodyInit` insists on a view over a plain `ArrayBuffer`, which is what
    // this is — the type simply does not say so once it has been through a
    // `subarray`.
    return new Response(body as BodyInit | null, { status, headers });
  }
}
