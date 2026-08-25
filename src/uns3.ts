import type {
  DeleteObjectParams,
  GetObjectParams,
  HeadObjectParams,
  ListObjectsV2Params,
  ListObjectsV2Response,
  PutObjectParams,
} from "uns3";
import {
  assertOptionalOptionsObject,
  assertOptionsObject,
  badOption,
  describe,
  malformed,
  TavolatoError,
} from "./error.ts";
import { ByteReader } from "./internal/bytes.ts";
import { type ColumnChunkInfo, MAGIC, type RowGroupInfo } from "./internal/format.ts";
import {
  assertChunkCount,
  type FooterInfo,
  readFooter,
  readParquet,
  readRowGroup,
} from "./reader.ts";
import type {
  ParquetFile,
  ParquetSchema,
  ReadOptions,
  ReadRow,
  Row,
  SchemaDefinition,
  WriterOptions,
} from "./types.ts";
import { createWriter } from "./writer.ts";

/**
 * IANA media type for Apache Parquet.
 *
 * @see https://www.iana.org/assignments/media-types/application/vnd.apache.parquet
 */
export const PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet";

/**
 * Anything that can hand over a finished Parquet file. `finish` may defer,
 * which is what a `ParquetWriter` with an asynchronous codec does.
 */
export interface ParquetSource {
  finish(): Uint8Array | Promise<Uint8Array>;
}

/**
 * `uns3` put parameters, minus the body (which comes from the writer) and with
 * a Parquet-aware default for `contentType`.
 */
export type PutParquetParams = Omit<PutObjectParams, "body">;

/**
 * Uploads a Parquet file to S3 with `uns3`.
 *
 * Accepts either raw bytes or anything with a `finish()` method — a
 * `ParquetWriter` can be handed over directly, in which case it is finished
 * here and becomes unusable afterwards.
 *
 * `contentType` defaults to {@link PARQUET_CONTENT_TYPE}; pass your own value
 * (or `false` to let `uns3` resolve it from the key) to override.
 *
 * `uns3` is an optional peer dependency and is only used for its types: this
 * helper never imports it at runtime, it just calls `client.put`.
 *
 * The one call of {@link createParquetStore}'s `put` that predates it, kept as
 * it was. A store does the same thing with the key in front and the bucket
 * configured once — reach for that when you are doing more than one upload.
 *
 * @example
 * import { S3Client } from "uns3";
 * import { createWriter, defineSchema } from "tavolato";
 * import { putParquet } from "tavolato/uns3";
 *
 * const writer = createWriter(defineSchema({ n: { type: "i64" } }));
 * writer.append({ n: 1n });
 *
 * const client = new S3Client({ ... });
 * await putParquet(client, { bucket: "metrics", key: "events/2026-08-22.parquet" }, writer);
 */
export async function putParquet(
  client: Pick<ParquetStoreClient, "put">,
  params: PutParquetParams,
  source: Uint8Array | ParquetSource,
): Promise<Response> {
  assertOptionsObject(params, "putParquet params", "ERR_STORE_INPUT_INVALID");
  const body = source instanceof Uint8Array ? source : await source.finish();
  return await client.put({
    contentType: PARQUET_CONTENT_TYPE,
    ...params,
    body,
  });
}

/*
 * ---------------------------------------------------------------------------
 * The store
 *
 * A Parquet file is laid out so that it can be read in pieces: the footer at
 * the end says where every column chunk of every row group begins, and each of
 * those chunks decodes on its own. Locally that buys a projection the right to
 * seek past the columns nobody asked for. Over HTTP it buys something worth a
 * great deal more — the bytes of an unwanted chunk are never *transferred*, and
 * a read of two columns out of forty costs two columns of bandwidth.
 *
 * So the store is a client-injection wrapper in the shape of `uns3`'s own
 * client (put / get / head / del / list), and its `get` is where the format
 * pays for itself. Nothing here imports `uns3` at runtime; the types are the
 * whole of the dependency.
 * ---------------------------------------------------------------------------
 */

/**
 * The slice of `uns3`'s `S3Client` a store drives.
 *
 * Declared structurally rather than as `S3Client` itself so that the store can
 * be handed anything that speaks those five methods — a real client, a client
 * wrapped in your own retry policy, a fake in a test — and so that `uns3`
 * remains a type-only dependency.
 */
export interface ParquetStoreClient {
  get(params: GetObjectParams): Promise<Response>;
  head(params: HeadObjectParams): Promise<Response>;
  put(params: PutObjectParams): Promise<Response>;
  del(params: DeleteObjectParams): Promise<Response>;
  list(params?: ListObjectsV2Params): Promise<ListObjectsV2Response>;
}

/**
 * Rows and the schema they belong to: what `put` builds a file out of when it
 * is not handed one.
 */
export interface ParquetRows<TDefinition extends SchemaDefinition = SchemaDefinition> {
  /** A schema from `defineSchema`. */
  readonly schema: ParquetSchema<TDefinition>;
  /** The rows to write, in file order. */
  readonly rows: Iterable<Row<TDefinition>>;
}

/** What `put` accepts: a finished file, something that will finish one, or rows. */
export type PutInput<TDefinition extends SchemaDefinition = SchemaDefinition> =
  | Uint8Array
  | ParquetSource
  | ParquetRows<TDefinition>;

/**
 * Factory-level defaults for a store: configure once, override per call.
 *
 * Everything here is a default and nothing is a lock — the same name passed to
 * a single call wins, and `codecs` / `types` are replaced wholesale rather than
 * merged, since a partial merge of a codec table is a subtler thing than it
 * looks.
 */
export interface StoreDefaults {
  /** Bucket for every call, as `uns3`'s own `defaultBucket` is for a client. */
  bucket?: string;
  /** Default `ReadOptions.codecs` for `get`. */
  codecs?: ReadOptions["codecs"];
  /** Default `ReadOptions.types` for `get` and `head`. */
  types?: ReadOptions["types"];
  /** Default writer options for a `put` handed {@link ParquetRows}. */
  writer?: WriterOptions;
  /**
   * Bytes to read from the end of an object when a read needs its footer.
   * Defaults to 64 KiB, which holds the footer of essentially every file a
   * flat schema produces; a footer larger than this costs one extra request,
   * never a wrong answer.
   *
   * Lower it when your files are small and every byte on the wire counts, raise
   * it when your schemas are wide enough that 64 KiB of metadata is a real
   * possibility.
   */
  tailBytes?: number;
}

/**
 * Per-call parameters for `store.put`: `uns3`'s put parameters minus the two
 * the store supplies itself, plus the writer options a `{ schema, rows }`
 * upload is built with.
 */
export interface PutParams extends Omit<PutObjectParams, "body" | "key"> {
  /** Writer options for a `put` handed {@link ParquetRows}; ignored for bytes. */
  writer?: WriterOptions;
}

/**
 * Options for `store.get`: how to read the file, and how to ask for it.
 *
 * The read half is {@link ReadOptions} — `columns` narrows the columns exactly
 * as it does locally — plus `groups`, which narrows the *rows* to whole row
 * groups. The request half is `uns3`'s get parameters, minus the two the store
 * owns: `key` is the first argument, and `range` is what the store computes.
 */
export interface GetOptions extends Omit<GetObjectParams, "key" | "range">, ReadOptions {
  /**
   * Row group indices to read, in the file's numbering. Every other group is
   * skipped: its bytes are neither fetched nor decoded.
   *
   * The rows come back in the **file's** order whatever order the indices were
   * given in, for the same reason `columns` does — a selection is a set, and a
   * deterministic order is worth more than honouring an accident of argument
   * order. An index the file does not have, a repeated one, or an empty list is
   * `ERR_READ_OPTION_INVALID`.
   *
   * `head` is how you learn how many groups there are.
   */
  groups?: readonly number[];
}

/** Options for `store.head`: `uns3`'s head parameters, plus the column types. */
export interface HeadOptions extends Omit<HeadObjectParams, "key" | "range"> {
  /**
   * Logical column types, as `ReadOptions.types`. A schema is resolved here
   * exactly as `readSchema` resolves one, so an annotated column needs its
   * adapter here too.
   */
  types?: ReadOptions["types"];
}

/** Parameters for `store.del`: `uns3`'s delete parameters, minus the key. */
export type DelParams = Omit<DeleteObjectParams, "key">;

/** Parameters for `store.list`: `uns3`'s list parameters, unchanged. */
export type ListParams = ListObjectsV2Params;

/**
 * What a remote object turns out to be, without downloading it: the facts the
 * store keeps about the object, and the facts the footer states about the file.
 */
export interface ParquetHead {
  /** Object size in bytes. */
  readonly size: number;
  /** Entity tag, where the store reports one. */
  readonly etag: string | undefined;
  /** The file's schema, in the shape `defineSchema` produces. */
  readonly schema: ParquetSchema;
  /** Rows the footer declares, across every row group. */
  readonly rowCount: number;
  /** Number of row groups, which is the range `GetOptions.groups` indexes. */
  readonly groupCount: number;
}

/**
 * A Parquet-aware store over one `uns3` client: `uns3`'s vocabulary, with
 * Parquet on the way in and out.
 *
 * Every method defers — this is the network, and there is nothing to be
 * synchronous about — so all of them return real promises rather than the
 * maybe-promises the local reader and writer deal in.
 */
export interface ParquetStore {
  /**
   * Uploads a Parquet file: finished bytes, a writer to finish, or rows and
   * their schema to build one from.
   *
   * @throws {TavolatoError} `ERR_STORE_INPUT_INVALID`, or anything
   * `createWriter` and `append` raise for rows that do not fit their schema.
   */
  put<TDefinition extends SchemaDefinition>(
    key: string,
    data: PutInput<TDefinition>,
    params?: PutParams,
  ): Promise<Response>;
  /**
   * Reads an object back as rows — the whole file, or only the columns and row
   * groups asked for.
   *
   * @throws {TavolatoError} `ERR_READ_*` as a local read would, plus
   * `ERR_STORE_OBJECT_CHANGED` and `ERR_STORE_RANGE_UNSATISFIED`.
   */
  get(key: string, options?: GetOptions): Promise<ParquetFile>;
  /**
   * Reads what an object *is* — size, etag, schema, row and group counts —
   * without downloading a single page.
   *
   * @throws {TavolatoError} `ERR_READ_MALFORMED`, `ERR_READ_UNSUPPORTED` or
   * `ERR_STORE_RANGE_UNSATISFIED`.
   */
  head(key: string, options?: HeadOptions): Promise<ParquetHead>;
  /** Deletes an object. A straight pass through to `client.del`. */
  del(key: string, params?: DelParams): Promise<Response>;
  /** Lists objects. A straight pass through to `client.list`. */
  list(params?: ListParams): Promise<ListObjectsV2Response>;
}

/** Bytes read from the end of an object when a read needs the footer. */
const DEFAULT_TAIL_BYTES = 64 * 1024;

/** The trailing envelope of every Parquet file: the footer length, then the magic. */
const FOOTER_SUFFIX = 4 + MAGIC.length;

/**
 * What a ranged read accepts: the partial answer it asked for, and the whole
 * body from a store that ignored the `Range` header altogether.
 *
 * Passed as `expectedStatus` so that a client checking statuses for us knows a
 * `206` is the point rather than a surprise.
 */
const RANGED_STATUS: number[] = [200, 206];

/** A half-open byte span of the object: `start` included, `end` not. */
interface Span {
  readonly start: number;
  readonly end: number;
}

/** One range as it came back: where it starts in the object, and its bytes. */
interface Fetched extends Span {
  readonly bytes: Uint8Array;
}

/** One row group a read wants, and the spans its selected chunks occupy. */
interface GroupPlan {
  readonly group: RowGroupInfo;
  readonly spans: readonly Span[];
}

/** Everything the tail of an object says, and what it cost to find out. */
interface RemoteFooter {
  readonly footer: FooterInfo;
  /** Object size in bytes. */
  readonly size: number;
  readonly etag: string | undefined;
  /** Where the metadata begins: no column chunk may reach past it. */
  readonly footerStart: number;
  /** The whole object, when a ranged request came back with all of it. */
  readonly whole: Uint8Array | undefined;
}

/**
 * Creates a Parquet store over an `uns3` client.
 *
 * The store speaks `uns3`'s own vocabulary — `put`, `get`, `head`, `del`,
 * `list` — with the key in front and everything else in an options object, and
 * with Parquet in the middle: `put` takes rows and uploads a file, `get` hands
 * rows back, `head` answers what a file holds without downloading it.
 *
 * **`get` is the reason this exists.** Ask for nothing in particular and it is
 * one plain GET of the whole object, decoded by `readParquet`. Ask for
 * `columns`, `groups`, or both, and it becomes a *ranged* read: the footer is
 * fetched from the object's tail, the byte spans of the selected column chunks
 * are computed from it, adjacent spans are coalesced, and only those ranges are
 * transferred. Two columns of a forty column file cost two columns of
 * bandwidth. The rows that come back are exactly what a local `readParquet` of
 * the whole object under the same options would have produced.
 *
 * `uns3` is an optional peer dependency, used here for its types alone: nothing
 * in this module imports it at runtime, it only calls the five methods of the
 * client you pass in.
 *
 * One thing is asked of that client, and only by the ranged path: its `get` has
 * to hand back a `206 Partial Content` rather than treat it as a failure, since
 * that is the correct answer to the `Range` header the store just sent — `uns3`
 * within the declared peer range does. A client that refuses one throws its own
 * error, untouched, out of `get` and `head`.
 *
 * @example
 * import { S3Client } from "uns3";
 * import { defineSchema } from "tavolato";
 * import { createParquetStore } from "tavolato/uns3";
 *
 * const store = createParquetStore(new S3Client({ ... }), { bucket: "metrics" });
 * const schema = defineSchema({ at: { type: "timestamp" }, n: { type: "i64" } });
 *
 * await store.put("events/2026-08-22.parquet", { schema, rows: [{ at: Date.now(), n: 1n }] });
 *
 * @example
 * // A writer already in hand works too: the store finishes it, asynchronous
 * // codec and all.
 * const writer = createWriter(schema);
 * writer.append({ at: Date.now(), n: 2n });
 * await store.put("events/2026-08-22.parquet", writer);
 *
 * @example
 * // One column of one row group: the rest of the object is never transferred.
 * const { rows } = await store.get("events/2026-08-22.parquet", {
 *   columns: ["n"],
 *   groups: [0],
 * });
 *
 * @example
 * // What is in there, without downloading it:
 * const { size, rowCount, groupCount, schema } = await store.head("events/2026-08-22.parquet");
 *
 * @throws {TavolatoError} `ERR_READ_OPTION_INVALID` where `tailBytes` cannot be
 * a byte count.
 */
export function createParquetStore(
  client: ParquetStoreClient,
  defaults: StoreDefaults = {},
): ParquetStore {
  assertOptionsObject(defaults, "StoreDefaults", "ERR_READ_OPTION_INVALID");
  const tailBytes = defaults.tailBytes ?? DEFAULT_TAIL_BYTES;
  if (!Number.isSafeInteger(tailBytes) || tailBytes < FOOTER_SUFFIX) {
    throw badOption(
      `StoreDefaults.tailBytes must be an integer of at least ${FOOTER_SUFFIX} — the bytes a Parquet file ends with — received ${describe(
        defaults.tailBytes,
      )}`,
    );
  }

  /** Applies the default bucket to a call's own parameters. */
  const addressed = <T extends { bucket?: string }>(params: T): T => ({
    ...params,
    bucket: params.bucket ?? defaults.bucket,
  });

  /**
   * Runs one request, turning the two ways a store can answer "the object
   * changed under you" into the one typed error.
   */
  const request = async (run: () => Promise<Response>): Promise<Response> => {
    let response: Response;
    try {
      response = await run();
    } catch (cause) {
      // Everything else is the client's error, in the client's own type, and
      // is left exactly as it was thrown.
      if (statusOf(cause) === 412) throw objectChanged(cause);
      throw cause;
    }
    if (response.status === 412) throw objectChanged(undefined);
    return response;
  };

  /** One ranged GET, pinned to `etag` where the object had one to pin to. */
  const ranged = async (
    key: string,
    params: Omit<GetObjectParams, "key" | "range">,
    range: Span,
    etag: string | undefined,
  ): Promise<Response> =>
    await request(
      async () =>
        await client.get({
          expectedStatus: RANGED_STATUS,
          ...addressed(params),
          ...(etag === undefined ? {} : { ifMatch: etag }),
          key,
          // `uns3` takes an inclusive end, as the `Range` header does.
          range: { start: range.start, end: range.end - 1 },
        }),
    );

  /** The object turned out to be in hand after all: read it as a local file. */
  const whole = (
    bytes: Uint8Array,
    etag: string | undefined,
    options: ReadOptions,
  ): RemoteFooter => ({
    footer: readFooter(bytes, options),
    size: bytes.length,
    etag,
    footerStart: bytes.length,
    whole: bytes,
  });

  /** Resolves the object's size when a valid `Content-Range` leaves its total unknown. */
  const sizeFromHead = async (
    key: string,
    params: Omit<HeadObjectParams, "key" | "range">,
    etag: string | undefined,
  ): Promise<number> => {
    const response = await request(
      async () =>
        await client.head({
          ...addressed(params),
          ...(etag === undefined ? {} : { ifMatch: etag }),
          key,
        }),
    );
    const length = response.headers.get("content-length");
    const size = length === null ? Number.NaN : Number(length);
    if (!Number.isSafeInteger(size) || size < 0) {
      throw new TavolatoError(
        `The store answered a ranged read of "${key}" with an unknown total, and its HEAD states no usable Content-Length`,
        "ERR_STORE_RANGE_UNSATISFIED",
      );
    }
    return size;
  };

  /**
   * Reads the object's tail and everything the footer says, in one request
   * where the footer fits in the tail and two where it does not.
   *
   * The object's size comes from the ranged response's own `Content-Range`
   * rather than from a `HEAD` in front of it, which is one request saved on
   * every read; the `HEAD` is only there for a valid response whose total is
   * `*`.
   */
  const open = async (
    key: string,
    params: Omit<GetObjectParams, "key" | "range">,
    options: ReadOptions,
  ): Promise<RemoteFooter> => {
    const first = await request(
      async () =>
        await client.get({
          expectedStatus: RANGED_STATUS,
          ...addressed(params),
          key,
          // No start: `bytes=-n` is the last `n` bytes, or the whole object
          // when it is shorter than that.
          range: { end: tailBytes },
        }),
    );
    const etag = etagOf(first);
    const body = await bytesOf(first);

    // A store that ignores `Range` answers `200` with everything. Nothing is
    // wrong and nothing needs saving: the file is right here.
    if (first.status === 200) {
      return whole(body, etag, options);
    }

    const tailRange = contentRangeOf(first);
    expectRangeBytes(tailRange, body, "the file's tail");
    const size = tailRange.total ?? (await sizeFromHead(key, params, etag));
    expectSuffix(tailRange, tailBytes, size);
    if (body.length === size) return whole(body, etag, options);

    let tail = body;
    const need = footerBytes(tail) + FOOTER_SUFFIX;
    if (need > size) {
      throw malformed(
        `The footer declares ${need - FOOTER_SUFFIX} bytes of metadata, which does not fit a ${size} byte file`,
      );
    }
    if (need > tail.length) {
      // The footer is bigger than the tail that was read: fetch exactly the
      // part that is missing, and put the two halves back together.
      const missing = { start: size - need, end: size - tail.length };
      const response = await ranged(key, params, missing, etag);
      const bytes = await bytesOf(response);
      if (response.status === 200) return whole(bytes, etag, options);
      expectSpan(response, missing, bytes, size, "the file's metadata");
      tail = concat(bytes, tail);
    }

    // `readFooter` wants a file, and a file is what it gets: the magic, the
    // metadata, its length and the magic again — the smallest envelope those
    // bytes are legal in. Every offset inside still points into the real
    // object, which is what the read then goes and fetches.
    const image = concat(MAGIC, tail.subarray(tail.length - need));
    return {
      footer: readFooter(image, options),
      size,
      etag,
      footerStart: size - need,
      whole: undefined,
    };
  };

  /** Decodes bytes already in hand, honouring both halves of the selection. */
  const local = async (
    bytes: Uint8Array,
    options: ReadOptions,
    groups: readonly number[] | undefined,
  ): Promise<ParquetFile> => {
    if (groups === undefined) return await readParquet(bytes, options);
    const footer = readFooter(bytes, options);
    const rows: ReadRow[] = [];
    for (const index of resolveGroups(groups, footer.rowGroups.length)) {
      // A cursor per group, exactly as `readRowGroups` gives each of its steps
      // one: they are independent reads that happen to share a buffer.
      await readRowGroup(
        new ByteReader(bytes.subarray(0, footer.pageBytesEnd), 0, undefined, MAGIC.length),
        footer,
        footer.rowGroups[index],
        rows,
        options.codecs,
      );
    }
    return { schema: footer.schema, rows };
  };

  return Object.freeze({
    async put<TDefinition extends SchemaDefinition>(
      key: string,
      data: PutInput<TDefinition>,
      params: PutParams = {},
    ): Promise<Response> {
      assertOptionsObject(params, "store.put params", "ERR_STORE_INPUT_INVALID");
      const { writer: writerOptions, ...rest } = params;
      const body = await finished(data, defaults.writer, writerOptions);
      return await client.put({
        contentType: PARQUET_CONTENT_TYPE,
        ...addressed(rest),
        key,
        body,
      });
    },

    async get(key: string, options: GetOptions = {}): Promise<ParquetFile> {
      assertOptionsObject(options, "GetOptions", "ERR_READ_OPTION_INVALID");
      const { columns, groups, codecs, types, ...params } = options;
      const read: ReadOptions = {
        codecs: codecs ?? defaults.codecs,
        types: types ?? defaults.types,
        ...(columns === undefined ? {} : { columns }),
      };
      // Checked before a single request goes out: an option that cannot be used
      // is the caller's mistake whatever the object holds.
      const wanted = requestedGroups(groups);

      if (wanted === undefined && columns === undefined) {
        // Nothing was narrowed, so there is nothing to be clever about: the
        // whole object is wanted, and one GET is the cheapest way to have it.
        const response = await request(async () => await client.get({ ...addressed(params), key }));
        return await readParquet(await bytesOf(response), read);
      }

      const opened = await open(key, params, read);
      if (opened.whole !== undefined) return await local(opened.whole, read, wanted);
      const { footer, etag, footerStart } = opened;

      // Which groups, which chunks, and therefore which bytes. Nothing has been
      // fetched yet: this is the footer talking.
      const indices = resolveGroups(
        wanted ?? every(footer.rowGroups.length),
        footer.rowGroups.length,
      );
      const plans: GroupPlan[] = [];
      const spans: Span[] = [];
      for (const index of indices) {
        const group = footer.rowGroups[index];
        assertChunkCount(footer, group);
        const own = footer.columns.map((column) =>
          chunkSpan(group.columns[column.index], column.name, footerStart),
        );
        for (const span of own) spans.push(span);
        plans.push({ group, spans: own });
      }

      const fetched: Fetched[] = [];
      for (const range of coalesce(spans)) {
        const response = await ranged(key, params, range, etag);
        const bytes = await bytesOf(response);
        // Same fallback as the tail read, one step further in: a store that
        // ignores ranges hands over everything, and everything is enough.
        if (response.status === 200) return await local(bytes, read, wanted);
        expectSpan(response, range, bytes, opened.size, "a column chunk");
        fetched.push({ ...range, bytes });
      }

      const rows: ReadRow[] = [];
      for (const plan of plans) {
        const { input, group } = windowOf(plan, footer, fetched);
        await readRowGroup(input, footer, group, rows, read.codecs);
      }
      return { schema: footer.schema, rows };
    },

    async head(key: string, options: HeadOptions = {}): Promise<ParquetHead> {
      assertOptionsObject(options, "HeadOptions", "ERR_READ_OPTION_INVALID");
      const { types, ...params } = options;
      const { footer, size, etag } = await open(key, params, { types: types ?? defaults.types });
      return Object.freeze({
        size,
        etag,
        schema: footer.schema,
        rowCount: footer.rowCount,
        groupCount: footer.rowGroups.length,
      });
    },

    async del(key: string, params: DelParams = {}): Promise<Response> {
      return await client.del({ ...addressed(params), key });
    },

    async list(params: ListParams = {}): Promise<ListObjectsV2Response> {
      return await client.list(addressed(params));
    },
  });
}

/**
 * Turns whatever `put` was handed into the bytes to upload.
 *
 * The three shapes are told apart by what they *are* rather than by a flag:
 * bytes are bytes, anything that can `finish()` is a writer, and what is left
 * has to be a schema and rows or it is not something this can upload at all.
 */
async function finished(
  data: unknown,
  defaultWriter: WriterOptions | undefined,
  callWriter: WriterOptions | undefined,
): Promise<Uint8Array> {
  if (data instanceof Uint8Array) return data;
  if (typeof (data as Partial<ParquetSource> | null | undefined)?.finish === "function") {
    return await (data as ParquetSource).finish();
  }
  const rows = data as Partial<ParquetRows> | null | undefined;
  if (
    typeof rows?.schema !== "object" ||
    rows.schema === null ||
    !Array.isArray(rows.schema.columns) ||
    typeof (rows.rows as Iterable<unknown> | null | undefined)?.[Symbol.iterator] !== "function"
  ) {
    throw new TavolatoError(
      `store.put takes a Uint8Array, a writer, or { schema, rows }, received ${describe(data)}`,
      "ERR_STORE_INPUT_INVALID",
    );
  }
  assertOptionalOptionsObject(defaultWriter, "StoreDefaults.writer", "ERR_WRITER_OPTION_INVALID");
  assertOptionalOptionsObject(callWriter, "PutParams.writer", "ERR_WRITER_OPTION_INVALID");
  const writer = { ...defaultWriter, ...callWriter };
  const parquet = createWriter(rows.schema, writer);
  await parquet.appendAll(rows.rows as Iterable<Row<SchemaDefinition>>);
  return await parquet.finish();
}

/** Every row group index of a file, for a read that narrowed only its columns. */
function every(count: number): readonly number[] {
  const indices: number[] = [];
  for (let index = 0; index < count; index++) indices.push(index);
  return indices;
}

/**
 * Validates `groups` as a selection request, before any of it is checked
 * against a file.
 *
 * The mirror of what `ReadOptions.columns` gets: a list, of indices, of at
 * least one, each named once. Whether they are *in* the file is a question only
 * the footer can answer, and {@link resolveGroups} asks it there.
 */
function requestedGroups(groups: readonly number[] | undefined): readonly number[] | undefined {
  if (groups === undefined) return undefined;
  if (!Array.isArray(groups)) {
    throw badOption(
      `GetOptions.groups must be an array of row group indices, received ${describe(groups)}`,
    );
  }
  if (groups.length === 0) {
    throw badOption(
      "GetOptions.groups is empty: a read that reads no row group at all is a mistake rather than a request",
    );
  }
  const requested: readonly unknown[] = groups;
  const wanted = new Set<number>();
  for (const [position, index] of requested.entries()) {
    if (typeof index !== "number" || !Number.isSafeInteger(index) || index < 0) {
      throw badOption(
        `GetOptions.groups[${position}] must be a row group index, received ${describe(index)}`,
      );
    }
    // Refused rather than deduplicated, exactly as a repeated column name is:
    // both are a caller saying something they did not mean.
    if (wanted.has(index)) {
      throw badOption(
        `GetOptions.groups names ${index} twice; a row group is read once or not at all`,
      );
    }
    wanted.add(index);
  }
  return groups;
}

/**
 * Checks a group selection against the file and puts it in the file's order.
 *
 * Ascending, whatever order the indices arrived in: rows come back in file
 * order for the same reason projected columns do.
 */
function resolveGroups(groups: readonly number[], count: number): readonly number[] {
  for (const index of groups) {
    if (index < count) continue;
    throw badOption(
      `GetOptions.groups names row group ${index}, which this file does not have; it holds ${count}`,
    );
  }
  return [...groups].sort((left, right) => left - right);
}

/**
 * Where one column chunk lives in the object.
 *
 * A chunk starts at its dictionary page where it has one and at its first data
 * page where it does not — a dictionary-encoded chunk is refused by the decode
 * a moment later, but it is refused *there*, with the message a local read
 * gives, rather than here by a byte count that failed to add up.
 */
function chunkSpan(chunk: ColumnChunkInfo, column: string, footerStart: number): Span {
  const size = chunk.totalCompressedSize;
  const start = Math.min(chunk.dictionaryPageOffset ?? chunk.dataPageOffset, chunk.dataPageOffset);
  const end = start + size;
  const outside =
    !Number.isSafeInteger(end) ||
    end > footerStart ||
    (size === 0 ? start < 0 : start < MAGIC.length);
  if (outside) {
    throw malformed(
      `Column "${column}" holds ${size} bytes at offset ${start}, which is not inside the ${footerStart} bytes of pages this file has`,
      column,
    );
  }
  return { start, end };
}

/**
 * Merges the spans a read needs into the ranges it will ask for.
 *
 * Adjacent and overlapping spans become one: the columns of a row group are
 * written back to back, so a read of neighbouring columns — or of every column
 * of one group — collapses into a single request rather than one per chunk.
 * A gap, however small, is left as a gap: closing it would mean paying for
 * bytes the read has already decided it does not want.
 */
function coalesce(spans: readonly Span[]): readonly Span[] {
  const sorted = [...spans].sort((left, right) => left.start - right.start);
  const ranges: Span[] = [];
  for (const span of sorted) {
    if (span.start === span.end) continue;
    const last = ranges[ranges.length - 1];
    if (last !== undefined && span.start <= last.end) {
      if (span.end > last.end) ranges[ranges.length - 1] = { start: last.start, end: span.end };
      continue;
    }
    ranges.push(span);
  }
  return ranges;
}

/**
 * Lays one row group's chunks out in a buffer of their own and points the group
 * at them.
 *
 * A sparse read has no whole file behind its footer offsets, so the bytes that
 * were fetched are copied into a window and each chunk offset is rewritten to
 * where its bounded view begins. The decode that follows is the same
 * `readRowGroup` a local read runs, over a buffer that holds the wanted chunks
 * and nothing else: memory is the bytes fetched for one group, not the size of
 * the object.
 *
 * Only the selected chunks move. The rest keep their file offsets, which is
 * harmless because nothing reads them — and keeps the chunk list the length the
 * footer says it is, which `assertChunkCount` insists on.
 */
function windowOf(
  plan: GroupPlan,
  footer: FooterInfo,
  fetched: readonly Fetched[],
): { input: ByteReader; group: RowGroupInfo } {
  let total = 0;
  for (const span of plan.spans) total += span.end - span.start;

  const window = new Uint8Array(total);
  const columns = [...plan.group.columns];
  let at = 0;
  for (const [position, column] of footer.columns.entries()) {
    const chunk = plan.group.columns[column.index];
    const span = plan.spans[position];
    if (span.start !== span.end) window.set(sliceOf(fetched, span), at);
    columns[column.index] = { ...chunk, dataPageOffset: at + (chunk.dataPageOffset - span.start) };
    at += span.end - span.start;
  }
  return {
    input: new ByteReader(window),
    group: {
      columns,
      totalByteSize: plan.group.totalByteSize,
      numRows: plan.group.numRows,
    },
  };
}

/** The bytes of one span, out of the range that was fetched to hold it. */
function sliceOf(fetched: readonly Fetched[], span: Span): Uint8Array {
  for (const range of fetched) {
    if (span.start < range.start || span.end > range.end) continue;
    return range.bytes.subarray(span.start - range.start, span.end - range.start);
  }
  // Unreachable while the spans and the ranges come from the same coalesce.
  throw malformed(`No fetched range holds bytes ${span.start} to ${span.end}`);
}

/** How many bytes of metadata the tail says the footer holds. */
function footerBytes(tail: Uint8Array): number {
  if (tail.length < FOOTER_SUFFIX) {
    throw malformed(
      `A Parquet file ends with ${FOOTER_SUFFIX} bytes of envelope, and only ${tail.length} were read`,
    );
  }
  const magic = tail.subarray(tail.length - MAGIC.length);
  for (const [index, byte] of MAGIC.entries()) {
    if (magic[index] !== byte) {
      throw malformed("Not a Parquet file: it must both start and end with the PAR1 magic");
    }
  }
  const view = new DataView(tail.buffer, tail.byteOffset, tail.byteLength);
  const length = view.getUint32(tail.length - FOOTER_SUFFIX, true);
  if (length === 0) throw malformed("The footer declares 0 bytes of metadata");
  return length;
}

/** Holds a store to an explicit range before any returned bytes are believed. */
function expectSpan(
  response: Response,
  span: Span,
  bytes: Uint8Array,
  size: number,
  what: string,
): void {
  const range = contentRangeOf(response);
  expectRangeBytes(range, bytes, what);
  if (range.start !== span.start || range.end !== span.end - 1) {
    throw rangeUnsatisfied(
      `A ranged read of ${what} asked for bytes ${span.start}-${span.end - 1} and received bytes ${range.start}-${range.end}`,
    );
  }
  expectRangeTotal(range, size, what);
}

/** The object moved under a read that had already started. */
function objectChanged(cause: unknown): TavolatoError {
  return new TavolatoError(
    "The object changed between two requests of the same read, so the footer no longer describes the bytes being fetched",
    "ERR_STORE_OBJECT_CHANGED",
    undefined,
    cause,
  );
}

/** The HTTP status an S3 client hung on an error, where it hung one there. */
function statusOf(error: unknown): number | undefined {
  const status = (error as { status?: unknown } | null | undefined)?.status;
  return typeof status === "number" ? status : undefined;
}

/** A response body, as bytes. */
async function bytesOf(response: Response): Promise<Uint8Array> {
  return new Uint8Array(await response.arrayBuffer());
}

/** The response's entity tag, where it has one. */
function etagOf(response: Response): string | undefined {
  return response.headers.get("etag") ?? undefined;
}

interface ContentRange {
  readonly start: number;
  readonly end: number;
  /** Undefined is the RFC `*`: a structurally valid, unknown total. */
  readonly total: number | undefined;
}

/** Parses the single byte range every accepted `206` must state. */
function contentRangeOf(response: Response): ContentRange {
  const header = response.headers.get("content-range");
  if (header === null) {
    throw rangeUnsatisfied("A 206 Partial Content response has no Content-Range header");
  }
  const match = /^bytes\s+(\d+)-(\d+)\/(\d+|\*)$/i.exec(header.trim());
  if (match === null) {
    throw rangeUnsatisfied(
      `A 206 Partial Content response has malformed Content-Range ${JSON.stringify(header)}`,
    );
  }
  const start = Number(match[1]);
  const end = Number(match[2]);
  const total = match[3] === "*" ? undefined : Number(match[3]);
  if (
    !Number.isSafeInteger(start) ||
    !Number.isSafeInteger(end) ||
    start < 0 ||
    end < start ||
    (total !== undefined && (!Number.isSafeInteger(total) || total <= 0 || end >= total))
  ) {
    throw rangeUnsatisfied(
      `A 206 Partial Content response has invalid Content-Range ${JSON.stringify(header)}`,
    );
  }
  return { start, end, total };
}

/** Proves the body is exactly the single range its response declares. */
function expectRangeBytes(range: ContentRange, bytes: Uint8Array, what: string): void {
  const declared = range.end - range.start + 1;
  if (bytes.length === declared) return;
  throw rangeUnsatisfied(
    `A ranged read of ${what} declares ${declared} bytes at offset ${range.start} and received ${bytes.length}`,
  );
}

/** Holds a known object size against a response that chose to state its total. */
function expectRangeTotal(range: ContentRange, size: number, what: string): void {
  if (range.total === undefined || range.total === size) return;
  throw rangeUnsatisfied(
    `A ranged read of ${what} belongs to a ${size} byte object but its Content-Range states ${range.total}`,
  );
}

/** Proves a suffix response covers exactly the tail asked for. */
function expectSuffix(range: ContentRange, wanted: number, size: number): void {
  expectRangeTotal(range, size, "the file's tail");
  const start = Math.max(0, size - wanted);
  const end = size - 1;
  if (range.start === start && range.end === end) return;
  throw rangeUnsatisfied(
    `A ranged read of the file's tail asked for its last ${wanted} bytes and received bytes ${range.start}-${range.end} of ${size}`,
  );
}

/** One typed refusal for every way a partial answer can contradict its request. */
function rangeUnsatisfied(message: string): TavolatoError {
  return new TavolatoError(message, "ERR_STORE_RANGE_UNSATISFIED");
}

/** Joins two byte runs into one. */
function concat(head: Uint8Array, tail: Uint8Array): Uint8Array {
  const joined = new Uint8Array(head.length + tail.length);
  joined.set(head);
  joined.set(tail, head.length);
  return joined;
}
