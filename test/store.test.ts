import { gunzipSync, gzipSync } from "node:zlib";
import { beforeAll, describe, expect, it } from "vitest";
import {
  createWriter,
  decimal,
  defineSchema,
  readParquet,
  readRowGroups,
  uuid,
} from "../src/index.ts";
import type { ParquetFile, ReadOptions, ReadRow, Row, SchemaDefinition } from "../src/types.ts";
import type { ParquetSchema } from "../src/types.ts";
import {
  type ColumnChunkMeta,
  CompressionCodec,
  encodeFileMetadata,
  type RowGroupMeta,
  snapshotColumn,
} from "../src/internal/format.ts";
import { createParquetStore, PARQUET_CONTENT_TYPE, type ParquetStore } from "../src/uns3.ts";
import { sealFile, startFile, writeDataPage } from "./_build.ts";
import { expectError, expectRejection } from "./_errors.ts";
import { FakeS3, FakeS3Error, racing, wrap } from "./_store.ts";
import { sync } from "./_sync.ts";

/*
 * ---------------------------------------------------------------------------
 * The store, against an S3 that only exists in this process.
 *
 * Two things are being asserted throughout, and they pull in opposite
 * directions on purpose. The rows a ranged read hands back have to be *exactly*
 * what a local `readParquet` of the whole object would have produced for the
 * same options — that is the correctness half, and it is checked against the
 * local reader every time rather than against a fixture. And the bytes a ranged
 * read transfers have to be *fewer* than the object holds — that is the point
 * half, and it is checked in numbers the fake counted rather than in adjectives.
 * ---------------------------------------------------------------------------
 */

const GZIP = { name: "GZIP" as const, compress: gzipSync };
const codecs = { GZIP: { decompress: (page: Uint8Array) => gunzipSync(page) } };
const money = decimal({ precision: 12, scale: 2 });
const id = uuid();

/** A file wide enough for a projection to be worth something. */
const catalogue = defineSchema({
  ref: { type: id },
  n: { type: "i64" },
  name: { type: "string" },
  price: { type: money },
  qty: { type: "i32" },
  live: { type: "bool" },
  at: { type: "timestamp" },
  doc: { type: "json" },
  note: { type: "string", optional: true },
});

const ROWS = 8000;
const GROUP = 2000;

function catalogueRows(count: number): Row<typeof catalogue.definition>[] {
  return Array.from({ length: count }, (_unused, index) => ({
    ref: `0000000${(index % 10).toString(16)}-1111-4222-8333-44444444444${(index % 10).toString(16)}`,
    n: BigInt(index) * 7n,
    name: `product ${index} — ünïcode ${index % 13}`,
    price: `${index % 5000}.${(index % 100).toString().padStart(2, "0")}`,
    qty: index % 977,
    live: index % 3 === 0,
    at: 1_700_000_000_000 + index * 1000,
    doc: { index, tags: ["alpha", "beta"], nested: { even: index % 2 === 0 } },
    note: index % 7 === 0 ? null : `note ${index}`,
  }));
}

/** Builds a file with tavolato's own writer. */
function build<TDefinition extends SchemaDefinition>(
  schema: ParquetSchema<TDefinition>,
  rows: readonly Row<TDefinition>[],
  options?: { rowGroupSize?: number; codec?: typeof GZIP },
): Uint8Array {
  const writer = createWriter(schema, { rowGroupSize: GROUP, ...options });
  for (const row of rows) sync(writer.append(row));
  return sync(writer.finish());
}

/**
 * What a local read of the whole object gives for the same selection: the
 * oracle every remote read is measured against.
 */
async function localRows(
  bytes: Uint8Array,
  options: ReadOptions,
  groups?: readonly number[],
): Promise<ReadRow[]> {
  if (groups === undefined) return (await readParquet(bytes, options)).rows;
  const rows: ReadRow[] = [];
  let index = 0;
  for (const step of readRowGroups(bytes, options)) {
    const group = await step;
    if (groups.includes(index)) rows.push(...group);
    index++;
  }
  return rows;
}

const plain = build(catalogue, catalogueRows(ROWS));
const compressed = build(catalogue, catalogueRows(600), { rowGroupSize: 200, codec: GZIP });
const single = build(defineSchema({ n: { type: "i64" } }), [{ n: 1n }, { n: 2n }]);

const read: ReadOptions = { types: [id, money] };

function storeOf(quirks: Partial<FakeS3["quirks"]> = {}): {
  s3: FakeS3;
  store: ParquetStore;
} {
  const s3 = new FakeS3();
  Object.assign(s3.quirks, quirks);
  const store = createParquetStore(s3, { bucket: "b", types: [id, money], codecs });
  return { s3, store };
}

describe("createParquetStore: put", () => {
  it("uploads rows it builds a file out of, and reads them back", async () => {
    const { s3, store } = storeOf();
    const rows = catalogueRows(50);

    const response = await store.put("cat.parquet", { schema: catalogue, rows });

    expect(response.status).toBe(200);
    const stored = s3.stored("cat.parquet");
    expect(stored).toBeInstanceOf(Uint8Array);
    const local = await readParquet(stored as Uint8Array, read);
    expect(local.rows).toHaveLength(50);
    expect(local.rows[7].name).toBe(rows[7].name);
  });

  it("stores raw bytes verbatim", async () => {
    const { s3, store } = storeOf();
    await store.put("raw.parquet", plain);
    expect(s3.stored("raw.parquet")).toBe(plain);
  });

  it("stamps the Parquet media type and the default bucket", async () => {
    const s3 = new FakeS3();
    const puts: string[] = [];
    const store = createParquetStore(
      wrap(s3, {
        put: async (params) => {
          puts.push(`${params.bucket}/${params.key}/${String(params.contentType)}`);
          return await s3.put(params);
        },
      }),
      { bucket: "metrics" },
    );

    await store.put("a.parquet", single);
    await store.put("b.parquet", single, {
      bucket: "other",
      contentType: "application/octet-stream",
    });

    expect(puts).toEqual([
      `metrics/a.parquet/${PARQUET_CONTENT_TYPE}`,
      "other/b.parquet/application/octet-stream",
    ]);
  });

  it("finishes a writer handed over directly", async () => {
    const { s3, store } = storeOf();
    const writer = createWriter(defineSchema({ n: { type: "i64" } }));
    writer.append({ n: 42n });

    await store.put("w.parquet", writer);

    expect(writer.finished).toBe(true);
    const file = sync(readParquet(s3.stored("w.parquet") as Uint8Array));
    expect(file.rows).toEqual([{ n: 42n }]);
  });

  it("builds the file with the writer options it was given", async () => {
    const { s3, store } = storeOf();
    const rows = catalogueRows(300);

    await store.put(
      "gz.parquet",
      { schema: catalogue, rows },
      { writer: { rowGroupSize: 100, codec: GZIP } },
    );

    const bytes = s3.stored("gz.parquet") as Uint8Array;
    const walk = readRowGroups(bytes, { ...read, codecs });
    expect(walk.groupCount).toBe(3);
    // Compressed: the same rows, written plain, take more room.
    expect(bytes.length).toBeLessThan(build(catalogue, rows, { rowGroupSize: 100 }).length);
  });

  it("takes writer options from the factory too", async () => {
    const s3 = new FakeS3();
    const store = createParquetStore(s3, { bucket: "b", writer: { rowGroupSize: 25 } });
    await store.put("d.parquet", { schema: catalogue, rows: catalogueRows(100) });
    expect(readRowGroups(s3.stored("d.parquet") as Uint8Array, read).groupCount).toBe(4);
  });

  it("refuses something that is neither bytes, a writer, nor rows", async () => {
    const { store } = storeOf();
    for (const data of [null, undefined, 42, "bytes", {}, { schema: catalogue }]) {
      await expectRejection(
        "ERR_STORE_INPUT_INVALID",
        store.put("x.parquet", data as unknown as Uint8Array),
      );
    }
  });

  it("passes every other put parameter through", async () => {
    const s3 = new FakeS3();
    const store = createParquetStore(s3, { bucket: "b" });
    await store.put("x.parquet", single, { ifNoneMatch: "*" });
    await store.put("x.parquet", single, { ifMatch: '"nope"' });
    // uns3 treats a 412 from a put as an answer rather than an error.
    expect(s3.requests.at(-1)).toMatchObject({ method: "PUT", status: 412 });
  });
});

describe("createParquetStore: get", () => {
  it("reads the whole object in one plain GET when nothing is narrowed", async () => {
    const { s3, store } = storeOf();
    s3.seed("cat.parquet", plain);

    const file = await store.get("cat.parquet");

    expect(file.rows).toEqual(await localRows(plain, read));
    expect(s3.requestCount).toBe(1);
    expect(s3.ranges).toEqual([undefined]);
    expect(s3.bytesServed).toBe(plain.length);
  });

  it("transfers a fraction of the object for a projected read", async () => {
    const { s3, store } = storeOf();
    s3.seed("cat.parquet", plain);

    const file = await store.get("cat.parquet", { columns: ["n"] });

    expect(file.rows).toEqual(await localRows(plain, { ...read, columns: ["n"] }));
    expect(file.schema.columns.map((column) => column.name)).toEqual(["n"]);
    expect(s3.bytesServed).toBeLessThan(plain.length / 4);
  });

  it("transfers a fraction of the object for a row group subset", async () => {
    const { s3, store } = storeOf();
    s3.seed("cat.parquet", plain);

    const file = await store.get("cat.parquet", { groups: [2] });

    expect(file.rows).toEqual(await localRows(plain, read, [2]));
    expect(file.rows).toHaveLength(GROUP);
    expect(s3.bytesServed).toBeLessThan(plain.length / 2);
  });

  it("transfers least of all when both halves are narrowed", async () => {
    const { s3, store } = storeOf();
    s3.seed("cat.parquet", plain);

    const file = await store.get("cat.parquet", { columns: ["n", "qty"], groups: [1] });

    expect(file.rows).toEqual(await localRows(plain, { ...read, columns: ["n", "qty"] }, [1]));
    // One row group of two narrow columns out of four groups of nine columns.
    expect(s3.bytesServed).toBeLessThan(plain.length / 10);
  });

  it("coalesces the chunks of neighbouring columns into one request", async () => {
    const { s3, store } = storeOf();
    s3.seed("cat.parquet", plain);

    // `n` and `name` sit next to each other in every row group, so each group
    // is one range rather than two: four groups, four ranged reads, plus the
    // tail that found the footer.
    await store.get("cat.parquet", { columns: ["n", "name"] });

    const ranges = s3.requests.filter((request) => request.status === 206);
    expect(ranges).toHaveLength(5);
  });

  it("asks for one range per group and never for the columns in between", async () => {
    const { s3, store } = storeOf();
    s3.seed("cat.parquet", plain);

    await store.get("cat.parquet", { columns: ["n", "doc"], groups: [0] });

    // `n` and `doc` have five columns between them: two ranges, not one.
    const ranges = s3.requests.filter((request) => request.status === 206).slice(1);
    expect(ranges).toHaveLength(2);
    expect(ranges[0].bytes + ranges[1].bytes).toBeLessThan(plain.length / 8);
  });

  it("needs no second request for an object smaller than the tail", async () => {
    const { s3, store } = storeOf();
    s3.seed("small.parquet", single);

    const file = await store.get("small.parquet", { columns: ["n"] });

    expect(file.rows).toEqual([{ n: 1n }, { n: 2n }]);
    // The suffix range came back with the whole file, so there is nothing left
    // to fetch: the read is over.
    expect(s3.requestCount).toBe(1);
  });
});

describe("createParquetStore: ranged reads are the local read", () => {
  const cases: {
    name: string;
    bytes: () => Uint8Array;
    options: ReadOptions;
    columns?: readonly string[];
    groups?: readonly number[];
  }[] = [
    { name: "every column of every group", bytes: () => plain, options: read },
    {
      name: "a projection over every group",
      bytes: () => plain,
      options: read,
      columns: ["n", "at", "note"],
    },
    { name: "one group, every column", bytes: () => plain, options: read, groups: [1] },
    { name: "several groups out of order", bytes: () => plain, options: read, groups: [3, 0] },
    {
      name: "a projection of adapter columns",
      bytes: () => plain,
      options: read,
      columns: ["ref", "price"],
      groups: [0, 2],
    },
    {
      name: "a json column on its own",
      bytes: () => plain,
      options: read,
      columns: ["doc"],
      groups: [2],
    },
    {
      name: "an optional column with nulls in it",
      bytes: () => plain,
      options: read,
      columns: ["note"],
    },
    {
      name: "a compressed file",
      bytes: () => compressed,
      options: { ...read, codecs },
      columns: ["n", "doc"],
    },
    {
      name: "a compressed file, one group",
      bytes: () => compressed,
      options: { ...read, codecs },
      groups: [1],
    },
    { name: "a two row file", bytes: () => single, options: {}, columns: ["n"] },
  ];

  it.each(cases)("$name", async ({ bytes, options, columns, groups }) => {
    const { s3, store } = storeOf();
    s3.seed("x.parquet", bytes());

    const file = await store.get("x.parquet", {
      ...(columns === undefined ? {} : { columns }),
      ...(groups === undefined ? {} : { groups }),
    });

    // The whole of the local read, under the same options, over the whole of
    // the file: schema included, and column by column rather than by name.
    const read = { ...options, ...(columns === undefined ? {} : { columns }) };
    expect(file.rows).toEqual(await localRows(bytes(), read, groups));
    expect(file.schema).toEqual((await readParquet(bytes(), read)).schema);
  });

  it("reads rows in file order however the groups were asked for", async () => {
    const { s3, store } = storeOf();
    s3.seed("x.parquet", plain);

    const file = await store.get("x.parquet", { groups: [3, 1], columns: ["n"] });

    expect(file.rows).toEqual(await localRows(plain, { ...read, columns: ["n"] }, [1, 3]));
    expect(file.rows[0].n).toBe(BigInt(GROUP) * 7n);
  });

  it("lifts a column's refusals exactly as a local projection does", async () => {
    const { s3 } = storeOf();
    s3.seed("x.parquet", plain);
    // No adapters registered: `ref` and `price` are annotated columns nothing
    // claims, and a read of the whole file is refused for it...
    const bare = createParquetStore(s3, { bucket: "b" });
    await expectRejection("ERR_READ_UNSUPPORTED", bare.get("x.parquet"));

    // ...while a read that projects them away is not.
    const file = await bare.get("x.parquet", { columns: ["n", "qty"], groups: [0] });
    expect(file.rows).toHaveLength(GROUP);
  });

  it("refuses a compressed chunk whose codec nobody registered", async () => {
    const s3 = new FakeS3();
    s3.seed("gz.parquet", compressed);
    const bare = createParquetStore(s3, { bucket: "b", types: [id, money] });
    await expectRejection("ERR_READ_UNSUPPORTED", bare.get("gz.parquet", { groups: [0] }));
  });
});

describe("createParquetStore: footers bigger than the tail", () => {
  let wide: Uint8Array;
  let names: string[];

  beforeAll(() => {
    // 120 columns of metadata is far more than the 512 byte tail below, and a
    // little more than 8 KiB in total: the read has to notice and come back
    // for the rest.
    names = Array.from({ length: 120 }, (_unused, index) => `column_number_${index}`);
    const definition: SchemaDefinition = {};
    for (const name of names) definition[name] = { type: "i32" };
    const schema = defineSchema(definition);
    const rows = Array.from({ length: 40 }, (_unused, row) => {
      const value: Record<string, number> = {};
      for (const [index, name] of names.entries()) value[name] = row * 1000 + index;
      return value;
    });
    wide = build(schema, rows as never, { rowGroupSize: 20 });
  });

  it("fetches the part of the footer the tail missed", async () => {
    const s3 = new FakeS3();
    const store = createParquetStore(s3, { bucket: "b", tailBytes: 512 });
    s3.seed("wide.parquet", wide);

    const file = await store.get("wide.parquet", { columns: [names[0], names[119]] });

    expect(file.rows).toEqual(await localRows(wide, { columns: [names[0], names[119]] }));
    const partial = s3.requests.filter((request) => request.status === 206);
    // The tail, the rest of the footer, and then the chunks.
    expect(partial[0].bytes).toBe(512);
    expect(partial[1].bytes).toBeGreaterThan(512);
    expect(s3.bytesServed).toBeLessThan(wide.length);
  });

  it("falls back when the range is ignored while the rest of the footer is fetched", async () => {
    const s3 = new FakeS3();
    s3.seed("wide.parquet", wide);
    const store = createParquetStore(
      racing(s3, () => {
        s3.quirks.ignoreRange = true;
      }),
      { bucket: "b", tailBytes: 512 },
    );

    const file = await store.get("wide.parquet", { columns: [names[3]], groups: [1] });

    expect(file.rows).toEqual(await localRows(wide, { columns: [names[3]] }, [1]));
    expect(s3.requests.map((request) => request.status)).toEqual([206, 200]);
  });

  it("reads the same footer whatever the tail size", async () => {
    const s3 = new FakeS3();
    s3.seed("wide.parquet", wide);
    const tight = createParquetStore(s3, { bucket: "b", tailBytes: 8 });
    const roomy = createParquetStore(s3, { bucket: "b" });

    expect(await tight.head("wide.parquet")).toEqual(await roomy.head("wide.parquet"));
  });

  it("refuses a tail size that cannot hold the envelope", () => {
    for (const tailBytes of [0, 7, -1, 1.5, Number.NaN, "64" as unknown as number]) {
      expect(() => createParquetStore(new FakeS3(), { tailBytes })).toThrowError(
        /StoreDefaults\.tailBytes/,
      );
    }
  });
});

describe("createParquetStore: stores that ignore Range", () => {
  it("falls back to the whole body when the tail read is answered with a 200", async () => {
    const { s3, store } = storeOf({ ignoreRange: true });
    s3.seed("cat.parquet", plain);

    const file = await store.get("cat.parquet", { columns: ["n"], groups: [1] });

    expect(file.rows).toEqual(await localRows(plain, { ...read, columns: ["n"] }, [1]));
    // One request, the whole object, and a correct answer out of it.
    expect(s3.requestCount).toBe(1);
    expect(s3.bytesServed).toBe(plain.length);
  });

  it("falls back when the range is ignored only once the chunks are asked for", async () => {
    const s3 = new FakeS3();
    s3.seed("cat.parquet", plain);
    // The tail comes back as a proper 206; the store starts ignoring ranges
    // right afterwards, which is the worst moment for it to happen.
    const store2 = createParquetStore(
      racing(s3, () => {
        s3.quirks.ignoreRange = true;
      }),
      { bucket: "b", types: [id, money] },
    );

    const file = await store2.get("cat.parquet", { columns: ["n"], groups: [0] });

    expect(file.rows).toEqual(await localRows(plain, { ...read, columns: ["n"] }, [0]));
    expect(s3.requests.map((request) => request.status)).toEqual([206, 200]);
  });

  it("refuses a partial answer without a Content-Range", async () => {
    const { s3, store } = storeOf({ omitContentRange: true });
    s3.seed("cat.parquet", plain);

    await expectRejection(
      "ERR_STORE_RANGE_UNSATISFIED",
      store.get("cat.parquet", { columns: ["n"], groups: [0] }),
    );
  });

  it("refuses a malformed Content-Range", async () => {
    const { s3, store } = storeOf({ contentRange: "bytes nonsense" });
    s3.seed("cat.parquet", plain);

    await expectRejection(
      "ERR_STORE_RANGE_UNSATISFIED",
      store.get("cat.parquet", { columns: ["n"], groups: [0] }),
    );
  });

  it("uses HEAD to resolve an unknown total and accepts later unknown totals", async () => {
    const { s3, store } = storeOf({ contentRangeTotal: "*" });
    s3.seed("cat.parquet", plain);

    const file = await store.get("cat.parquet", { columns: ["n"], groups: [0] });

    expect(file.rows).toEqual(await localRows(plain, { ...read, columns: ["n"] }, [0]));
    expect(s3.requests.map((request) => request.method)).toEqual(["GET", "HEAD", "GET"]);
  });

  it("refuses an unknown total when HEAD has no usable size", async () => {
    const { s3, store } = storeOf({ contentRangeTotal: "*", omitContentLength: true });
    s3.seed("cat.parquet", plain);

    await expectRejection(
      "ERR_STORE_RANGE_UNSATISFIED",
      store.get("cat.parquet", { columns: ["n"], groups: [0] }),
    );
  });

  it("pins the HEAD that resolves an unknown total to the tail's etag", async () => {
    const s3 = new FakeS3();
    s3.quirks.contentRangeTotal = "*";
    s3.seed("cat.parquet", plain);
    const store = createParquetStore(
      racing(s3, (answered) => {
        if (answered === 0) s3.replace("cat.parquet", plain);
      }),
      { bucket: "b", types: [id, money] },
    );

    await expectRejection(
      "ERR_STORE_OBJECT_CHANGED",
      store.get("cat.parquet", { columns: ["n"], groups: [0] }),
    );
  });

  it("refuses a later partial answer without a Content-Range", async () => {
    const s3 = new FakeS3();
    s3.seed("cat.parquet", plain);
    const store = createParquetStore(
      racing(s3, (answered) => {
        if (answered === 0) s3.quirks.omitContentRange = true;
      }),
      { bucket: "b", types: [id, money] },
    );

    await expectRejection(
      "ERR_STORE_RANGE_UNSATISFIED",
      store.get("cat.parquet", { columns: ["n"], groups: [0] }),
    );
  });

  it("refuses an equal-length response from the wrong offset", async () => {
    const { s3, store } = storeOf({ shiftRanges: 1 });
    s3.seed("cat.parquet", plain);

    await expectRejection(
      "ERR_STORE_RANGE_UNSATISFIED",
      store.get("cat.parquet", { columns: ["n"], groups: [0] }),
    );
  });

  it("refuses a Content-Range total that changes mid-read", async () => {
    const s3 = new FakeS3();
    s3.seed("cat.parquet", plain);
    const store = createParquetStore(
      racing(s3, (answered) => {
        if (answered === 0) s3.quirks.contentRangeTotal = plain.length + 1;
      }),
      { bucket: "b", types: [id, money] },
    );

    await expectRejection(
      "ERR_STORE_RANGE_UNSATISFIED",
      store.get("cat.parquet", { columns: ["n"], groups: [0] }),
    );
  });

  it("refuses bytes that are not the ones it asked for", async () => {
    const { s3, store } = storeOf({ truncateRanges: true });
    s3.seed("cat.parquet", plain);

    await expectRejection(
      "ERR_STORE_RANGE_UNSATISFIED",
      store.get("cat.parquet", { columns: ["n"] }),
    );
  });
});

describe("createParquetStore: the object changing under a read", () => {
  it("pins the read to the etag the footer came with", async () => {
    const { s3, store } = storeOf();
    s3.seed("cat.parquet", plain);

    await store.get("cat.parquet", { columns: ["n"], groups: [0] });

    const etag = s3.etag("cat.parquet");
    const conditional = s3.requests.filter((request) => request.ifMatch !== undefined);
    expect(conditional).not.toHaveLength(0);
    for (const request of conditional) expect(request.ifMatch).toBe(etag);
  });

  it("says so when the object is replaced mid-read", async () => {
    const s3 = new FakeS3();
    s3.seed("cat.parquet", plain);
    const store = createParquetStore(
      racing(s3, (answered) => {
        if (answered === 0) s3.replace("cat.parquet", plain);
      }),
      { bucket: "b", types: [id, money] },
    );

    const error = await expectRejection(
      "ERR_STORE_OBJECT_CHANGED",
      store.get("cat.parquet", { columns: ["n"] }),
    );
    // The client's own error is kept underneath the typed one.
    expect(error.cause).toBeInstanceOf(FakeS3Error);
  });

  it("says so for a client that returns the 412 rather than throwing it", async () => {
    const s3 = new FakeS3();
    s3.quirks.neverThrow = true;
    s3.seed("cat.parquet", plain);
    const store = createParquetStore(
      racing(s3, (answered) => {
        if (answered === 0) s3.replace("cat.parquet", plain);
      }),
      { bucket: "b", types: [id, money] },
    );

    const error = await expectRejection(
      "ERR_STORE_OBJECT_CHANGED",
      store.get("cat.parquet", { columns: ["n"] }),
    );
    expect(error.cause).toBeUndefined();
  });
});

describe("createParquetStore: head", () => {
  it("answers what the file holds without downloading it", async () => {
    const { s3, store } = storeOf();
    const etag = s3.seed("cat.parquet", plain);

    const head = await store.head("cat.parquet");

    expect(head.size).toBe(plain.length);
    expect(head.etag).toBe(etag);
    expect(head.rowCount).toBe(ROWS);
    expect(head.groupCount).toBe(ROWS / GROUP);
    expect(head.schema.columns.map((column) => column.name)).toEqual(
      catalogue.columns.map((column) => column.name),
    );
  });

  it("never reads past the tail", async () => {
    const { s3, store } = storeOf();
    s3.seed("cat.parquet", plain);

    await store.head("cat.parquet");

    expect(s3.requestCount).toBe(1);
    expect(s3.bytesServed).toBeLessThanOrEqual(64 * 1024);
    expect(s3.bytesServed).toBeLessThan(plain.length / 4);
    expect(s3.ranges).toEqual([{ end: 64 * 1024 }]);
  });

  it("resolves annotated columns with the types it is given", async () => {
    const s3 = new FakeS3();
    s3.seed("cat.parquet", plain);
    const bare = createParquetStore(s3, { bucket: "b" });

    await expectRejection("ERR_READ_UNSUPPORTED", bare.head("cat.parquet"));
    const head = await bare.head("cat.parquet", { types: [id, money] });
    expect(head.schema.columns[0].type).toBe(id);
  });

  it("refuses bytes that are not a Parquet file", async () => {
    const { s3, store } = storeOf();
    s3.seed("junk.parquet", new Uint8Array(64).fill(7));

    await expectRejection("ERR_READ_MALFORMED", store.head("junk.parquet"));
  });

  it("refuses an object too short to be one", async () => {
    const { s3, store } = storeOf();
    s3.seed("tiny.parquet", new Uint8Array(3));

    await expectRejection("ERR_READ_MALFORMED", store.head("tiny.parquet"));
  });
});

describe("createParquetStore: files a ranged read cannot take apart", () => {
  /**
   * A one column, one group file with the chunk metadata under the test's
   * control, padded so that it is larger than the tail below and therefore
   * genuinely read in pieces.
   */
  function handBuilt(chunk: Partial<ColumnChunkMeta> = {}): Uint8Array {
    const out = startFile();
    out.raw(new Uint8Array(2048));
    const values = [1n, 2n, 3n];
    const dataPageOffset = out.length;
    const page = writeDataPage(out, values);
    const group: RowGroupMeta = {
      columns: [
        {
          name: "n",
          physical: "i64",
          optional: false,
          codec: CompressionCodec.UNCOMPRESSED,
          numValues: values.length,
          nullCount: 0,
          dataPageOffset,
          totalUncompressedSize: page.uncompressedSize,
          totalCompressedSize: page.compressedSize,
          ...chunk,
        },
      ],
      numRows: values.length,
      totalByteSize: page.uncompressedSize,
      totalCompressedSize: page.compressedSize,
      fileOffset: dataPageOffset,
    };
    const footer = encodeFileMetadata(
      [snapshotColumn({ name: "n", type: "i64", optional: false })],
      [group],
      values.length,
      "probe",
    );
    return sealFile(out, footer);
  }

  const tight = (s3: FakeS3): ParquetStore =>
    createParquetStore(s3, { bucket: "b", tailBytes: 512 });

  it("reads a chunk whose total_compressed_size the file states properly", async () => {
    const s3 = new FakeS3();
    s3.seed("hand.parquet", handBuilt());
    const file = await tight(s3).get("hand.parquet", { columns: ["n"] });
    expect(file.rows).toEqual([{ n: 1n }, { n: 2n }, { n: 3n }]);
  });

  it("refuses a negative required total_compressed_size in every read path", async () => {
    const s3 = new FakeS3();
    const bytes = handBuilt({ totalCompressedSize: -1 });
    const local = expectError("ERR_READ_MALFORMED", () => readParquet(bytes));
    expect(local.message).toContain("total_compressed_size");
    expect(local.column).toBe("n");

    s3.seed("hand.parquet", bytes);
    const error = await expectRejection(
      "ERR_READ_MALFORMED",
      tight(s3).get("hand.parquet", { columns: ["n"] }),
    );
    expect(error.message).toContain("total_compressed_size");
    expect(error.column).toBe("n");
  });

  it("refuses a chunk of no bytes at all", async () => {
    const s3 = new FakeS3();
    s3.seed("hand.parquet", handBuilt({ totalCompressedSize: 0 }));
    await expectRejection("ERR_READ_MALFORMED", tight(s3).get("hand.parquet", { columns: ["n"] }));
  });

  it("refuses a chunk that does not lie inside the file's pages", async () => {
    const s3 = new FakeS3();
    s3.seed("before.parquet", handBuilt({ dataPageOffset: 0 }));
    s3.seed("after.parquet", handBuilt({ totalCompressedSize: 10_000_000 }));

    for (const key of ["before.parquet", "after.parquet"]) {
      const error = await expectRejection(
        "ERR_READ_MALFORMED",
        tight(s3).get(key, { columns: ["n"] }),
      );
      expect(error.message).toContain("which is not inside");
    }
  });

  it("refuses a footer longer than the object it sits in", async () => {
    const s3 = new FakeS3();
    const bytes = handBuilt();
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    view.setUint32(bytes.length - 8, 0xff_ff_ff, true);
    s3.seed("liar.parquet", bytes);

    const error = await expectRejection(
      "ERR_READ_MALFORMED",
      tight(s3).get("liar.parquet", { columns: ["n"] }),
    );
    expect(error.message).toContain("does not fit");
  });

  it("refuses an object whose tail is not a Parquet envelope", async () => {
    const s3 = new FakeS3();
    s3.seed("junk.parquet", new Uint8Array(4096).fill(7));

    await expectRejection("ERR_READ_MALFORMED", tight(s3).head("junk.parquet"));
  });

  it("refuses a suffix response that does not cover the requested tail", async () => {
    const s3 = new FakeS3();
    s3.quirks.truncateTail = true;
    s3.seed("hand.parquet", handBuilt());

    const error = await expectRejection(
      "ERR_STORE_RANGE_UNSATISFIED",
      tight(s3).get("hand.parquet", { columns: ["n"] }),
    );
    expect(error.message).toContain("file's tail");
  });

  it("refuses a footer of no bytes at all", async () => {
    const s3 = new FakeS3();
    const bytes = handBuilt();
    new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).setUint32(
      bytes.length - 8,
      0,
      true,
    );
    s3.seed("empty.parquet", bytes);

    const error = await expectRejection(
      "ERR_READ_MALFORMED",
      tight(s3).get("empty.parquet", { columns: ["n"] }),
    );
    expect(error.message).toContain("0 bytes of metadata");
  });

  it("reads on where a store has no entity tags to pin the object with", async () => {
    const s3 = new FakeS3();
    s3.quirks.omitEtag = true;
    s3.seed("hand.parquet", handBuilt());
    const store = tight(s3);

    const file = await store.get("hand.parquet", { columns: ["n"] });
    expect(file.rows).toHaveLength(3);
    // Nothing to be conditional about, so nothing was made conditional.
    expect(s3.requests.every((item) => item.ifMatch === undefined)).toBe(true);
    expect((await store.head("hand.parquet")).etag).toBeUndefined();
  });

  it("refuses every malformed Content-Range", async () => {
    for (const contentRange of ["bytes nonsense", "bytes 0-1/99999999999999999999"]) {
      const s3 = new FakeS3();
      s3.quirks.contentRange = contentRange;
      s3.seed("hand.parquet", handBuilt());

      await expectRejection(
        "ERR_STORE_RANGE_UNSATISFIED",
        tight(s3).get("hand.parquet", { columns: ["n"] }),
      );
    }
  });
});

describe("createParquetStore: del and list", () => {
  it("deletes through the client", async () => {
    const { s3, store } = storeOf();
    s3.seed("cat.parquet", plain);

    const response = await store.del("cat.parquet");

    expect(response.status).toBe(204);
    expect(s3.stored("cat.parquet")).toBeUndefined();
  });

  it("lists what the client lists", async () => {
    const { s3, store } = storeOf();
    s3.seed("events/a.parquet", single);
    s3.seed("events/b.parquet", single);
    s3.seed("other.parquet", single);

    const all = await store.list();
    expect(all.contents.map((object) => object.key)).toEqual([
      "events/a.parquet",
      "events/b.parquet",
      "other.parquet",
    ]);
    expect(all.contents[0].size).toBe(single.length);
    expect(all.isTruncated).toBe(false);

    const prefixed = await store.list({ prefix: "events/" });
    expect(prefixed.contents).toHaveLength(2);

    const grouped = await store.list({ delimiter: "/" });
    expect(grouped.commonPrefixes).toEqual(["events/"]);

    const page = await store.list({ maxKeys: 2 });
    expect(page.isTruncated).toBe(true);
    expect(page.nextContinuationToken).toBe("2");
  });
});

describe("createParquetStore: options and errors", () => {
  it("refuses a group selection that cannot be one", async () => {
    const { s3, store } = storeOf();
    s3.seed("cat.parquet", plain);

    for (const groups of [[], [-1], [1.5], [0, 0], ["0"], 3]) {
      await expectRejection(
        "ERR_READ_OPTION_INVALID",
        store.get("cat.parquet", { groups: groups as number[] }),
      );
    }
    // Not one request went out for any of them.
    expect(s3.requestCount).toBe(0);
  });

  it("refuses a group the file does not have", async () => {
    const { s3, store } = storeOf();
    s3.seed("cat.parquet", plain);

    const error = await expectRejection(
      "ERR_READ_OPTION_INVALID",
      store.get("cat.parquet", { groups: [4] }),
    );
    expect(error.message).toContain("it holds 4");
  });

  it("refuses a column the file does not declare, as a local read does", async () => {
    const { s3, store } = storeOf();
    s3.seed("cat.parquet", plain);

    await expectRejection(
      "ERR_READ_OPTION_INVALID",
      store.get("cat.parquet", { columns: ["nope"] }),
    );
  });

  it("lets the client's own errors through untouched", async () => {
    const { store } = storeOf();

    await expect(store.get("missing.parquet")).rejects.toBeInstanceOf(FakeS3Error);
    await expect(store.get("missing.parquet", { columns: ["n"] })).rejects.toBeInstanceOf(
      FakeS3Error,
    );
    await expect(store.head("missing.parquet")).rejects.toBeInstanceOf(FakeS3Error);
  });

  it("lets through a failure that carries no status at all", async () => {
    const offline = new TypeError("fetch failed");
    const s3 = new FakeS3();
    s3.seed("cat.parquet", plain);
    const store = createParquetStore(
      racing(s3, () => {
        throw offline;
      }),
      { bucket: "b" },
    );

    await expect(store.get("cat.parquet", { columns: ["n"] })).rejects.toBe(offline);
  });

  it("takes read options per call over the factory's", async () => {
    const s3 = new FakeS3();
    s3.seed("gz.parquet", compressed);
    const store = createParquetStore(s3, { bucket: "b", types: [id, money] });

    await expectRejection("ERR_READ_UNSUPPORTED", store.get("gz.parquet", { groups: [0] }));
    const file = await store.get("gz.parquet", { groups: [0], codecs });
    expect(file.rows).toEqual(await localRows(compressed, { ...read, codecs }, [0]));
  });

  it("passes request parameters through to the client", async () => {
    const s3 = new FakeS3();
    s3.seed("cat.parquet", plain, "elsewhere");
    const store = createParquetStore(s3, { bucket: "b", types: [id, money] });

    const file: ParquetFile = await store.get("cat.parquet", {
      bucket: "elsewhere",
      columns: ["n"],
      groups: [0],
      signal: AbortSignal.timeout(5000),
    });

    expect(file.rows).toHaveLength(GROUP);
  });
});
