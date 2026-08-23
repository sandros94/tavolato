import { gunzipSync, gzipSync, zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { describe, expect, it } from "vitest";
import { createWriter, defineSchema, readParquet, readSchema } from "../src/index.ts";
import type {
  ParquetSchema,
  ReadOptions,
  ReadRow,
  Row,
  SchemaDefinition,
  WriterCodec,
} from "../src/index.ts";
import { sync } from "./_sync.ts";

/**
 * The closed loop: every fixture the DuckDB suite hands to DuckDB is also
 * handed straight back to tavolato's own reader, and must come back identical.
 *
 * DuckDB proves the files are real Parquet; this file proves the reader is the
 * writer's exact inverse. Both halves are needed — agreeing with itself is not
 * a specification.
 *
 * The whole suite runs three times: once uncompressed, once through GZIP and
 * once through ZSTD, both taken straight from `node:zlib`. Compression is a
 * byte transform around a page body and must therefore change nothing at all
 * about what comes back — running the same assertions over it is the only
 * honest way to say so. `sync` doubles as the assertion that a synchronous
 * codec keeps the whole path synchronous.
 */

interface Leg {
  readonly label: string;
  readonly codec?: WriterCodec;
}

/*
 * `compress` takes the runtime's function as it is; `decompress` cannot,
 * because tavolato passes the page's uncompressed size where `node:zlib` takes
 * an options object. The one-line wrapper is deliberate: it keeps a number out
 * of somebody else's options slot.
 */
const LEGS: readonly Leg[] = [
  { label: "uncompressed" },
  {
    label: "GZIP",
    codec: { name: "GZIP", compress: gzipSync, decompress: (page) => gunzipSync(page) },
  },
  {
    label: "ZSTD",
    codec: {
      name: "ZSTD",
      compress: zstdCompressSync,
      decompress: (page) => zstdDecompressSync(page),
    },
  },
];

describe.each(LEGS)("$label", ({ codec }) => {
  const read: ReadOptions =
    codec === undefined
      ? {}
      : {
          codecs: {
            [codec.name]: {
              decompress: (page: Uint8Array, size: number) => codec.decompress!(page, size),
            },
          },
        };

  /** Writes `rows` with `schema`, reads the bytes back, returns both halves. */
  function roundtrip<TDefinition extends SchemaDefinition>(
    schema: ParquetSchema<TDefinition>,
    rows: Iterable<Row<TDefinition>>,
    rowGroupSize?: number,
  ): { schema: ParquetSchema; rows: ReadRow[] } {
    const writer = createWriter(schema, {
      ...(rowGroupSize === undefined ? {} : { rowGroupSize }),
      codec,
    });
    sync(writer.appendAll(rows));
    return sync(readParquet(sync(writer.finish()), read));
  }

  describe("schema", () => {
    it("recovers names, types and nullability in declaration order", () => {
      const schema = defineSchema({
        s: { type: "string" },
        so: { type: "string", optional: true },
        f: { type: "f64" },
        fo: { type: "f64", optional: true },
        i: { type: "i64" },
        io: { type: "i64", optional: true },
        b: { type: "bool" },
        bo: { type: "bool", optional: true },
        t: { type: "timestamp" },
        to: { type: "timestamp", optional: true },
      });
      const read = roundtrip(schema, [{ s: "x", f: 1, i: 1n, b: true, t: 0 }]).schema;
      expect(read.columns).toEqual(schema.columns);
      expect(read.definition).toEqual({
        s: { type: "string", optional: false },
        so: { type: "string", optional: true },
        f: { type: "f64", optional: false },
        fo: { type: "f64", optional: true },
        i: { type: "i64", optional: false },
        io: { type: "i64", optional: true },
        b: { type: "bool", optional: false },
        bo: { type: "bool", optional: true },
        t: { type: "timestamp", optional: false },
        to: { type: "timestamp", optional: true },
      });
    });

    it("recovers non-ASCII column names", () => {
      const schema = defineSchema({ "città": { type: "string" }, "n°": { type: "i64" } });
      const { schema: read, rows } = roundtrip(schema, [{ "città": "Roma", "n°": 1n }]);
      expect(read.columns.map((column) => column.name)).toEqual(["città", "n°"]);
      expect(rows).toEqual([{ "città": "Roma", "n°": 1n }]);
    });

    it("readSchema sees the same schema without touching the pages", () => {
      const schema = defineSchema({ a: { type: "bool", optional: true }, b: { type: "f64" } });
      const writer = createWriter(schema, { codec });
      sync(writer.append({ a: null, b: 1.5 }));
      const bytes = sync(writer.finish());
      // readSchema takes no options and never reaches a page, so it reads a
      // compressed file exactly as it reads an uncompressed one.
      expect(readSchema(bytes)).toEqual(sync(readParquet(bytes, read)).schema);
    });

    it("recovers a json column and hands it straight back to createWriter", () => {
      const declared = defineSchema({
        k: { type: "i64" },
        doc: { type: "json" },
        maybe: { type: "json", optional: true },
      });
      const { schema: recovered } = roundtrip(declared, [{ k: 0n, doc: "{}" }]);
      expect(recovered.columns).toEqual(declared.columns);
      expect(recovered.definition).toEqual({
        k: { type: "i64", optional: false },
        doc: { type: "json", optional: false },
        maybe: { type: "json", optional: true },
      });

      // The schema a file yields is valid input again: same columns, same file.
      const again = createWriter(recovered, { codec });
      sync(again.append({ k: 1n, doc: '{"round":"trip"}' }));
      const reread = sync(readParquet(sync(again.finish()), read));
      expect(reread.schema.columns).toEqual(declared.columns);
      expect(reread.rows).toEqual([{ k: 1n, doc: '{"round":"trip"}', maybe: null }]);
    });

    it("returns a frozen schema, like defineSchema does", () => {
      const { schema } = roundtrip(defineSchema({ n: { type: "i64" } }), [{ n: 1n }]);
      expect(Object.isFrozen(schema)).toBe(true);
      expect(Object.isFrozen(schema.columns)).toBe(true);
      expect(Object.isFrozen(schema.columns[0])).toBe(true);
    });
  });

  describe("values", () => {
    it("round-trips every supported column type", () => {
      const schema = defineSchema({
        s: { type: "string" },
        f: { type: "f64" },
        i: { type: "i64" },
        b: { type: "bool" },
        t: { type: "timestamp" },
      });
      const { rows } = roundtrip(schema, [
        { s: "alpha", f: 1.5, i: 42n, b: true, t: 1_700_000_000_000 },
        { s: "beta", f: -0.25, i: -7, b: false, t: new Date(1_700_000_001_500) },
      ]);
      expect(rows).toEqual([
        { s: "alpha", f: 1.5, i: 42n, b: true, t: new Date(1_700_000_000_000) },
        { s: "beta", f: -0.25, i: -7n, b: false, t: new Date(1_700_000_001_500) },
      ]);
    });

    it("round-trips nulls in every column", () => {
      const schema = defineSchema({
        s: { type: "string", optional: true },
        f: { type: "f64", optional: true },
        i: { type: "i64", optional: true },
        b: { type: "bool", optional: true },
        t: { type: "timestamp", optional: true },
        k: { type: "i64" },
      });
      const { rows } = roundtrip(schema, [
        { s: "x", f: 1, i: 1n, b: true, t: 0, k: 0n },
        { s: null, f: null, i: null, b: null, t: null, k: 1n },
        { k: 2n }, // every optional column omitted entirely
        { s: "y", f: 2, i: 2n, b: false, t: 1000, k: 3n },
      ]);
      // An omitted optional column reads back as an explicit null, not a missing key.
      expect(rows).toEqual([
        { s: "x", f: 1, i: 1n, b: true, t: new Date(0), k: 0n },
        { s: null, f: null, i: null, b: null, t: null, k: 1n },
        { s: null, f: null, i: null, b: null, t: null, k: 2n },
        { s: "y", f: 2, i: 2n, b: false, t: new Date(1000), k: 3n },
      ]);
      expect(Object.keys(rows[1])).toEqual(["s", "f", "i", "b", "t", "k"]);
    });

    it("round-trips an all-null column", () => {
      const schema = defineSchema({
        empty: { type: "string", optional: true },
        k: { type: "i64" },
      });
      const { rows } = roundtrip(
        schema,
        Array.from({ length: 100 }, (_, index) => ({ empty: null, k: BigInt(index) })),
      );
      expect(rows).toHaveLength(100);
      expect(rows.every((row) => row.empty === null)).toBe(true);
      expect(rows.at(-1)).toEqual({ empty: null, k: 99n });
    });

    it("round-trips empty strings and non-ASCII UTF-8", () => {
      const values = [
        "",
        "ascii",
        "é",
        "日本語テキスト",
        "🎉🚀",
        "é", // combining acute accent
        "line\nbreak\ttab",
        "quote'\"back\\slash",
        "x".repeat(5000),
      ];
      const schema = defineSchema({ k: { type: "i64" }, s: { type: "string" } });
      const { rows } = roundtrip(
        schema,
        values.map((value, index) => ({ k: BigInt(index), s: value })),
      );
      expect(rows.map((row) => row.s)).toEqual(values);
    });

    it("round-trips json documents as the exact strings they were given", () => {
      // The last two are the point of the type: tavolato never parses and never
      // re-serializes, so whitespace and key order come back untouched. A
      // library that round-tripped through `JSON.parse` would fail both.
      const documents = [
        "{}",
        "[]",
        "null",
        "42",
        '"a bare string"',
        '{"nested":{"deep":[1,2,3]}}',
        '{"unicode":"日本語","emoji":"🎉"}',
        '  {\n    "spaced": true\n  }  ',
        '{"z":1,"a":2}',
      ];
      const schema = defineSchema({
        k: { type: "i64" },
        doc: { type: "json" },
        maybe: { type: "json", optional: true },
      });
      const { schema: recovered, rows } = roundtrip(
        schema,
        documents.map((doc, index) => ({
          k: BigInt(index),
          doc,
          maybe: index % 2 === 0 ? null : doc,
        })),
      );
      expect(recovered.columns.map((column) => column.type)).toEqual(["i64", "json", "json"]);
      expect(rows.map((row) => row.doc)).toEqual(documents);
      expect(rows.map((row) => row.maybe)).toEqual(
        documents.map((doc, index) => (index % 2 === 0 ? null : doc)),
      );
    });

    it("round-trips the signed 64-bit extremes as bigints", () => {
      const extremes = [
        -(2n ** 63n),
        -(2n ** 63n) + 1n,
        -1n,
        0n,
        1n,
        2n ** 63n - 2n,
        2n ** 63n - 1n,
      ];
      const schema = defineSchema({ k: { type: "i64" }, n: { type: "i64", optional: true } });
      const { rows } = roundtrip(schema, [
        ...extremes.map((value, index) => ({ k: BigInt(index), n: value })),
        { k: BigInt(extremes.length), n: null },
      ]);
      expect(rows.map((row) => row.n)).toEqual([...extremes, null]);
    });

    it("returns bigints even where the writer accepted safe integer numbers", () => {
      const schema = defineSchema({ n: { type: "i64" } });
      const { rows } = roundtrip(schema, [
        { n: Number.MAX_SAFE_INTEGER },
        { n: -Number.MAX_SAFE_INTEGER },
        { n: 0 },
      ]);
      // Consistency over convenience: i64 is always a bigint on the way out, even
      // for values a number could have held exactly.
      expect(rows).toEqual([
        { n: BigInt(Number.MAX_SAFE_INTEGER) },
        { n: -BigInt(Number.MAX_SAFE_INTEGER) },
        { n: 0n },
      ]);
      expect(rows.every((row) => typeof row.n === "bigint")).toBe(true);
    });

    it("returns Dates even where the writer accepted epoch milliseconds", () => {
      const schema = defineSchema({ t: { type: "timestamp" } });
      const { rows } = roundtrip(schema, [
        { t: 1_700_000_000_000 },
        { t: new Date(1_700_000_000_001) },
        { t: 0 },
        { t: -1 },
      ]);
      expect(rows).toEqual([
        { t: new Date(1_700_000_000_000) },
        { t: new Date(1_700_000_000_001) },
        { t: new Date(0) },
        { t: new Date(-1) },
      ]);
      expect(rows.every((row) => row.t instanceof Date)).toBe(true);
    });

    it("round-trips double specials, negative zero included", () => {
      const specials = [0, 1e308, 5e-324, Number.MAX_VALUE, Number.MIN_VALUE, Number.NaN];
      const schema = defineSchema({ k: { type: "i64" }, f: { type: "f64" } });
      const { rows } = roundtrip(schema, [
        ...specials.map((value, index) => ({ k: BigInt(index), f: value })),
        { k: 100n, f: -0 },
        { k: 101n, f: Number.POSITIVE_INFINITY },
        { k: 102n, f: Number.NEGATIVE_INFINITY },
      ]);
      expect(rows.slice(0, specials.length).map((row) => row.f)).toEqual(specials);
      // `toEqual` cannot tell -0 from 0, so the sign bit is checked directly.
      expect(Object.is(rows[specials.length].f, -0)).toBe(true);
      expect(rows.at(-2)?.f).toBe(Number.POSITIVE_INFINITY);
      expect(rows.at(-1)?.f).toBe(Number.NEGATIVE_INFINITY);
    });

    it("round-trips booleans across byte and level-run boundaries", () => {
      const schema = defineSchema({
        k: { type: "i64" },
        required: { type: "bool" },
        nullable: { type: "bool", optional: true },
      });
      const expected = Array.from({ length: 1003 }, (_, index) => ({
        k: BigInt(index),
        required: index % 3 === 0,
        nullable: index % 5 === 0 ? null : index % 2 === 0,
      }));
      expect(roundtrip(schema, expected).rows).toEqual(expected);
    });
  });

  describe("file structure", () => {
    it("reads a file with zero rows back as an empty row list", () => {
      const schema = defineSchema({
        s: { type: "string" },
        n: { type: "i64", optional: true },
      });
      const bytes = sync(createWriter(schema, { codec }).finish());
      const { schema: columns, rows } = sync(readParquet(bytes, read));
      expect(rows).toEqual([]);
      expect(columns.columns).toEqual(schema.columns);
    });

    it("reassembles rows across row group boundaries, in order", () => {
      const schema = defineSchema({ n: { type: "i64" }, s: { type: "string", optional: true } });
      // 30 rows at 7 per group: four full groups plus a two row remainder.
      const expected = Array.from({ length: 30 }, (_, index) => ({
        n: BigInt(index),
        s: index % 4 === 0 ? null : `row-${index}`,
      }));
      expect(roundtrip(schema, expected, 7).rows).toEqual(expected);
    });

    it("round-trips one row group per row", () => {
      const schema = defineSchema({ n: { type: "i64" } });
      const expected = Array.from({ length: 20 }, (_, index) => ({ n: BigInt(index) }));
      expect(roundtrip(schema, expected, 1).rows).toEqual(expected);
    });

    it("round-trips 100k rows across ten row groups", () => {
      const schema = defineSchema({
        id: { type: "i64" },
        name: { type: "string", optional: true },
        value: { type: "f64" },
        flag: { type: "bool", optional: true },
        ts: { type: "timestamp" },
      });
      const count = 100_000;
      const base = 1_700_000_000_000;
      const expected = Array.from({ length: count }, (_, id) => ({
        id: BigInt(id),
        name: id % 7 === 0 ? null : `host-${id}`,
        value: id + 0.5,
        flag: id % 11 === 0 ? null : id % 2 === 0,
        ts: new Date(base + id * 1000),
      }));
      // A generator, so `appendAll` pulls the rows one at a time rather than
      // materialising a second 100k array.
      function* asRows() {
        for (const row of expected) yield { ...row, ts: row.ts.getTime() };
      }
      const writer = createWriter(schema, { codec }); // default rowGroupSize of 10_000
      sync(writer.appendAll(asRows()));
      const { rows } = sync(readParquet(sync(writer.finish()), read));

      expect(rows).toHaveLength(count);
      // Compared field by field rather than with one deep-equal over 100k objects:
      // a mismatch reports the row it happened on instead of a wall of diff.
      let mismatches = 0;
      for (const [index, row] of rows.entries()) {
        const want = expected[index];
        if (
          row.id !== want.id ||
          row.name !== want.name ||
          row.value !== want.value ||
          row.flag !== want.flag ||
          (row.ts as Date).getTime() !== want.ts.getTime()
        ) {
          mismatches++;
        }
      }
      expect(mismatches).toBe(0);
      expect(rows[0]).toEqual(expected[0]);
      expect(rows.at(-1)).toEqual(expected.at(-1));
    });
  });
});
