import { gunzipSync, gzipSync, zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { describe, expect, it } from "vitest";
import {
  createWriter,
  date,
  decimal,
  defineSchema,
  float16,
  integer,
  readParquet,
  readSchema,
  time,
  timestamp,
  uuid,
} from "../src/index.ts";
import type {
  AnyLogicalAdapter,
  JsonValue,
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
    options: { rowGroupSize?: number; types?: readonly AnyLogicalAdapter[] } = {},
  ): { schema: ParquetSchema; rows: ReadRow[] } {
    const { rowGroupSize, types } = options;
    const writer = createWriter(schema, {
      ...(rowGroupSize === undefined ? {} : { rowGroupSize }),
      codec,
    });
    sync(writer.appendAll(rows));
    return sync(
      readParquet(sync(writer.finish()), { ...read, ...(types === undefined ? {} : { types }) }),
    );
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
      const { schema: recovered } = roundtrip(declared, [{ k: 0n, doc: {} }]);
      expect(recovered.columns).toEqual(declared.columns);
      expect(recovered.definition).toEqual({
        k: { type: "i64", optional: false },
        doc: { type: "json", optional: false },
        maybe: { type: "json", optional: true },
      });

      // The schema a file yields is valid input again: same columns, same file.
      const again = createWriter(recovered, { codec });
      sync(again.append({ k: 1n, doc: { round: "trip" } }));
      const reread = sync(readParquet(sync(again.finish()), read));
      expect(reread.schema.columns).toEqual(declared.columns);
      expect(reread.rows).toEqual([{ k: 1n, doc: { round: "trip" }, maybe: null }]);
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
        g: { type: "f32" },
        i: { type: "i64" },
        n: { type: "i32" },
        b: { type: "bool" },
        t: { type: "timestamp" },
      });
      const { rows } = roundtrip(schema, [
        { s: "alpha", f: 1.5, g: 0.5, i: 42n, n: 42, b: true, t: 1_700_000_000_000 },
        { s: "beta", f: -0.25, g: -0.25, i: -7, n: -7, b: false, t: new Date(1_700_000_001_500) },
      ]);
      expect(rows).toEqual([
        { s: "alpha", f: 1.5, g: 0.5, i: 42n, n: 42, b: true, t: new Date(1_700_000_000_000) },
        {
          s: "beta",
          f: -0.25,
          g: -0.25,
          i: -7n,
          n: -7,
          b: false,
          t: new Date(1_700_000_001_500),
        },
      ]);
    });

    it("round-trips the signed 32-bit extremes as numbers", () => {
      const schema = defineSchema({ k: { type: "i64" }, n: { type: "i32", optional: true } });
      const extremes = [-(2 ** 31), -(2 ** 31) + 1, -1, 0, 1, 2 ** 31 - 2, 2 ** 31 - 1];
      const { rows } = roundtrip(schema, [
        ...extremes.map((value, index) => ({ k: BigInt(index), n: value })),
        { k: BigInt(extremes.length), n: null },
      ]);
      expect(rows.map((row) => row.n)).toEqual([...extremes, null]);
      expect(rows.every((row) => row.n === null || typeof row.n === "number")).toBe(true);
    });

    it("round-trips single specials, and rounds every value exactly once", () => {
      // 0.1 is not a single, so it moves on the way in. What matters is that it
      // then stops moving: reading gives the stored value, and writing that
      // value again reproduces it.
      const schema = defineSchema({ k: { type: "i64" }, g: { type: "f32" } });
      const values = [0, 1, -1, 0.5, 0.1, 3.402_823_466_385_288_6e38, 1.175_494_35e-38];
      const { rows } = roundtrip(schema, [
        ...values.map((value, index) => ({ k: BigInt(index), g: value })),
        { k: 100n, g: -0 },
        { k: 101n, g: Number.NaN },
        { k: 102n, g: Number.POSITIVE_INFINITY },
        { k: 103n, g: Number.NEGATIVE_INFINITY },
      ]);
      const stored = rows.slice(0, values.length).map((row) => row.g as number);
      expect(stored[4]).not.toBe(0.1); // rounded to single, once
      expect(stored[4]).toBeCloseTo(0.1, 7);
      expect(Object.is(rows[values.length].g, -0)).toBe(true);
      expect(Number.isNaN(rows[values.length + 1].g as number)).toBe(true);
      expect(rows.at(-2)?.g).toBe(Number.POSITIVE_INFINITY);
      expect(rows.at(-1)?.g).toBe(Number.NEGATIVE_INFINITY);

      // Re-writing what came back must reproduce it exactly.
      const again = roundtrip(
        schema,
        stored.map((value, index) => ({ k: BigInt(index), g: value })),
      );
      expect(again.rows.map((row) => row.g)).toEqual(stored);
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

    it("round-trips json documents as the structures they were given", () => {
      // Every shape JSON has, including the scalars a document is allowed to be
      // all by itself. What comes back is deep-equal to what went in — the same
      // guarantee the other column types make, over a value with an inside.
      const documents: JsonValue[] = [
        {},
        [],
        42,
        0,
        -1.5,
        true,
        false,
        "a bare string",
        "",
        { nested: { deep: [1, 2, 3] } },
        { unicode: "日本語", emoji: "🎉" },
        { mixed: [1, "two", true, null, { three: 3 }, []] },
        { "key with spaces": 1, "ключ": 2, "": 3 },
        [[[[1]]]],
        { inner: null },
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

    it("round-trips a json document through a second write unchanged", () => {
      // What the reader hands back is legal input again, and writing it a
      // second time produces the same document — which is what makes the
      // structure, rather than the text, the value of the column.
      const schema = defineSchema({ k: { type: "i64" }, doc: { type: "json" } });
      const document = { b: [1, { c: "d" }], a: null };
      const first = roundtrip(schema, [{ k: 0n, doc: document }]);
      const second = roundtrip(schema, [{ k: 0n, doc: first.rows[0].doc as JsonValue }]);
      expect(second.rows).toEqual(first.rows);
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

  /*
   * Logical column types are pure value transforms, and a codec is a byte
   * transform around the page they land in — so running the whole set through
   * every leg is what says the two seams do not know about each other.
   */
  describe("logical column types", () => {
    const money = decimal({ precision: 9, scale: 2 });
    const big = decimal({ precision: 18, scale: 4 });
    const huge = decimal({ precision: 38, scale: 6 });
    const id = uuid();
    const day = date();
    const clock = time({ unit: "millis" });
    const precise = time({ unit: "micros" });
    const nanos = time({ unit: "nanos" });
    const instant = timestamp({ unit: "micros" });
    const half = float16();
    const tiny = integer({ bitWidth: 8 });
    const short = integer({ bitWidth: 16 });
    const wide = integer({ bitWidth: 32, signed: false });
    const huge64 = integer({ bitWidth: 64, signed: false });
    const types = [
      money,
      big,
      huge,
      id,
      day,
      clock,
      precise,
      nanos,
      instant,
      half,
      tiny,
      short,
      wide,
      huge64,
    ];

    const schema = defineSchema({
      money: { type: money },
      big: { type: big },
      huge: { type: huge },
      id: { type: id },
      day: { type: day },
      clock: { type: clock },
      precise: { type: precise },
      nanos: { type: nanos },
      instant: { type: instant },
      half: { type: half },
      tiny: { type: tiny },
      short: { type: short },
      wide: { type: wide },
      huge64: { type: huge64 },
      maybe: { type: money, optional: true },
    });

    const rows = [
      {
        money: "1234567.89",
        big: "-12345678901234.5678",
        huge: "12345678901234567890123456789012.345678",
        id: "b3f2c1a0-1111-4222-8333-444455556666",
        day: new Date(Date.UTC(2026, 7, 24)),
        clock: 45_296_789,
        precise: 45_296_789_012n,
        nanos: 45_296_789_012_345n,
        instant: 1_767_225_845_123_456n,
        half: 1.5,
        tiny: -128,
        short: 32_767,
        wide: 4_294_967_295,
        huge64: 18_446_744_073_709_551_615n,
        maybe: "0.01",
      },
      {
        money: "-0.01",
        big: "0.0000",
        huge: "-0.000001",
        id: "00000000-0000-0000-0000-000000000000",
        day: new Date(0),
        clock: 0,
        precise: 0n,
        nanos: 0n,
        instant: -1n,
        half: -65_504,
        tiny: 127,
        short: -32_768,
        wide: 0,
        huge64: 0n,
        maybe: null,
      },
    ];

    it("hands every value back exactly as it was written", () => {
      const read = roundtrip(schema, rows, { types });
      expect(read.rows).toEqual(rows);
    });

    it("recovers the column types themselves, and writes the same file again", () => {
      const recovered = roundtrip(schema, rows, { types });
      expect(recovered.schema.columns).toEqual(schema.columns);
      // The claiming adapter *is* the column's type, so a recovered schema is
      // valid `createWriter` input with no registry to rebuild.
      expect(recovered.schema.columns[0].type).toBe(money);
      expect(recovered.schema.definition.id).toEqual({ type: id, optional: false });

      const again = createWriter(recovered.schema, { codec });
      sync(again.appendAll(recovered.rows as never[]));
      const reread = sync(readParquet(sync(again.finish()), { ...read, types }));
      expect(reread.rows).toEqual(rows);
    });

    it("keeps a half-precision value exactly, once it has been rounded", () => {
      const halfSchema = defineSchema({ k: { type: "i64" }, h: { type: half } });
      const values = [0, -0, 1, -2, 0.5, 6.103_515_625e-5, 5.960_464_477_539_063e-8, 65_504];
      const first = roundtrip(
        halfSchema,
        values.map((value, index) => ({ k: BigInt(index), h: value })),
        { types },
      ).rows.map((row) => row.h as number);
      expect(first).toEqual(values);
      expect(Object.is(first[1], -0)).toBe(true);

      // A value that is not a half moves once, and then never again.
      const rounded = roundtrip(halfSchema, [{ k: 0n, h: 0.1 }], { types }).rows[0].h as number;
      expect(rounded).toBe(0.099_975_585_937_5);
      expect(roundtrip(halfSchema, [{ k: 0n, h: rounded }], { types }).rows[0].h).toBe(rounded);
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
      expect(roundtrip(schema, expected, { rowGroupSize: 7 }).rows).toEqual(expected);
    });

    it("round-trips one row group per row", () => {
      const schema = defineSchema({ n: { type: "i64" } });
      const expected = Array.from({ length: 20 }, (_, index) => ({ n: BigInt(index) }));
      expect(roundtrip(schema, expected, { rowGroupSize: 1 }).rows).toEqual(expected);
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
