import { gzipSync, zstdCompressSync } from "node:zlib";
import { afterAll, describe, expect, it } from "vitest";
import {
  createWriter,
  date,
  decimal,
  defineSchema,
  float16,
  integer,
  readParquet,
  TavolatoError,
  time,
  timestamp,
  uuid,
} from "../src/index.ts";
import type { JsonValue, WriterCodec } from "../src/index.ts";
import { cleanupTempDir, duckdb, duckdbRow, sqlPath, writeParquet } from "./_duckdb.ts";
import { sync } from "./_sync.ts";

/**
 * DuckDB is the specification for this library: everything the writer emits is
 * written to a temp directory and read back with the DuckDB CLI. Large fixtures
 * are checked with aggregate queries rather than by dumping rows.
 */

afterAll(() => cleanupTempDir());

/** Writes a file and returns its SQL-quoted path. */
function emit(name: string, bytes: Uint8Array | Promise<Uint8Array>): string {
  return sqlPath(writeParquet(name, sync(bytes)));
}

describe("value round-trips", () => {
  it("round-trips every supported column type", () => {
    const schema = defineSchema({
      s: { type: "string" },
      f: { type: "f64" },
      i: { type: "i64" },
      b: { type: "bool" },
      t: { type: "timestamp" },
    });
    const writer = createWriter(schema);
    writer.append({ s: "alpha", f: 1.5, i: 42n, b: true, t: 1_700_000_000_000 });
    writer.append({ s: "beta", f: -0.25, i: -7, b: false, t: new Date(1_700_000_001_500) });
    const path = emit("types.parquet", writer.finish());

    expect(
      duckdb(`SELECT s, f, i, b, epoch_ms(t) AS t FROM read_parquet(${path}) ORDER BY i DESC;`),
    ).toEqual([
      { s: "alpha", f: 1.5, i: 42, b: true, t: 1_700_000_000_000 },
      { s: "beta", f: -0.25, i: -7, b: false, t: 1_700_000_001_500 },
    ]);
  });

  it("round-trips the narrower numeric types", () => {
    const schema = defineSchema({ n: { type: "i32" }, f: { type: "f32" } });
    const writer = createWriter(schema);
    writer.append({ n: -(2 ** 31), f: 0.5 });
    writer.append({ n: 2 ** 31 - 1, f: -0.25 });
    const path = emit("narrow.parquet", writer.finish());

    // The physical types are the narrow ones, not a widening of i64/f64.
    expect(
      duckdb(`DESCRIBE SELECT * FROM read_parquet(${path});`).map((row) => row.column_type),
    ).toEqual(["INTEGER", "FLOAT"]);
    expect(duckdb(`SELECT n, f FROM read_parquet(${path}) ORDER BY n;`)).toEqual([
      { n: -2_147_483_648, f: 0.5 },
      { n: 2_147_483_647, f: -0.25 },
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
    const writer = createWriter(schema);
    writer.append({ s: "x", f: 1, i: 1n, b: true, t: 0, k: 0n });
    writer.append({ s: null, f: null, i: null, b: null, t: null, k: 1n });
    writer.append({ k: 2n }); // every optional column omitted entirely
    writer.append({ s: "y", f: 2, i: 2n, b: false, t: 1000, k: 3n });
    const path = emit("nulls.parquet", writer.finish());

    expect(
      duckdb(`SELECT k, s, f, i, b, epoch_ms(t) AS t FROM read_parquet(${path}) ORDER BY k;`),
    ).toEqual([
      { k: 0, s: "x", f: 1, i: 1, b: true, t: 0 },
      { k: 1, s: null, f: null, i: null, b: null, t: null },
      { k: 2, s: null, f: null, i: null, b: null, t: null },
      { k: 3, s: "y", f: 2, i: 2, b: false, t: 1000 },
    ]);

    // The chunk statistics must agree with the definition levels.
    expect(
      duckdb(
        `SELECT path_in_schema AS name, stats_null_count AS nulls FROM parquet_metadata(${path}) ORDER BY name;`,
      ),
    ).toEqual([
      { name: "b", nulls: 2 },
      { name: "f", nulls: 2 },
      { name: "i", nulls: 2 },
      { name: "k", nulls: 0 },
      { name: "s", nulls: 2 },
      { name: "t", nulls: 2 },
    ]);
  });

  it("round-trips an all-null column", () => {
    const schema = defineSchema({
      empty: { type: "string", optional: true },
      k: { type: "i64" },
    });
    const writer = createWriter(schema);
    for (let index = 0; index < 100; index++) writer.append({ empty: null, k: BigInt(index) });
    const path = emit("all-null.parquet", writer.finish());

    expect(
      duckdbRow(`SELECT count(*) AS rows, count(empty) AS present FROM read_parquet(${path});`),
    ).toEqual({ rows: 100, present: 0 });
    expect(
      duckdbRow(
        `SELECT stats_null_count AS nulls FROM parquet_metadata(${path}) WHERE path_in_schema = 'empty';`,
      ),
    ).toEqual({ nulls: 100 });
  });

  it("round-trips empty strings and non-ASCII UTF-8", () => {
    const values = [
      "",
      "ascii",
      "é",
      "日本語テキスト",
      "🎉🚀",
      "é", // combining acute accent
      "line\nbreak\ttab",
      "quote'\"back\\slash",
      "x".repeat(5000),
    ];
    const schema = defineSchema({ k: { type: "i64" }, s: { type: "string" } });
    const writer = createWriter(schema);
    for (const [index, value] of values.entries()) writer.append({ k: BigInt(index), s: value });
    const path = emit("utf8.parquet", writer.finish());

    const rows = duckdb<{ s: string }>(`SELECT s FROM read_parquet(${path}) ORDER BY k;`);
    expect(rows.map((row) => row.s)).toEqual(values);
    // Byte lengths confirm the UTF-8 payload survived intact, not just the
    // decoded text. `sum` widens to HUGEINT, which the CLI prints as a string.
    const encoder = new TextEncoder();
    expect(duckdbRow(`SELECT sum(strlen(s))::BIGINT AS bytes FROM read_parquet(${path});`)).toEqual(
      { bytes: values.reduce((total, value) => total + encoder.encode(value).length, 0) },
    );
  });

  it("round-trips the signed 64-bit extremes", () => {
    // DuckDB prints BIGINT as a JSON number, which JavaScript cannot parse
    // losslessly, so the values come back as text.
    const schema = defineSchema({ k: { type: "i64" }, n: { type: "i64", optional: true } });
    const writer = createWriter(schema);
    const extremes = [-(2n ** 63n), -(2n ** 63n) + 1n, -1n, 0n, 1n, 2n ** 63n - 2n, 2n ** 63n - 1n];
    for (const [index, value] of extremes.entries()) {
      writer.append({ k: BigInt(index), n: value });
    }
    writer.append({ k: BigInt(extremes.length), n: null });
    const path = emit("i64-edges.parquet", writer.finish());

    const rows = duckdb<{ n: string | null }>(
      `SELECT n::VARCHAR AS n FROM read_parquet(${path}) ORDER BY k;`,
    );
    expect(rows.map((row) => row.n)).toEqual([...extremes.map((value) => value.toString()), null]);
  });

  it("accepts safe integer numbers for i64", () => {
    const schema = defineSchema({ n: { type: "i64" } });
    const writer = createWriter(schema);
    writer.append({ n: Number.MAX_SAFE_INTEGER });
    writer.append({ n: -Number.MAX_SAFE_INTEGER });
    const path = emit("i64-numbers.parquet", writer.finish());

    expect(
      duckdb<{ n: string }>(`SELECT n::VARCHAR AS n FROM read_parquet(${path});`).map(
        (row) => row.n,
      ),
    ).toEqual([String(Number.MAX_SAFE_INTEGER), String(-Number.MAX_SAFE_INTEGER)]);
  });

  it("round-trips double specials", () => {
    const schema = defineSchema({ k: { type: "i64" }, f: { type: "f64" } });
    const writer = createWriter(schema);
    const specials = [0, -0, 1e308, 5e-324, Number.MAX_VALUE, Number.MIN_VALUE];
    for (const [index, value] of specials.entries()) writer.append({ k: BigInt(index), f: value });
    writer.append({ k: 100n, f: Number.NaN });
    writer.append({ k: 101n, f: Number.POSITIVE_INFINITY });
    writer.append({ k: 102n, f: Number.NEGATIVE_INFINITY });
    const path = emit("f64-specials.parquet", writer.finish());

    expect(
      duckdbRow(`
      SELECT
        count(*) FILTER (WHERE k < 100 AND f = list_extract([0.0, -0.0, 1e308, 5e-324, ${Number.MAX_VALUE}, ${Number.MIN_VALUE}], k + 1)) AS matched,
        count(*) FILTER (WHERE k = 100 AND isnan(f)) AS nan,
        count(*) FILTER (WHERE k = 101 AND isinf(f) AND f > 0) AS pos_inf,
        count(*) FILTER (WHERE k = 102 AND isinf(f) AND f < 0) AS neg_inf
      FROM read_parquet(${path});
    `),
    ).toEqual({ matched: specials.length, nan: 1, pos_inf: 1, neg_inf: 1 });

    // Negative zero keeps its sign bit through the PLAIN encoding.
    expect(
      duckdbRow(
        `SELECT f = 0 AND 1 / f < 0 AS negative_zero FROM read_parquet(${path}) WHERE k = 1;`,
      ),
    ).toEqual({ negative_zero: true });
  });

  it("round-trips booleans across byte boundaries", () => {
    const schema = defineSchema({
      k: { type: "i64" },
      required: { type: "bool" },
      nullable: { type: "bool", optional: true },
    });
    const writer = createWriter(schema);
    for (let index = 0; index < 1003; index++) {
      writer.append({
        k: BigInt(index),
        required: index % 3 === 0,
        nullable: index % 5 === 0 ? null : index % 2 === 0,
      });
    }
    const path = emit("bools.parquet", writer.finish());

    expect(
      duckdbRow(`
      SELECT
        count(*) AS rows,
        count(*) FILTER (WHERE required <> (k % 3 = 0)) AS required_mismatch,
        count(*) FILTER (WHERE nullable IS DISTINCT FROM CASE WHEN k % 5 = 0 THEN NULL ELSE k % 2 = 0 END) AS nullable_mismatch
      FROM read_parquet(${path});
    `),
    ).toEqual({ rows: 1003, required_mismatch: 0, nullable_mismatch: 0 });
  });
});

describe("compression", () => {
  const codecs: readonly WriterCodec[] = [
    { name: "GZIP", compress: gzipSync },
    { name: "ZSTD", compress: zstdCompressSync },
  ];

  it.each(codecs)("hands DuckDB a readable $name file", (codec) => {
    const schema = defineSchema({
      k: { type: "i64" },
      s: { type: "string" },
      o: { type: "string", optional: true },
      f: { type: "f64" },
      b: { type: "bool", optional: true },
      t: { type: "timestamp" },
    });
    // Several row groups, so every chunk of every column goes through the hook.
    const writer = createWriter(schema, { codec, rowGroupSize: 7 });
    for (let index = 0; index < 30; index++) {
      // Synchronous codec, so every flush lands before the next row is staged.
      sync(
        writer.append({
          k: BigInt(index),
          s: `row-${index}`,
          o: index % 3 === 0 ? null : `opt-${index}`,
          f: index + 0.5,
          b: index % 5 === 0 ? null : index % 2 === 0,
          t: 1_700_000_000_000 + index,
        }),
      );
    }
    const path = emit(`compressed-${codec.name}.parquet`, writer.finish());

    expect(
      duckdbRow(`
      SELECT
        count(*) AS rows,
        sum(k)::BIGINT AS k_sum,
        count(o) AS present,
        count(*) FILTER (WHERE s <> 'row-' || k) AS s_mismatch,
        count(*) FILTER (WHERE f <> k + 0.5) AS f_mismatch,
        count(*) FILTER (WHERE b IS DISTINCT FROM CASE WHEN k % 5 = 0 THEN NULL ELSE k % 2 = 0 END) AS b_mismatch,
        count(*) FILTER (WHERE epoch_ms(t) <> 1700000000000 + k) AS t_mismatch
      FROM read_parquet(${path});
    `),
    ).toEqual({
      rows: 30,
      k_sum: (29 * 30) / 2,
      present: 20,
      s_mismatch: 0,
      f_mismatch: 0,
      b_mismatch: 0,
      t_mismatch: 0,
    });

    // The codec is stamped on every chunk, and the declared sizes differ, which
    // is what says the bodies really were compressed.
    expect(duckdb(`SELECT DISTINCT compression FROM parquet_metadata(${path});`)).toEqual([
      { compression: codec.name },
    ]);
    expect(
      duckdbRow(`
      SELECT count(*) AS shrunk FROM parquet_metadata(${path})
      WHERE total_compressed_size < total_uncompressed_size;
    `).shrunk,
    ).toBeGreaterThan(0);
  });

  it("leaves an uncompressed file saying so", () => {
    const schema = defineSchema({ n: { type: "i64" } });
    const writer = createWriter(schema);
    writer.append({ n: 1n });
    const path = emit("uncompressed.parquet", writer.finish());
    expect(duckdb(`SELECT DISTINCT compression FROM parquet_metadata(${path});`)).toEqual([
      { compression: "UNCOMPRESSED" },
    ]);
  });
});

describe("json columns", () => {
  const schema = defineSchema({
    k: { type: "i64" },
    doc: { type: "json" },
    maybe: { type: "json", optional: true },
  });

  function path(): string {
    const writer = createWriter(schema);
    for (let index = 0; index < 5; index++) {
      writer.append({
        // Structures in, and the file DuckDB then reads holds the JSON text
        // they serialize to: the value is the document, the wire format is
        // still a string in a BYTE_ARRAY annotated JSON.
        k: BigInt(index),
        doc: { id: index, tag: `t${index}`, tags: [index, index + 1] },
        maybe: index % 2 === 0 ? null : { odd: index },
      });
    }
    return emit("json.parquet", writer.finish());
  }

  it("is annotated so DuckDB gives it back as JSON, not as text", () => {
    const declared = duckdbRow(
      `SELECT type, converted_type, logical_type FROM parquet_schema(${path()}) WHERE name = 'doc';`,
    );
    expect(declared).toEqual({
      type: "BYTE_ARRAY",
      converted_type: "JSON",
      logical_type: "JsonType()",
    });
    expect(
      duckdb(`DESCRIBE SELECT doc FROM read_parquet(${path()});`).map((row) => row.column_type),
    ).toEqual(["JSON"]);
  });

  it("is queryable with DuckDB's JSON operators", () => {
    expect(
      duckdb(`
      SELECT doc->>'$.tag' AS tag, (doc->>'$.id')::BIGINT AS id, doc->>'$.tags[1]' AS second
      FROM read_parquet(${path()}) ORDER BY k;
    `),
    ).toEqual(
      Array.from({ length: 5 }, (_, index) => ({
        tag: `t${index}`,
        id: index,
        second: String(index + 1),
      })),
    );
    expect(duckdbRow(`SELECT count(maybe) AS present FROM read_parquet(${path()});`)).toEqual({
      present: 2,
    });
  });

  it("carries nested, array and unicode documents past DuckDB and back", () => {
    // The oracle in both directions over one file: DuckDB reads the documents
    // with its own JSON operators, and tavolato reads the very same bytes back
    // into the structures they were written from.
    const documents: JsonValue[] = [
      { nested: { deep: [1, 2, 3] } },
      { unicode: "日本語", emoji: "🎉", ключ: "значение" },
      { list: [1, "two", null, true, {}, []] },
      { escapes: 'quote " backslash \\ newline \n tab \t' },
      "a bare string",
      42,
      true,
      [],
    ];
    const wide = defineSchema({ k: { type: "i64" }, doc: { type: "json" } });
    const writer = createWriter(wide);
    documents.forEach((doc, index) => writer.append({ k: BigInt(index), doc }));
    const bytes = sync(writer.finish());
    const file = emit("json-shapes.parquet", bytes);

    // DuckDB's reading. `->>` yields text, so the expectations are DuckDB's own
    // rendering of what it found rather than tavolato's.
    expect(
      duckdb(`
      SELECT doc->>'$.nested.deep[2]' AS deep, doc->>'$.unicode' AS unicode,
             doc->>'$.list[1]' AS second, json_type(doc) AS kind
      FROM read_parquet(${file}) ORDER BY k;
    `),
    ).toEqual([
      { deep: "3", unicode: null, second: null, kind: "OBJECT" },
      { deep: null, unicode: "日本語", second: null, kind: "OBJECT" },
      { deep: null, unicode: null, second: "two", kind: "OBJECT" },
      { deep: null, unicode: null, second: null, kind: "OBJECT" },
      { deep: null, unicode: null, second: null, kind: "VARCHAR" },
      { deep: null, unicode: null, second: null, kind: "UBIGINT" },
      { deep: null, unicode: null, second: null, kind: "BOOLEAN" },
      { deep: null, unicode: null, second: null, kind: "ARRAY" },
    ]);

    // And tavolato's, from the same bytes.
    expect(readParquet(bytes).rows.map((row) => row.doc)).toEqual(documents);
  });
});

/**
 * The other half of the adapter story: DuckDB reads what the in-box column
 * types write, as the type they claim to be. `parquet_schema` proves the
 * annotation is the one the format prescribes; `DESCRIBE` proves a reader that
 * is not tavolato acts on it.
 */
describe("logical column types", () => {
  const schema = defineSchema({
    k: { type: "i64" },
    d: { type: date() },
    small: { type: decimal({ precision: 9, scale: 2 }) },
    medium: { type: decimal({ precision: 18, scale: 4 }) },
    large: { type: decimal({ precision: 38, scale: 6 }) },
    id: { type: uuid() },
    clock: { type: time({ unit: "millis", isAdjustedToUTC: false }) },
    precise: { type: time({ unit: "micros", isAdjustedToUTC: false }) },
    at: { type: timestamp({ unit: "micros", isAdjustedToUTC: true }) },
    half: { type: float16() },
    i8: { type: integer({ bitWidth: 8 }) },
    i16: { type: integer({ bitWidth: 16 }) },
    u8: { type: integer({ bitWidth: 8, signed: false }) },
    u32: { type: integer({ bitWidth: 32, signed: false }) },
    u64: { type: integer({ bitWidth: 64, signed: false }) },
    maybe: { type: uuid(), optional: true },
  });

  function path(): string {
    const writer = createWriter(schema);
    for (let index = 0; index < 3; index++) {
      writer.append({
        k: BigInt(index),
        d: new Date(Date.UTC(2026, 7, 24 + index)),
        small: `${index}.25`,
        medium: `-${index}.0001`,
        large: `12345678901234567890123456789012.${index}00000`,
        id: `b3f2c1a0-1111-4222-8333-44445555666${index}`,
        clock: 45_296_789 + index,
        precise: 45_296_789_012n + BigInt(index),
        at: 1_767_236_645_123_456n + BigInt(index),
        half: 1.5 * index,
        i8: index - 128,
        i16: index - 32_768,
        u8: 255 - index,
        u32: 4_294_967_295 - index,
        u64: 18_446_744_073_709_551_615n - BigInt(index),
        maybe: index % 2 === 0 ? null : `00000000-0000-0000-0000-00000000000${index}`,
      });
    }
    return emit("logical.parquet", writer.finish());
  }

  it("declares the physical and logical types the format prescribes", () => {
    expect(
      duckdb(
        `SELECT name, type, type_length, converted_type, scale, precision, logical_type
         FROM parquet_schema(${path()}) WHERE num_children IS NULL;`,
      ),
    ).toEqual([
      {
        name: "k",
        type: "INT64",
        type_length: null,
        converted_type: null,
        scale: null,
        precision: null,
        logical_type: null,
      },
      {
        name: "d",
        type: "INT32",
        type_length: null,
        converted_type: "DATE",
        scale: null,
        precision: null,
        logical_type: "DateType()",
      },
      {
        name: "small",
        type: "INT32",
        type_length: null,
        converted_type: "DECIMAL",
        scale: 2,
        precision: 9,
        logical_type: "DecimalType(scale=2, precision=9)",
      },
      {
        name: "medium",
        type: "INT64",
        type_length: null,
        converted_type: "DECIMAL",
        scale: 4,
        precision: 18,
        logical_type: "DecimalType(scale=4, precision=18)",
      },
      {
        name: "large",
        type: "FIXED_LEN_BYTE_ARRAY",
        type_length: "16",
        converted_type: "DECIMAL",
        scale: 6,
        precision: 38,
        logical_type: "DecimalType(scale=6, precision=38)",
      },
      {
        name: "id",
        type: "FIXED_LEN_BYTE_ARRAY",
        type_length: "16",
        converted_type: null,
        scale: null,
        precision: null,
        logical_type: "UUIDType()",
      },
      {
        name: "clock",
        type: "INT32",
        type_length: null,
        converted_type: "TIME_MILLIS",
        scale: null,
        precision: null,
        logical_type:
          "TimeType(isAdjustedToUTC=0, unit=TimeUnit(MILLIS=MilliSeconds(), MICROS=<null>, NANOS=<null>))",
      },
      {
        name: "precise",
        type: "INT64",
        type_length: null,
        converted_type: "TIME_MICROS",
        scale: null,
        precision: null,
        logical_type:
          "TimeType(isAdjustedToUTC=0, unit=TimeUnit(MILLIS=<null>, MICROS=MicroSeconds(), NANOS=<null>))",
      },
      {
        name: "at",
        type: "INT64",
        type_length: null,
        converted_type: "TIMESTAMP_MICROS",
        scale: null,
        precision: null,
        logical_type:
          "TimestampType(isAdjustedToUTC=1, unit=TimeUnit(MILLIS=<null>, MICROS=MicroSeconds(), NANOS=<null>))",
      },
      {
        name: "half",
        type: "FIXED_LEN_BYTE_ARRAY",
        type_length: "2",
        converted_type: null,
        scale: null,
        precision: null,
        logical_type: "Float16Type()",
      },
      {
        name: "i8",
        type: "INT32",
        type_length: null,
        converted_type: "INT_8",
        scale: null,
        precision: null,
        logical_type: "IntType(bitWidth=\b, isSigned=1)",
      },
      {
        name: "i16",
        type: "INT32",
        type_length: null,
        converted_type: "INT_16",
        scale: null,
        precision: null,
        logical_type: "IntType(bitWidth=, isSigned=1)",
      },
      {
        name: "u8",
        type: "INT32",
        type_length: null,
        converted_type: "UINT_8",
        scale: null,
        precision: null,
        logical_type: "IntType(bitWidth=\b, isSigned=0)",
      },
      {
        name: "u32",
        type: "INT32",
        type_length: null,
        converted_type: "UINT_32",
        scale: null,
        precision: null,
        logical_type: "IntType(bitWidth= , isSigned=0)",
      },
      {
        name: "u64",
        type: "INT64",
        type_length: null,
        converted_type: "UINT_64",
        scale: null,
        precision: null,
        logical_type: "IntType(bitWidth=@, isSigned=0)",
      },
      {
        name: "maybe",
        type: "FIXED_LEN_BYTE_ARRAY",
        type_length: "16",
        converted_type: null,
        scale: null,
        precision: null,
        logical_type: "UUIDType()",
      },
    ]);
  });

  it("gives DuckDB the types it names, not bytes to interpret", () => {
    expect(
      duckdb(`DESCRIBE SELECT * FROM read_parquet(${path()});`).map((row) => row.column_type),
    ).toEqual([
      "BIGINT",
      "DATE",
      "DECIMAL(9,2)",
      "DECIMAL(18,4)",
      "DECIMAL(38,6)",
      "UUID",
      "TIME",
      "TIME",
      "TIMESTAMP WITH TIME ZONE",
      "FLOAT",
      "TINYINT",
      "SMALLINT",
      "UTINYINT",
      "UINTEGER",
      "UBIGINT",
      "UUID",
    ]);
  });

  it("agrees with DuckDB on every value", () => {
    expect(
      duckdb(`
      SELECT
        k, d::VARCHAR AS d, small::VARCHAR AS small, medium::VARCHAR AS medium,
        large::VARCHAR AS large, id::VARCHAR AS id, clock::VARCHAR AS clock,
        precise::VARCHAR AS precise, epoch_us("at") AS at, half,
        i8, i16, u8, u32, u64::VARCHAR AS u64, maybe::VARCHAR AS maybe
      FROM read_parquet(${path()}) ORDER BY k;
    `),
    ).toEqual(
      Array.from({ length: 3 }, (_, index) => ({
        k: index,
        d: `2026-08-${24 + index}`,
        small: `${index}.25`,
        medium: `-${index}.0001`,
        large: `12345678901234567890123456789012.${index}00000`,
        id: `b3f2c1a0-1111-4222-8333-44445555666${index}`,
        // 45_296_789 ms and 45_296_789_012 µs are the same instant of the day;
        // DuckDB prints a TIME without its trailing zeroes.
        clock: ["12:34:56.789", "12:34:56.79", "12:34:56.791"][index],
        precise: `12:34:56.78901${2 + index}`,
        at: 1_767_236_645_123_456 + index,
        half: 1.5 * index,
        i8: index - 128,
        i16: index - 32_768,
        u8: 255 - index,
        u32: 4_294_967_295 - index,
        u64: (18_446_744_073_709_551_615n - BigInt(index)).toString(),
        maybe: index % 2 === 0 ? null : `00000000-0000-0000-0000-00000000000${index}`,
      })),
    );
  });

  it("gives DuckDB the minimal valid fixed-width DECIMAL layout", () => {
    const compact = defineSchema({ value: { type: decimal({ precision: 30, scale: 2 }) } });
    const writer = createWriter(compact);
    writer.append({ value: "1234567890123456789012345678.90" });
    writer.append({ value: "-1.25" });
    const file = emit("compact-decimal.parquet", writer.finish());

    expect(
      duckdb(
        `SELECT type, type_length, precision, scale
         FROM parquet_schema(${file}) WHERE name = 'value';`,
      ),
    ).toEqual([{ type: "FIXED_LEN_BYTE_ARRAY", type_length: "13", precision: 30, scale: 2 }]);
    expect(
      duckdb(`SELECT value::VARCHAR AS value FROM read_parquet(${file}) ORDER BY value;`),
    ).toEqual([{ value: "-1.25" }, { value: "1234567890123456789012345678.90" }]);
  });

  it("stays readable through a codec", () => {
    // Adapters transform values, codecs transform the page those values land
    // in, and neither knows about the other — DuckDB is the one asked to agree.
    const compressed = defineSchema({
      d: { type: date() },
      n: { type: decimal({ precision: 12, scale: 2 }) },
      id: { type: uuid() },
    });
    const writer = createWriter(compressed, { codec: { name: "GZIP", compress: gzipSync } });
    for (let index = 0; index < 40; index++) {
      sync(
        writer.append({
          d: new Date(Date.UTC(2026, 0, 1 + index)),
          n: `${index}.50`,
          id: `b3f2c1a0-1111-4222-8333-4444555566${String(index).padStart(2, "0")}`,
        }),
      );
    }
    const path = emit("logical-gzip.parquet", writer.finish());

    expect(duckdb(`SELECT DISTINCT compression FROM parquet_metadata(${path});`)).toEqual([
      { compression: "GZIP" },
    ]);
    expect(
      duckdbRow(`
      SELECT
        count(*) AS rows,
        count(*) FILTER (WHERE d <> DATE '2026-01-01' + (n - 0.5)::INTEGER) AS d_mismatch,
        sum(n)::VARCHAR AS total
      FROM read_parquet(${path});
    `),
    ).toEqual({ rows: 40, d_mismatch: 0, total: "800.00" });
  });
});

describe("schema declaration", () => {
  const schema = defineSchema({
    s: { type: "string" },
    so: { type: "string", optional: true },
    f: { type: "f64" },
    i: { type: "i64" },
    b: { type: "bool" },
    t: { type: "timestamp" },
    to: { type: "timestamp", optional: true },
  });

  function path(): string {
    const writer = createWriter(schema);
    writer.append({ s: "x", f: 1, i: 1n, b: true, t: 0 });
    return emit("declared-schema.parquet", writer.finish());
  }

  it("matches DESCRIBE", () => {
    expect(
      duckdb(`DESCRIBE SELECT * FROM read_parquet(${path()});`).map((row) => ({
        column_name: row.column_name,
        column_type: row.column_type,
      })),
    ).toEqual([
      { column_name: "s", column_type: "VARCHAR" },
      { column_name: "so", column_type: "VARCHAR" },
      { column_name: "f", column_type: "DOUBLE" },
      { column_name: "i", column_type: "BIGINT" },
      { column_name: "b", column_type: "BOOLEAN" },
      // TIMESTAMP_MILLIS is UTC-normalised by definition, so DuckDB surfaces it
      // as an instant rather than a wall clock.
      { column_name: "t", column_type: "TIMESTAMP WITH TIME ZONE" },
      { column_name: "to", column_type: "TIMESTAMP WITH TIME ZONE" },
    ]);
  });

  it("matches parquet_schema, including nullability", () => {
    const rows = duckdb(
      `SELECT name, type, repetition_type, converted_type, num_children FROM parquet_schema(${path()});`,
    );
    expect(rows).toEqual([
      {
        name: "schema",
        type: null,
        repetition_type: null,
        converted_type: null,
        num_children: 7,
      },
      {
        name: "s",
        type: "BYTE_ARRAY",
        repetition_type: "REQUIRED",
        converted_type: "UTF8",
        num_children: null,
      },
      {
        name: "so",
        type: "BYTE_ARRAY",
        repetition_type: "OPTIONAL",
        converted_type: "UTF8",
        num_children: null,
      },
      {
        name: "f",
        type: "DOUBLE",
        repetition_type: "REQUIRED",
        converted_type: null,
        num_children: null,
      },
      {
        name: "i",
        type: "INT64",
        repetition_type: "REQUIRED",
        converted_type: null,
        num_children: null,
      },
      {
        name: "b",
        type: "BOOLEAN",
        repetition_type: "REQUIRED",
        converted_type: null,
        num_children: null,
      },
      {
        name: "t",
        type: "INT64",
        repetition_type: "REQUIRED",
        converted_type: "TIMESTAMP_MILLIS",
        num_children: null,
      },
      {
        name: "to",
        type: "INT64",
        repetition_type: "OPTIONAL",
        converted_type: "TIMESTAMP_MILLIS",
        num_children: null,
      },
    ]);
  });

  it("annotates logical types alongside the deprecated converted types", () => {
    const rows = duckdb<{ name: string; logical_type: string | null }>(
      `SELECT name, logical_type FROM parquet_schema(${path()}) WHERE name IN ('s', 't', 'i');`,
    );
    const byName = Object.fromEntries(rows.map((row) => [row.name, row.logical_type]));
    expect(byName.s).toBe("StringType()");
    expect(byName.t).toContain("isAdjustedToUTC=1");
    expect(byName.t).toContain("MILLIS=MilliSeconds()");
    expect(byName.i).toBeNull();
  });

  it("declares PLAIN, and RLE only where levels exist", () => {
    const rows = duckdb<{ path_in_schema: string; encodings: string }>(
      `SELECT path_in_schema, encodings FROM parquet_metadata(${path()});`,
    );
    expect(Object.fromEntries(rows.map((row) => [row.path_in_schema, row.encodings]))).toEqual({
      s: "PLAIN",
      so: "RLE, PLAIN",
      f: "PLAIN",
      i: "PLAIN",
      b: "PLAIN",
      t: "PLAIN",
      to: "RLE, PLAIN",
    });
  });
});

describe("file structure", () => {
  it("produces a readable file with zero rows", () => {
    const schema = defineSchema({
      s: { type: "string" },
      n: { type: "i64", optional: true },
    });
    const path = emit("empty.parquet", createWriter(schema).finish());

    expect(duckdbRow(`SELECT count(*) AS rows FROM read_parquet(${path});`)).toEqual({ rows: 0 });
    expect(
      duckdbRow(`SELECT num_rows, num_row_groups FROM parquet_file_metadata(${path});`),
    ).toEqual({ num_rows: 0, num_row_groups: 0 });
    expect(
      duckdb(`DESCRIBE SELECT * FROM read_parquet(${path});`).map((row) => row.column_name),
    ).toEqual(["s", "n"]);
  });

  it("splits into row groups at rowGroupSize", () => {
    const schema = defineSchema({ n: { type: "i64" } });
    const writer = createWriter(schema, { rowGroupSize: 7 });
    for (let index = 0; index < 30; index++) writer.append({ n: BigInt(index) });
    const path = emit("row-groups.parquet", writer.finish());

    // 30 rows at 7 per group is four full groups plus a two row remainder.
    expect(
      duckdbRow(`SELECT num_rows, num_row_groups FROM parquet_file_metadata(${path});`),
    ).toEqual({ num_rows: 30, num_row_groups: 5 });
    expect(
      duckdb<{ num_rows: number }>(
        `SELECT DISTINCT row_group_id, row_group_num_rows AS num_rows FROM parquet_metadata(${path}) ORDER BY row_group_id;`,
      ).map((row) => row.num_rows),
    ).toEqual([7, 7, 7, 7, 2]);
    expect(duckdbRow(`SELECT sum(n)::BIGINT AS total FROM read_parquet(${path});`)).toEqual({
      total: (29 * 30) / 2,
    });
  });

  it("does not emit an empty trailing row group when rows divide evenly", () => {
    const schema = defineSchema({ n: { type: "i64" } });
    const writer = createWriter(schema, { rowGroupSize: 10 });
    for (let index = 0; index < 30; index++) writer.append({ n: BigInt(index) });
    const path = emit("row-groups-exact.parquet", writer.finish());

    expect(
      duckdbRow(`SELECT num_rows, num_row_groups FROM parquet_file_metadata(${path});`),
    ).toEqual({ num_rows: 30, num_row_groups: 3 });
  });

  it("round-trips non-ASCII column names", () => {
    const schema = defineSchema({ "città": { type: "string" }, "n°": { type: "i64" } });
    const writer = createWriter(schema);
    writer.append({ "città": "Roma", "n°": 1n });
    const path = emit("column-names.parquet", writer.finish());

    expect(duckdb(`SELECT * FROM read_parquet(${path});`)).toEqual([{ "città": "Roma", "n°": 1 }]);
    expect(
      duckdb<{ name: string }>(
        `SELECT name FROM parquet_schema(${path}) WHERE num_children IS NULL;`,
      ).map((row) => row.name),
    ).toEqual(["città", "n°"]);
  });

  it("round-trips a column named __proto__, which only JavaScript finds special", () => {
    // Parquet reserves no column names. DuckDB is the witness that the value is
    // really in the file, so a reader that loses it is losing data rather than
    // reading a file that never had it.
    const schema = defineSchema({ ["__proto__"]: { type: "i64" }, n: { type: "i64" } });
    const writer = createWriter(schema);
    writer.append({ ["__proto__"]: 7n, n: 1n });
    const path = emit("proto-column.parquet", writer.finish());

    // A computed key on this side too: an object literal spelling `__proto__`
    // declares a prototype, not a property, so the expectation would otherwise
    // be an empty-handed `{ n: 1 }`.
    expect(duckdb(`SELECT "__proto__", n FROM read_parquet(${path});`)).toEqual([
      { ["__proto__"]: 7, n: 1 },
    ]);
    expect(
      duckdb<{ name: string }>(
        `SELECT name FROM parquet_schema(${path}) WHERE num_children IS NULL;`,
      ).map((row) => row.name),
    ).toEqual(["__proto__", "n"]);
  });

  it("records created_by", () => {
    const schema = defineSchema({ n: { type: "i64" } });
    const path = emit("created-by.parquet", createWriter(schema).finish());
    expect(duckdbRow(`SELECT created_by FROM parquet_file_metadata(${path});`)).toEqual({
      created_by: "tavolato",
    });

    const custom = emit(
      "created-by-custom.parquet",
      createWriter(schema, { createdBy: "my-app version 1.2.3" }).finish(),
    );
    expect(duckdbRow(`SELECT created_by FROM parquet_file_metadata(${custom});`)).toEqual({
      created_by: "my-app version 1.2.3",
    });
  });

  it("stays consistent after a rejected row", () => {
    const schema = defineSchema({ n: { type: "i64" }, s: { type: "string" } });
    const writer = createWriter(schema);
    writer.append({ n: 1n, s: "one" });
    // @ts-expect-error deliberately wrong input
    expect(() => writer.append({ n: 2n, s: 2 })).toThrow(TavolatoError);
    writer.append({ n: 3n, s: "three" });
    const path = emit("rejected.parquet", writer.finish());

    expect(duckdb(`SELECT n, s FROM read_parquet(${path}) ORDER BY n;`)).toEqual([
      { n: 1, s: "one" },
      { n: 3, s: "three" },
    ]);
  });
});

describe("large files", () => {
  it("round-trips 100k rows across many row groups", () => {
    const schema = defineSchema({
      id: { type: "i64" },
      name: { type: "string", optional: true },
      value: { type: "f64" },
      flag: { type: "bool", optional: true },
      ts: { type: "timestamp" },
    });
    const rows = 100_000;
    const base = 1_700_000_000_000;
    const writer = createWriter(schema); // default rowGroupSize of 10_000

    for (let id = 0; id < rows; id++) {
      writer.append({
        id: BigInt(id),
        name: id % 7 === 0 ? null : `host-${id}`,
        value: id + 0.5,
        flag: id % 11 === 0 ? null : id % 2 === 0,
        ts: base + id * 1000,
      });
    }
    const path = emit("large.parquet", writer.finish());

    expect(
      duckdbRow(`SELECT num_rows, num_row_groups FROM parquet_file_metadata(${path});`),
    ).toEqual({ num_rows: rows, num_row_groups: rows / 10_000 });

    // Every value is verified inside DuckDB, so nothing large crosses the CLI.
    expect(
      duckdbRow(`
      SELECT
        count(*) AS rows,
        sum(id)::BIGINT AS id_sum,
        sum(value) AS value_sum,
        count(name) AS names,
        count(flag) AS flags,
        min(epoch_ms(ts)) AS ts_min,
        max(epoch_ms(ts)) AS ts_max,
        count(*) FILTER (WHERE name IS DISTINCT FROM CASE WHEN id % 7 = 0 THEN NULL ELSE 'host-' || id END) AS name_mismatch,
        count(*) FILTER (WHERE value <> id + 0.5) AS value_mismatch,
        count(*) FILTER (WHERE flag IS DISTINCT FROM CASE WHEN id % 11 = 0 THEN NULL ELSE id % 2 = 0 END) AS flag_mismatch,
        count(*) FILTER (WHERE epoch_ms(ts) <> ${base} + id * 1000) AS ts_mismatch
      FROM read_parquet(${path});
    `),
    ).toEqual({
      rows,
      id_sum: ((rows - 1) * rows) / 2,
      value_sum: ((rows - 1) * rows) / 2 + rows * 0.5,
      names: rows - Math.ceil(rows / 7),
      flags: rows - Math.ceil(rows / 11),
      ts_min: base,
      ts_max: base + (rows - 1) * 1000,
      name_mismatch: 0,
      value_mismatch: 0,
      flag_mismatch: 0,
      ts_mismatch: 0,
    });

    // Row group boundaries must not disturb ordering.
    expect(
      duckdbRow(`
      SELECT count(*) AS out_of_order
      FROM (SELECT id, lag(id) OVER () AS previous FROM read_parquet(${path}))
      WHERE previous IS NOT NULL AND id <> previous + 1;
    `),
    ).toEqual({ out_of_order: 0 });

    // Null counts, summed across the per-row-group chunk statistics, must add
    // up to the real number of nulls.
    expect(
      duckdb(`
      SELECT path_in_schema AS name, sum(stats_null_count)::BIGINT AS nulls
      FROM parquet_metadata(${path})
      WHERE path_in_schema IN ('name', 'flag')
      GROUP BY name ORDER BY name;
    `),
    ).toEqual([
      { name: "flag", nulls: Math.ceil(rows / 11) },
      { name: "name", nulls: Math.ceil(rows / 7) },
    ]);
  });

  // Costs a few gigabytes of memory and tens of seconds, so it only runs when
  // asked for: TAVOLATO_TEST_HUGE=1 pnpm test
  it.runIf(process.env.TAVOLATO_TEST_HUGE)(
    "splits a row group early instead of overflowing the i32 page size",
    () => {
      const schema = defineSchema({ k: { type: "i64" }, s: { type: "string" } });
      const writer = createWriter(schema); // rowGroupSize 10_000 never reached
      const block = "x".repeat(8 * 1024 * 1024);
      const rows = 300;
      for (let index = 0; index < rows; index++) writer.append({ k: BigInt(index), s: block });
      const path = emit("huge.parquet", writer.finish());

      // 300 rows of 8 MiB strings exceed the ~2 GiB page ceiling once, so the
      // file must hold two row groups, both readable and complete.
      expect(
        duckdbRow(`SELECT num_rows, num_row_groups FROM parquet_file_metadata(${path});`),
      ).toEqual({ num_rows: rows, num_row_groups: 2 });
      expect(
        duckdbRow(`
        SELECT count(*) AS rows, sum(strlen(s))::BIGINT AS bytes, sum(k)::BIGINT AS k_sum
        FROM read_parquet(${path});
      `),
      ).toEqual({
        rows,
        bytes: rows * block.length,
        k_sum: ((rows - 1) * rows) / 2,
      });
    },
    120_000,
  );
});
