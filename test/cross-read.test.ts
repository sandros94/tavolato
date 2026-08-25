import { readFileSync } from "node:fs";
import { join } from "node:path";
import { gunzipSync, zstdDecompressSync } from "node:zlib";
import { afterAll, describe, expect, it } from "vitest";
import {
  createWriter,
  date,
  decimal,
  integer,
  JSON_NULL,
  readParquet,
  readRowGroups,
  time,
  timestamp,
  uuid,
} from "../src/index.ts";
import type { CodecName, ReaderCodec, ReadRow } from "../src/index.ts";
import { cleanupTempDir, duckdb, duckdbRow, sqlPath, tempDir, writeParquet } from "./_duckdb.ts";
import { expectError } from "./_errors.ts";
import { sync } from "./_sync.ts";

/**
 * The other direction: DuckDB writes, tavolato reads.
 *
 * `COPY … (FORMAT PARQUET, COMPRESSION UNCOMPRESSED)` lands inside tavolato's
 * subset for the plain cases — v1 pages, PLAIN values, RLE definition levels —
 * and those files are read and compared value by value.
 *
 * DuckDB leaves that subset as soon as it has a reason to: a repetitive column
 * gets a dictionary, a `TIMESTAMPTZ` gets microseconds, a list gets a nested
 * schema. Those files must not be read at all; the typed error naming the
 * feature *is* the assertion, and it is what proves the scope promise is
 * enforced rather than merely documented.
 *
 * A codec is the one refusal that lifts. A compressed file is refused with that
 * same error until a decompressor is registered, and read value for value once
 * one is — both halves are checked below.
 */

afterAll(() => cleanupTempDir());

/** Has DuckDB write a Parquet file and returns its bytes. */
function copyTo(name: string, select: string, options = "COMPRESSION UNCOMPRESSED"): Uint8Array {
  const path = join(tempDir(), name);
  duckdb(`COPY (${select}) TO ${sqlPath(path)} (FORMAT PARQUET, ${options});`);
  return new Uint8Array(readFileSync(path));
}

describe("files DuckDB writes inside the subset", () => {
  it("reads every column type back, nulls included", () => {
    const bytes = copyTo(
      "cross-types.parquet",
      `SELECT
         'row-' || i AS s,
         (i + 0.5)::DOUBLE AS f,
         i::BIGINT AS n,
         (i % 2 = 0) AS b,
         CASE WHEN i % 3 = 0 THEN NULL ELSE 'opt-' || i END AS o
       FROM range(5) tbl(i)`,
    );
    const { schema, rows } = readParquet(bytes);

    // DuckDB declares every column nullable, which is a faithful OPTIONAL.
    expect(schema.columns).toEqual([
      { name: "s", type: "string", optional: true },
      { name: "f", type: "f64", optional: true },
      { name: "n", type: "i64", optional: true },
      { name: "b", type: "bool", optional: true },
      { name: "o", type: "string", optional: true },
    ]);
    expect(rows).toEqual(
      Array.from({ length: 5 }, (_, i) => ({
        s: `row-${i}`,
        f: i + 0.5,
        n: BigInt(i),
        b: i % 2 === 0,
        o: i % 3 === 0 ? null : `opt-${i}`,
      })),
    );
  });

  it("agrees with DuckDB's own reading of the same file, row for row", () => {
    const bytes = copyTo(
      "cross-agree.parquet",
      `SELECT i::BIGINT AS n, CASE WHEN i % 4 = 0 THEN NULL ELSE 'v' || i END AS s
       FROM range(200) tbl(i)`,
    );
    const path = sqlPath(join(tempDir(), "cross-agree.parquet"));
    const reference = duckdb<{ n: string; s: string | null }>(
      `SELECT n::VARCHAR AS n, s FROM read_parquet(${path}) ORDER BY n::BIGINT;`,
    );
    const { rows } = readParquet(bytes);

    expect(rows).toHaveLength(reference.length);
    expect(rows.map((row) => ({ n: String(row.n as bigint), s: row.s }))).toEqual(reference);
  });

  it("reads a file DuckDB split into several row groups", () => {
    const bytes = copyTo(
      "cross-groups.parquet",
      `SELECT i::BIGINT AS n FROM range(500) tbl(i)`,
      "COMPRESSION UNCOMPRESSED, ROW_GROUP_SIZE 100",
    );
    const groups = duckdbRow<{ num_row_groups: number }>(
      `SELECT num_row_groups FROM parquet_file_metadata(${sqlPath(join(tempDir(), "cross-groups.parquet"))});`,
    ).num_row_groups;
    const { rows } = readParquet(bytes);

    expect(groups).toBeGreaterThan(0);
    expect(rows).toHaveLength(500);
    expect(rows.map((row) => row.n)).toEqual(Array.from({ length: 500 }, (_, i) => BigInt(i)));
  });

  it("reads an empty result set", () => {
    const bytes = copyTo(
      "cross-empty.parquet",
      `SELECT i::BIGINT AS n FROM range(5) tbl(i) WHERE false`,
    );
    const { schema, rows } = readParquet(bytes);
    expect(schema.columns.map((column) => column.name)).toEqual(["n"]);
    expect(rows).toEqual([]);
  });

  it("reads the narrower physical types the built-ins own", () => {
    // DuckDB annotates an INTEGER with the deprecated INT_32, which says no
    // more than the bare INT32 does — the same leniency as INT_64 on i64.
    const bytes = copyTo(
      "cross-narrow.parquet",
      `SELECT i::INTEGER AS n, (i + 0.5)::FLOAT AS f FROM range(5) tbl(i)`,
    );
    const { schema, rows } = readParquet(bytes);
    expect(schema.columns.map((column) => column.type)).toEqual(["i32", "f32"]);
    expect(rows).toEqual(Array.from({ length: 5 }, (_, i) => ({ n: i, f: i + 0.5 })));
  });

  it("reads a JSON-annotated column as the documents it holds", () => {
    const bytes = copyTo(
      "cross-json.parquet",
      `SELECT ('{"id":' || i || ',"tag":"t' || i || '","tags":[' || i || ',null],"π":"日本"}')::JSON AS j
       FROM range(5) tbl(i)`,
    );
    const { schema, rows } = readParquet(bytes);
    expect(schema.columns).toEqual([{ name: "j", type: "json", optional: true }]);
    // DuckDB stores the document as text, and the annotation is what says the
    // text is JSON — which is exactly the licence tavolato needs to parse it.
    expect(rows.map((row) => row.j)).toEqual(
      Array.from({ length: 5 }, (_, i) => ({
        id: i,
        tag: `t${i}`,
        tags: [i, null],
        π: "日本",
      })),
    );
  });

  it("distinguishes DuckDB SQL null from its JSON document literal null", () => {
    const bytes = copyTo(
      "cross-json-null.parquet",
      `SELECT k, CASE WHEN k = 0 THEN NULL::JSON ELSE 'null'::JSON END AS j
       FROM range(2) tbl(k)`,
    );
    const { schema, rows } = readParquet(bytes);
    expect(schema.columns).toEqual([
      { name: "k", type: "i64", optional: true },
      { name: "j", type: "json", optional: true },
    ]);
    expect(rows).toEqual([
      { k: 0n, j: null },
      { k: 1n, j: JSON_NULL },
    ]);
    expect(rows[1].j).toBe(JSON_NULL);
  });

  it("hands a DuckDB document back to DuckDB unchanged", () => {
    // The full circle: DuckDB writes the JSON, tavolato parses it, tavolato
    // writes it again from the structure, and DuckDB reads the same values out
    // of the second file as out of the first.
    const bytes = copyTo(
      "cross-json-circle.parquet",
      `SELECT ('{"id":' || i || ',"nested":{"tags":["a","b"],"n":' || i || '.5}}')::JSON AS j
       FROM range(4) tbl(i)`,
    );
    const { schema, rows } = readParquet(bytes);

    const writer = createWriter(schema);
    for (const row of rows) writer.append({ j: row.j });
    const again = sqlPath(writeParquet("cross-json-again.parquet", sync(writer.finish())));

    const query = (path: string): unknown[] =>
      duckdb(
        `SELECT (j->>'$.id')::BIGINT AS id, j->>'$.nested.tags[1]' AS tag, (j->>'$.nested.n')::DOUBLE AS n
         FROM read_parquet(${path}) ORDER BY id;`,
      );
    expect(query(again)).toEqual(query(sqlPath(join(tempDir(), "cross-json-circle.parquet"))));
    expect(query(again)).toEqual(
      Array.from({ length: 4 }, (_, i) => ({ id: i, tag: "b", n: i + 0.5 })),
    );
  });
});

/**
 * Compression is the one place the scope opens up on request. A DuckDB file
 * that was refused a moment ago is read value for value once a decompressor is
 * handed over — and the same file is still refused without one.
 */
describe("files DuckDB compresses, read with a registered decompressor", () => {
  const legs: readonly { readonly name: CodecName; readonly codec: ReaderCodec }[] = [
    { name: "GZIP", codec: { decompress: (page) => gunzipSync(page) } },
    { name: "ZSTD", codec: { decompress: (page) => zstdDecompressSync(page) } },
  ];

  for (const { name, codec } of legs) {
    it(`reads a ${name} file once a decompressor is registered`, () => {
      // 200 distinct values per column: DuckDB reaches for a dictionary as soon
      // as repetition pays, and dictionary pages are still out of scope.
      const bytes = copyTo(
        `cross-codec-${name}.parquet`,
        `SELECT i::BIGINT AS n, 'v' || i AS s, (i + 0.5)::DOUBLE AS f,
                CASE WHEN i % 4 = 0 THEN NULL ELSE 'o' || i END AS o
         FROM range(200) tbl(i)`,
        `COMPRESSION ${name}, ROW_GROUP_SIZE 64`,
      );
      const path = sqlPath(join(tempDir(), `cross-codec-${name}.parquet`));
      const encodings = duckdb<{ encodings: string }>(
        `SELECT DISTINCT encodings FROM parquet_metadata(${path});`,
      ).map((row) => row.encodings);
      expect(encodings.length).toBeGreaterThan(0);
      expect(encodings.some((encoding) => encoding.includes("DICTIONARY"))).toBe(false);

      const refusal = expectError("ERR_READ_UNSUPPORTED", () => readParquet(bytes));
      expect(refusal.message).toContain(`compressed with ${name}`);
      expect(refusal.message).toContain(`register a decompressor for ${name}`);

      const { rows } = sync(readParquet(bytes, { codecs: { [name]: codec } }));
      expect(rows).toHaveLength(200);
      expect(rows).toEqual(
        Array.from({ length: 200 }, (_, i) => ({
          n: BigInt(i),
          s: `v${i}`,
          f: i + 0.5,
          o: i % 4 === 0 ? null : `o${i}`,
        })),
      );
    });
  }

  it("reads one row group at a time, and agrees with DuckDB group for group", () => {
    // DuckDB's own row groups, forced small, walked one at a time through a
    // codec hook: the boundaries are DuckDB's, and so is every value.
    const file = "cross-lazy.parquet";
    // DuckDB rounds a row group down to whole vectors, so 2048 is the smallest
    // group it will actually write; 6000 rows is three of them.
    const bytes = copyTo(
      file,
      `SELECT i::BIGINT AS n, 'v' || i AS s, (i + 0.5)::DOUBLE AS f
       FROM range(6000) tbl(i)`,
      "COMPRESSION GZIP, ROW_GROUP_SIZE 2048",
    );
    const path = sqlPath(join(tempDir(), file));
    const encodings = duckdb<{ encodings: string }>(
      `SELECT DISTINCT encodings FROM parquet_metadata(${path});`,
    ).map((row) => row.encodings);
    expect(encodings.some((encoding) => encoding.includes("DICTIONARY"))).toBe(false);

    const groups = duckdb<{ num_rows: number }>(
      `SELECT DISTINCT row_group_id, row_group_num_rows AS num_rows FROM parquet_metadata(${path}) ORDER BY row_group_id;`,
    ).map((row) => row.num_rows);
    expect(groups.length).toBeGreaterThan(1);

    const lazy = readRowGroups(bytes, {
      codecs: { GZIP: { decompress: (page) => gunzipSync(page) } },
    });
    expect(lazy.rowCount).toBe(6000);
    expect(lazy.groupCount).toBe(groups.length);

    const walked: ReadRow[][] = [];
    for (const rows of lazy) walked.push(sync(rows));
    expect(walked.map((rows) => rows.length)).toEqual(groups);

    const reference = duckdb<{ n: string; s: string; f: number }>(
      `SELECT n::VARCHAR AS n, s, f FROM read_parquet(${path}) ORDER BY n::BIGINT;`,
    );
    expect(
      walked.flat().map((row) => ({ n: String(row.n as bigint), s: row.s, f: row.f })),
    ).toEqual(reference);
  });

  it("reads a compressed JSON column too", () => {
    const bytes = copyTo(
      "cross-json-gzip.parquet",
      `SELECT ('{"id":' || i || '}')::JSON AS j FROM range(50) tbl(i)`,
      "COMPRESSION GZIP",
    );
    const { schema, rows } = sync(
      readParquet(bytes, { codecs: { GZIP: { decompress: (page) => gunzipSync(page) } } }),
    );
    expect(schema.columns[0].type).toBe("json");
    expect(rows.map((row) => row.j)).toEqual(Array.from({ length: 50 }, (_, i) => ({ id: i })));
  });
});

/**
 * Projection against the oracle. DuckDB is the reader that decides what a file
 * really holds, so a projected read is checked the only honest way: against
 * DuckDB reading the same columns out of the same file.
 */
describe("reading some of DuckDB's columns and none of the rest", () => {
  it("agrees with DuckDB's own projection of the same file", () => {
    const file = "cross-projection.parquet";
    const bytes = copyTo(
      file,
      `SELECT i::BIGINT AS n, 'v' || i AS s, (i + 0.5)::DOUBLE AS f,
              CASE WHEN i % 4 = 0 THEN NULL ELSE 'o' || i END AS o
       FROM range(300) tbl(i)`,
      "COMPRESSION UNCOMPRESSED, ROW_GROUP_SIZE 2048",
    );
    const path = sqlPath(join(tempDir(), file));
    const reference = duckdb<{ n: string; o: string | null }>(
      `SELECT n::VARCHAR AS n, o FROM read_parquet(${path}) ORDER BY n::BIGINT;`,
    );

    const { schema, rows } = readParquet(bytes, { columns: ["o", "n"] });
    // The file's order, not the order asked for, on both halves.
    expect(schema.columns.map((column) => column.name)).toEqual(["n", "o"]);
    expect(rows.map((row) => ({ n: String(row.n as bigint), o: row.o }))).toEqual(reference);
    expect(Object.keys(rows[0])).toEqual(["n", "o"]);
  });

  it("reads past a dictionary-encoded column by projecting it away", () => {
    // DuckDB reaches for a dictionary as soon as repetition pays, and a
    // dictionary page is out of scope. The plain column beside it is not, and
    // its chunk is a seek away.
    const file = "cross-projection-dictionary.parquet";
    const bytes = copyTo(
      file,
      `SELECT 'host-' || (i % 5) AS s, i::BIGINT AS n FROM range(20000) tbl(i)`,
    );
    const path = sqlPath(join(tempDir(), file));
    const encodings = duckdb<{ name: string; encodings: string }>(
      `SELECT path_in_schema AS name, encodings FROM parquet_metadata(${path});`,
    );
    expect(
      encodings.filter((row) => row.encodings.includes("DICTIONARY")).map((row) => row.name),
    ).toEqual(["s"]);

    const refusal = expectError("ERR_READ_UNSUPPORTED", () => readParquet(bytes));
    expect(refusal.message).toContain("dictionary");
    expect(refusal.column).toBe("s");

    const { schema, rows } = readParquet(bytes, { columns: ["n"] });
    expect(schema.columns).toEqual([{ name: "n", type: "i64", optional: true }]);
    expect(rows).toHaveLength(20_000);
    expect(rows.at(-1)).toEqual({ n: 19_999n });
  });

  it("reads past a compressed column by projecting it away", () => {
    // Every column of a DuckDB file shares its codec, so this is the refusal
    // lifting rather than a mixed file: with only the projected column read,
    // the codec is still needed — and once it is registered, only that column's
    // pages go through it.
    const file = "cross-projection-gzip.parquet";
    const bytes = copyTo(
      file,
      `SELECT i::BIGINT AS n, 'v' || i AS s FROM range(300) tbl(i)`,
      "COMPRESSION GZIP",
    );
    const { rows } = sync(
      readParquet(bytes, {
        columns: ["s"],
        codecs: { GZIP: { decompress: (page) => gunzipSync(page) } },
      }),
    );
    expect(rows).toHaveLength(300);
    expect(rows.at(-1)).toEqual({ s: "v299" });
  });

  it("reads past DuckDB's annotated columns by projecting them away", () => {
    // A DATE, a DECIMAL and a UUID, none of which has a built-in reading — and
    // a BIGINT beside them, which has.
    const bytes = copyTo(
      "cross-projection-annotated.parquet",
      `SELECT (DATE '2026-08-24' + i::INTEGER) AS d, (i + 0.25)::DECIMAL(18, 2) AS p,
              ('b3f2c1a0-1111-4222-8333-44445555666' || i)::UUID AS u, i::BIGINT AS n
       FROM range(3) tbl(i)`,
    );
    expectError("ERR_READ_UNSUPPORTED", () => readParquet(bytes));

    const { schema, rows } = readParquet(bytes, { columns: ["n"] });
    expect(schema.columns.map((column) => column.type)).toEqual(["i64"]);
    expect(rows).toEqual([{ n: 0n }, { n: 1n }, { n: 2n }]);

    // One of them back in, with the column type that claims it.
    expect(
      readParquet(bytes, { columns: ["n", "p"], types: [decimal({ precision: 18, scale: 2 })] })
        .rows,
    ).toEqual([
      { p: "0.25", n: 0n },
      { p: "1.25", n: 1n },
      { p: "2.25", n: 2n },
    ]);
  });
});

describe("files DuckDB writes outside the subset", () => {
  /** Each case names the feature the error must mention. */
  const cases: readonly {
    readonly what: string;
    readonly file: string;
    readonly select: string;
    readonly options?: string;
    readonly names: string;
  }[] = [
    {
      what: "dictionary encoding, which DuckDB picks for a repetitive column",
      file: "cross-dictionary.parquet",
      select: `SELECT 'host-' || (i % 5) AS s FROM range(20000) tbl(i)`,
      names: "dictionary",
    },
    {
      what: "the SNAPPY codec",
      file: "cross-snappy.parquet",
      select: `SELECT i::BIGINT AS n FROM range(100) tbl(i)`,
      options: "COMPRESSION SNAPPY",
      names: "SNAPPY",
    },
    {
      what: "the ZSTD codec",
      file: "cross-zstd.parquet",
      select: `SELECT i::BIGINT AS n FROM range(100) tbl(i)`,
      options: "COMPRESSION ZSTD",
      names: "ZSTD",
    },
    {
      what: "a nested schema",
      file: "cross-list.parquet",
      select: `SELECT [i, i + 1] AS lst FROM range(5) tbl(i)`,
      names: "nested",
    },
    {
      what: "a struct column",
      file: "cross-struct.parquet",
      select: `SELECT {'a': i, 'b': 'x'} AS obj FROM range(5) tbl(i)`,
      names: "nested",
    },
    {
      what: "microsecond timestamps",
      file: "cross-micros.parquet",
      select: `SELECT epoch_ms(1700000000000 + i)::TIMESTAMPTZ AS t FROM range(5) tbl(i)`,
      names: "MICROS",
    },
    {
      what: "DATE, which is not one of the column types",
      file: "cross-date.parquet",
      select: `SELECT (DATE '2026-08-23' + i::INTEGER) AS d FROM range(5) tbl(i)`,
      names: "DATE",
    },
    {
      what: "DECIMAL, which is not one of the column types",
      file: "cross-decimal.parquet",
      select: `SELECT (i / 100)::DECIMAL(10, 4) AS d FROM range(5) tbl(i)`,
      names: "DECIMAL",
    },
    {
      what: "raw BYTE_ARRAY without a STRING annotation",
      file: "cross-blob.parquet",
      select: `SELECT ('x' || i)::BLOB AS blob FROM range(5) tbl(i)`,
      names: "BYTE_ARRAY",
    },
  ];

  for (const { what, file, select, options, names } of cases) {
    it(`refuses ${what}`, () => {
      const bytes = copyTo(file, select, options);
      const error = expectError("ERR_READ_UNSUPPORTED", () => readParquet(bytes));
      expect(error.message).toContain(names);
      expect(error.message).toContain("tavolato only reads the files it writes");
    });
  }
});

/**
 * The annotated half of DuckDB's output. Each of these is refused until a
 * column type claims it, and read value for value once one does — the same
 * shape of promise the codec hooks make, for the same reason: tavolato will
 * not decide what a `DECIMAL` should be in JavaScript, but it will not stand
 * in the way either.
 */
describe("files DuckDB annotates, read with a matching column type", () => {
  it("reads DATE, TIME, TIMESTAMP and UUID", () => {
    const bytes = copyTo(
      "cross-annotated.parquet",
      `SELECT
         (DATE '2026-08-24' + i::INTEGER) AS d,
         (TIME '12:34:56.789012' + INTERVAL (i) SECOND) AS t,
         (TIMESTAMP '2026-01-01 03:04:05.123456' + INTERVAL (i) SECOND) AS ts,
         (TIMESTAMPTZ '2026-01-01 03:04:05.123456+00' + INTERVAL (i) SECOND) AS tstz,
         ('b3f2c1a0-1111-4222-8333-44445555666' || i)::UUID AS u
       FROM range(3) tbl(i)`,
    );
    const types = [
      date(),
      time({ unit: "micros", isAdjustedToUTC: false }),
      timestamp({ unit: "micros", isAdjustedToUTC: false }),
      timestamp({ unit: "micros", isAdjustedToUTC: true }),
      uuid(),
    ];

    const refusal = expectError("ERR_READ_UNSUPPORTED", () => readParquet(bytes));
    expect(refusal.message).toContain("pass a matching type in ReadOptions.types");

    const { schema, rows } = readParquet(bytes, { types });
    expect(schema.columns.map((column) => (column.type as { name: string }).name)).toEqual([
      "date",
      "time(micros)",
      "timestamp(micros)",
      "timestamp(micros)",
      "uuid",
    ]);
    // The naive TIMESTAMP and TIMESTAMPTZ are claimed by distinct adapters so
    // the schema returned by the reader preserves their different meanings.
    expect(rows).toEqual(
      Array.from({ length: 3 }, (_, i) => ({
        d: new Date(Date.UTC(2026, 7, 24 + i)),
        t: 45_296_789_012n + BigInt(i) * 1_000_000n,
        ts: 1_767_236_645_123_456n + BigInt(i) * 1_000_000n,
        tstz: 1_767_236_645_123_456n + BigInt(i) * 1_000_000n,
        u: `b3f2c1a0-1111-4222-8333-44445555666${i}`,
      })),
    );
  });

  it("reads a DECIMAL at each of the three widths DuckDB stores them in", () => {
    const bytes = copyTo(
      "cross-decimals.parquet",
      `SELECT
         (i + 0.25)::DECIMAL(9, 2) AS small,
         (i + 0.25)::DECIMAL(18, 2) AS medium,
         (i + 0.25)::DECIMAL(30, 2) AS large
       FROM range(3) tbl(i)`,
    );
    const types = [
      decimal({ precision: 9, scale: 2 }),
      decimal({ precision: 18, scale: 2 }),
      decimal({ precision: 30, scale: 2 }),
    ];
    const { schema, rows } = readParquet(bytes, { types });
    // Three precisions, three physical types, one JavaScript type.
    expect(schema.columns.map((column) => column.typeLength)).toEqual([undefined, undefined, 16]);
    expect(rows).toEqual(
      Array.from({ length: 3 }, (_, i) => ({
        small: `${i}.25`,
        medium: `${i}.25`,
        large: `${i}.25`,
      })),
    );
  });

  it("reads every signed and unsigned integer width", () => {
    const bytes = copyTo(
      "cross-integers.parquet",
      `SELECT
         (i - 1)::TINYINT AS i8, (i - 1)::SMALLINT AS i16, (i - 1)::INTEGER AS i32,
         (i - 1)::BIGINT AS i64, i::UTINYINT AS u8, i::USMALLINT AS u16,
         i::UINTEGER AS u32, i::UBIGINT AS u64
       FROM range(3) tbl(i)`,
    );
    const types = [
      integer({ bitWidth: 8 }),
      integer({ bitWidth: 16 }),
      integer({ bitWidth: 8, signed: false }),
      integer({ bitWidth: 16, signed: false }),
      integer({ bitWidth: 32, signed: false }),
      integer({ bitWidth: 64, signed: false }),
    ];
    const { schema, rows } = readParquet(bytes, { types });
    // i32 and i64 keep their built-in reading: the annotation DuckDB puts on
    // them says no more than the physical type already does.
    expect(schema.columns.map((column) => column.type).slice(2, 4)).toEqual(["i32", "i64"]);
    expect(rows).toEqual(
      Array.from({ length: 3 }, (_, i) => ({
        i8: i - 1,
        i16: i - 1,
        i32: i - 1,
        i64: BigInt(i - 1),
        u8: i,
        u16: i,
        u32: i,
        u64: BigInt(i),
      })),
    );
  });

  it("reads an annotated column out of a compressed file", () => {
    const bytes = copyTo(
      "cross-annotated-gzip.parquet",
      `SELECT (DATE '2026-08-24' + i::INTEGER) AS d, (i + 0.25)::DECIMAL(18, 2) AS n
       FROM range(50) tbl(i)`,
      "COMPRESSION GZIP",
    );
    const { rows } = sync(
      readParquet(bytes, {
        codecs: { GZIP: { decompress: (page) => gunzipSync(page) } },
        types: [date(), decimal({ precision: 18, scale: 2 })],
      }),
    );
    expect(rows).toEqual(
      Array.from({ length: 50 }, (_, i) => ({
        d: new Date(Date.UTC(2026, 7, 24 + i)),
        n: `${i}.25`,
      })),
    );
  });
});
