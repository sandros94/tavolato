import { readFileSync } from "node:fs";
import { join } from "node:path";
import { gunzipSync, zstdDecompressSync } from "node:zlib";
import { afterAll, describe, expect, it } from "vitest";
import { readParquet } from "../src/index.ts";
import type { CodecName, ReaderCodec } from "../src/index.ts";
import { cleanupTempDir, duckdb, duckdbRow, sqlPath, tempDir } from "./_duckdb.ts";
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
         epoch_ms(1700000000000 + i)::TIMESTAMP_MS AS t,
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
      { name: "t", type: "timestamp", optional: true },
      { name: "o", type: "string", optional: true },
    ]);
    expect(rows).toEqual(
      Array.from({ length: 5 }, (_, i) => ({
        s: `row-${i}`,
        f: i + 0.5,
        n: BigInt(i),
        b: i % 2 === 0,
        t: new Date(1_700_000_000_000 + i),
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
    expect(rows.map((row) => ({ n: String(row.n), s: row.s }))).toEqual(reference);
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

  it("reads a JSON-annotated column as a json column of strings", () => {
    const bytes = copyTo(
      "cross-json.parquet",
      `SELECT ('{"id":' || i || ',"tag":"t' || i || '"}')::JSON AS j FROM range(5) tbl(i)`,
    );
    const { schema, rows } = readParquet(bytes);
    expect(schema.columns).toEqual([{ name: "j", type: "json", optional: true }]);
    // DuckDB stores the document as text and tavolato hands the text back; the
    // annotation is the only thing that says what the text means.
    expect(rows.map((row) => row.j)).toEqual(
      Array.from({ length: 5 }, (_, i) => `{"id":${i},"tag":"t${i}"}`),
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
    expect(rows.map((row) => row.j)).toEqual(Array.from({ length: 50 }, (_, i) => `{"id":${i}}`));
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
      what: "INT32, which is not one of the column types",
      file: "cross-int32.parquet",
      select: `SELECT i::INTEGER AS n FROM range(5) tbl(i)`,
      names: "INT_32",
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
