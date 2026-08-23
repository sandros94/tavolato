import { readFileSync } from "node:fs";
import { join } from "node:path";
import { afterAll, describe, expect, it } from "vitest";
import { readParquet } from "../src/index.ts";
import { cleanupTempDir, duckdb, duckdbRow, sqlPath, tempDir } from "./_duckdb.ts";
import { expectError } from "./_errors.ts";

/**
 * The other direction: DuckDB writes, tavolato reads.
 *
 * `COPY … (FORMAT PARQUET, COMPRESSION UNCOMPRESSED)` lands inside tavolato's
 * subset for the plain cases — v1 pages, PLAIN values, RLE definition levels —
 * and those files are read and compared value by value.
 *
 * DuckDB leaves that subset as soon as it has a reason to: a repetitive column
 * gets a dictionary, a compressed file gets a codec, a `TIMESTAMPTZ` gets
 * microseconds, a list gets a nested schema. Those files must not be read at
 * all; the typed error naming the feature *is* the assertion, and it is what
 * proves the scope promise is enforced rather than merely documented.
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
      what: "INT32, which is not one of the five types",
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
      what: "DATE, which is not one of the five types",
      file: "cross-date.parquet",
      select: `SELECT (DATE '2026-08-23' + i::INTEGER) AS d FROM range(5) tbl(i)`,
      names: "DATE",
    },
    {
      what: "DECIMAL, which is not one of the five types",
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
