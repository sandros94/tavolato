import { gzipSync, zstdCompressSync } from "node:zlib";
import { afterAll, describe, expect, it } from "vitest";
import { createWriter, defineSchema, TavolatoError } from "../src/index.ts";
import type { WriterCodec } from "../src/index.ts";
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
        k: BigInt(index),
        doc: `{"id":${index},"tag":"t${index}","tags":[${index},${index + 1}]}`,
        maybe: index % 2 === 0 ? null : `{"odd":${index}}`,
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
