import { describe, expect, it } from "vitest";
import {
  createWriter,
  date,
  decimal,
  defineSchema,
  isTavolatoError,
  readParquet,
  readSchema,
  TavolatoError,
  uuid,
} from "../src/index.ts";
import type { ParquetSchema } from "../src/types.ts";
import { expectError } from "./_errors.ts";

describe("defineSchema", () => {
  it("normalizes columns in declaration order", () => {
    const schema = defineSchema({
      b: { type: "string", optional: true },
      a: { type: "i64" },
    });
    expect(schema.columns).toEqual([
      { name: "b", type: "string", optional: true },
      { name: "a", type: "i64", optional: false },
    ]);
  });

  it("accepts every supported column type", () => {
    const schema = defineSchema({
      s: { type: "string" },
      j: { type: "json" },
      f: { type: "f64" },
      g: { type: "f32" },
      i: { type: "i64" },
      n: { type: "i32" },
      b: { type: "bool" },
      t: { type: "timestamp" },
    });
    expect(schema.columns.map((column) => column.type)).toEqual([
      "string",
      "json",
      "f64",
      "f32",
      "i64",
      "i32",
      "bool",
      "timestamp",
    ]);
  });

  it("accepts a logical column type, and carries its byte width", () => {
    const id = uuid();
    const schema = defineSchema({ id: { type: id }, price: { type: decimal({ precision: 9 }) } });
    expect(schema.columns[0]).toEqual({ name: "id", type: id, optional: false, typeLength: 16 });
    // Only a FIXED_LEN_BYTE_ARRAY has a width, so nothing else grows the key.
    expect(schema.columns[1]).toEqual({
      name: "price",
      type: schema.definition.price.type,
      optional: false,
    });
    expect(Object.hasOwn(schema.columns[1], "typeLength")).toBe(false);
  });

  it("refuses an object that is not a column type", () => {
    for (const type of [
      {},
      { name: "x" },
      { name: "x", physical: "i128", matches: () => true },
      { name: "x", physical: "fixed", matches: () => true, annotate: () => ({ kind: "uuid" }) },
      { name: "x", physical: "i32", annotate: () => ({ kind: "date" }) },
    ]) {
      const error = expectError("ERR_SCHEMA_COLUMN_INVALID", () =>
        // @ts-expect-error deliberately wrong input
        defineSchema({ c: { type } }),
      );
      expect(error.column).toBe("c");
    }
  });

  it("refuses a schema with no columns", () => {
    expectError("ERR_SCHEMA_EMPTY", () => defineSchema({}));
  });

  it("refuses a non-object schema", () => {
    // @ts-expect-error deliberately wrong input
    expectError("ERR_SCHEMA_EMPTY", () => defineSchema(null));
  });

  it("refuses an array instead of a column map", () => {
    expectError("ERR_SCHEMA_EMPTY", () =>
      // @ts-expect-error deliberately wrong input
      defineSchema([{ type: "i64" }]),
    );
  });

  it("refuses an unsupported column type", () => {
    const error = expectError(
      "ERR_SCHEMA_COLUMN_INVALID",
      // @ts-expect-error deliberately wrong input
      () => defineSchema({ n: { type: "int32" } }),
    );
    expect(error.column).toBe("n");
  });

  it("refuses a malformed column definition", () => {
    // @ts-expect-error deliberately wrong input
    expectError("ERR_SCHEMA_COLUMN_INVALID", () => defineSchema({ n: "i64" }));
  });

  it("refuses a non-boolean optional flag", () => {
    expectError(
      "ERR_SCHEMA_COLUMN_INVALID",
      // @ts-expect-error deliberately wrong input
      () => defineSchema({ n: { type: "i64", optional: "yes" } }),
    );
  });

  it("refuses an empty column name", () => {
    expectError("ERR_SCHEMA_COLUMN_INVALID", () => defineSchema({ "": { type: "i64" } }));
  });
});

describe("writer schema validation", () => {
  const forged = (value: unknown): ParquetSchema => value as ParquetSchema;
  const adapterSchema = (annotate: () => { readonly kind: "date" | "uuid" }): ParquetSchema =>
    forged({
      columns: [
        {
          name: "n",
          type: {
            name: "stateful",
            physical: "i32",
            matches: () => false,
            annotate,
            read: (raw: unknown) => raw,
            write: (value: unknown) => value,
          },
          optional: false,
        },
      ],
    });

  it("refuses the empty structural schema before it can emit a file", () => {
    expectError("ERR_SCHEMA_EMPTY", () => createWriter(forged({ columns: [], definition: {} })));
  });

  it.each([
    ["a non-object schema", null, "ERR_SCHEMA_EMPTY", undefined],
    ["missing columns", { definition: {} }, "ERR_SCHEMA_EMPTY", undefined],
    ["non-array columns", { columns: {}, definition: {} }, "ERR_SCHEMA_COLUMN_INVALID", undefined],
    [
      "a malformed column",
      { columns: [null], definition: {} },
      "ERR_SCHEMA_COLUMN_INVALID",
      undefined,
    ],
    [
      "a non-string name",
      { columns: [{ name: 1, type: "i64", optional: false }], definition: {} },
      "ERR_SCHEMA_COLUMN_INVALID",
      undefined,
    ],
    [
      "an empty name",
      { columns: [{ name: "", type: "i64", optional: false }], definition: {} },
      "ERR_SCHEMA_COLUMN_INVALID",
      "",
    ],
    [
      "duplicate names",
      {
        columns: [
          { name: "n", type: "i64", optional: false },
          { name: "n", type: "i32", optional: false },
        ],
        definition: {},
      },
      "ERR_SCHEMA_COLUMN_INVALID",
      "n",
    ],
    [
      "an unsupported built-in",
      { columns: [{ name: "n", type: "int64", optional: false }], definition: {} },
      "ERR_SCHEMA_COLUMN_INVALID",
      "n",
    ],
    [
      "a bigint type",
      { columns: [{ name: "n", type: 1n, optional: false }], definition: {} },
      "ERR_SCHEMA_COLUMN_INVALID",
      "n",
    ],
    [
      "a malformed adapter",
      {
        columns: [
          {
            name: "n",
            type: { name: "broken", physical: "i64" },
            optional: false,
          },
        ],
        definition: {},
      },
      "ERR_SCHEMA_COLUMN_INVALID",
      "n",
    ],
    [
      "a non-boolean normalized optional flag",
      { columns: [{ name: "n", type: "i64", optional: "yes" }], definition: {} },
      "ERR_SCHEMA_COLUMN_INVALID",
      "n",
    ],
  ] as const)("refuses %s", (_, schema, code, column) => {
    const error = expectError(code, () => createWriter(forged(schema)));
    expect(error.column).toBe(column);
  });

  it("uses columns only and preserves the schema object", () => {
    const schema = forged({
      columns: [
        { name: "10", type: "i64", optional: false },
        { name: "2", type: "string", optional: false },
      ],
    });
    const writer = createWriter(schema);
    expect(writer.schema).toBe(schema);
    writer.append({ "10": 10n, "2": "two" });
    expect(readSchema(writer.finish()).columns.map((column) => column.name)).toEqual(["10", "2"]);
  });

  it("inspects an adapter's annotation exactly once while creating a writer", () => {
    let calls = 0;
    const writer = createWriter(
      adapterSchema(() => {
        calls++;
        if (calls > 1) throw new Error("called twice");
        return { kind: "date" };
      }),
    );
    expect(calls).toBe(1);
    writer.append({ n: 1 });
    expect(() => writer.finish()).not.toThrow();
  });

  it("types an adapter annotation failure at the writer boundary", () => {
    const error = expectError("ERR_SCHEMA_COLUMN_INVALID", () =>
      createWriter(
        adapterSchema(() => {
          throw new Error("no annotation");
        }),
      ),
    );
    expect(error.column).toBe("n");
    expect(error.message).toContain("no annotation");
  });

  it("writes the same annotation it validated when an adapter alternates answers", () => {
    let calls = 0;
    const writer = createWriter(
      adapterSchema(() => (++calls === 1 ? { kind: "date" } : { kind: "uuid" })),
    );
    writer.append({ n: 1 });
    const days = date({ as: "number" });
    expect(readParquet(writer.finish(), { types: [days] }).rows).toEqual([{ n: 1 }]);
    expect(calls).toBe(1);
  });

  it("reads a structural schema's columns exactly once", () => {
    let reads = 0;
    const columns = [{ name: "n", type: "i64", optional: false }] as const;
    const schema = forged({
      get columns(): unknown {
        reads++;
        return reads === 1 ? columns : null;
      },
    });
    const writer = createWriter(schema);
    writer.append({ n: 1n });
    expect(readParquet(writer.finish()).rows).toEqual([{ n: 1n }]);
    expect(reads).toBe(1);
  });

  it("owns the normalized column name it validated", () => {
    let reads = 0;
    const schema = forged({
      columns: [
        {
          get name(): string {
            reads++;
            return reads === 1 ? "n" : "changed";
          },
          type: "i64",
          optional: false,
        },
      ],
    });
    const writer = createWriter(schema);
    writer.append({ n: 1n });
    expect(readParquet(writer.finish()).rows).toEqual([{ n: 1n }]);
    expect(reads).toBe(1);
  });

  it("owns the normalized optional flag it validated", () => {
    let reads = 0;
    const schema = forged({
      columns: [
        {
          name: "n",
          type: "i64",
          get optional(): boolean {
            reads++;
            return reads === 1;
          },
        },
      ],
    });
    const writer = createWriter(schema);
    writer.append({ n: null });
    expect(readParquet(writer.finish()).rows).toEqual([{ n: null }]);
    expect(reads).toBe(1);
  });

  it("owns the annotation object returned during validation", () => {
    const annotation: { kind: "date" | "uuid" } = { kind: "date" };
    const writer = createWriter(adapterSchema(() => annotation));
    annotation.kind = "uuid";
    writer.append({ n: 1 });
    const days = date({ as: "number" });
    expect(readParquet(writer.finish(), { types: [days] }).rows).toEqual([{ n: 1 }]);
  });
});

describe("row validation", () => {
  const schema = defineSchema({
    s: { type: "string" },
    j: { type: "json" },
    f: { type: "f64" },
    g: { type: "f32" },
    i: { type: "i64" },
    n: { type: "i32" },
    b: { type: "bool" },
    t: { type: "timestamp" },
    opt: { type: "string", optional: true },
  });

  const valid = { s: "x", j: { ok: true }, f: 1, g: 1, i: 1n, n: 1, b: true, t: 0 } as const;

  it("accepts a well-formed row", () => {
    const writer = createWriter(schema);
    expect(() => writer.append({ ...valid })).not.toThrow();
    expect(writer.rowCount).toBe(1);
  });

  it("rejects an unknown column", () => {
    const writer = createWriter(schema);
    // @ts-expect-error deliberately wrong input
    const error = expectError("ERR_ROW_UNKNOWN_COLUMN", () => writer.append({ ...valid, zz: 1 }));
    expect(error.column).toBe("zz");
  });

  it("rejects a missing required column", () => {
    const writer = createWriter(schema);
    const { i: _omitted, ...rest } = valid;
    // @ts-expect-error deliberately wrong input
    const error = expectError("ERR_ROW_VALUE_MISSING", () => writer.append(rest));
    expect(error.column).toBe("i");
  });

  it("rejects an explicit null in a required column", () => {
    const writer = createWriter(schema);
    // @ts-expect-error deliberately wrong input
    expectError("ERR_ROW_VALUE_MISSING", () => writer.append({ ...valid, s: null }));
  });

  it("rejects a row that is not an object", () => {
    const writer = createWriter(schema);
    // @ts-expect-error deliberately wrong input
    expectError("ERR_ROW_NOT_AN_OBJECT", () => writer.append("nope"));
    // @ts-expect-error deliberately wrong input
    expectError("ERR_ROW_NOT_AN_OBJECT", () => writer.append([1, 2]));
  });

  it.each([
    ["s", 1],
    // A json column takes any JSON document — the objects and arrays included —
    // so what it refuses is what JSON itself has no spelling for.
    ["j", 1n],
    ["j", { big: 1n }],
    ["j", () => 1],
    ["j", Symbol("nope")],
    ["f", "1"],
    ["f", 1n],
    ["g", "1"],
    ["g", 1n],
    ["i", "1"],
    ["i", 1.5],
    ["i", Number.NaN],
    ["i", 2 ** 60],
    ["n", "1"],
    ["n", 1n],
    ["n", 1.5],
    ["n", Number.NaN],
    ["n", 2 ** 31],
    ["n", -(2 ** 31) - 1],
    ["b", 1],
    ["t", "2024-01-01"],
    ["t", 1.5],
    ["t", new Date("nope")],
    ["opt", 1],
  ])("rejects %s = %o", (column, value) => {
    const writer = createWriter(schema);
    const row = { ...valid, [column]: value } as unknown as Parameters<typeof writer.append>[0];
    const error = expectError("ERR_ROW_VALUE_INVALID", () => writer.append(row));
    expect(error.column).toBe(column);
  });

  it("rejects bigints outside the signed 64-bit range", () => {
    const writer = createWriter(schema);
    expectError("ERR_ROW_VALUE_INVALID", () => writer.append({ ...valid, i: 2n ** 63n }));
    expectError("ERR_ROW_VALUE_INVALID", () => writer.append({ ...valid, i: -(2n ** 63n) - 1n }));
  });

  it("accepts the signed 64-bit extremes", () => {
    const writer = createWriter(schema);
    expect(() => writer.append({ ...valid, i: 2n ** 63n - 1n })).not.toThrow();
    expect(() => writer.append({ ...valid, i: -(2n ** 63n) })).not.toThrow();
  });

  it("accepts the signed 32-bit extremes", () => {
    const writer = createWriter(schema);
    expect(() => writer.append({ ...valid, n: 2 ** 31 - 1 })).not.toThrow();
    expect(() => writer.append({ ...valid, n: -(2 ** 31) })).not.toThrow();
  });

  it("rejects epoch millis a Date cannot hold", () => {
    // A `timestamp` column reads back as a `Date`, and a `Date` stops at
    // ±8.64e15 milliseconds. Accepting a count past that writes a value whose
    // only possible reading is an Invalid Date.
    const writer = createWriter(schema);
    for (const t of [8_640_000_000_000_001, -8_640_000_000_000_001, 9e15]) {
      const error = expectError("ERR_ROW_VALUE_INVALID", () => writer.append({ ...valid, t }));
      expect(error.column).toBe("t");
    }
    // The extremes themselves are exactly what a Date holds, and stay legal.
    expect(() => writer.append({ ...valid, t: 8_640_000_000_000_000 })).not.toThrow();
    expect(() => writer.append({ ...valid, t: -8_640_000_000_000_000 })).not.toThrow();
  });

  it("leaves the writer untouched when a row is rejected", () => {
    const writer = createWriter(schema);
    writer.append({ ...valid });
    // @ts-expect-error deliberately wrong input
    expect(() => writer.append({ ...valid, b: "no" })).toThrow(TavolatoError);
    expect(writer.rowCount).toBe(1);
  });
});

describe("writer options", () => {
  const schema = defineSchema({ n: { type: "i64" } });

  it.each([0, -1, 1.5, Number.NaN])("rejects rowGroupSize %o", (rowGroupSize) => {
    expectError("ERR_WRITER_OPTION_INVALID", () => createWriter(schema, { rowGroupSize }));
  });

  it("rejects a non-string createdBy", () => {
    // @ts-expect-error deliberately wrong input
    expectError("ERR_WRITER_OPTION_INVALID", () => createWriter(schema, { createdBy: 1 }));
  });
});

describe("isTavolatoError", () => {
  it("rejects foreign errors", () => {
    expect(isTavolatoError(new Error("nope"))).toBe(false);
    expect(isTavolatoError(new Error("nope"), "ERR_SCHEMA_EMPTY")).toBe(false);
  });

  it("narrows without a code", () => {
    expect(isTavolatoError(new TavolatoError("x", "ERR_SCHEMA_EMPTY"))).toBe(true);
  });

  it("does not match a different code", () => {
    expect(isTavolatoError(new TavolatoError("x", "ERR_SCHEMA_EMPTY"), "ERR_WRITER_FINISHED")).toBe(
      false,
    );
  });
});
