import { describe, expect, it } from "vitest";
import { createWriter, defineSchema, isTavolatoError, TavolatoError } from "../src/index.ts";
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
      i: { type: "i64" },
      b: { type: "bool" },
      t: { type: "timestamp" },
    });
    expect(schema.columns.map((column) => column.type)).toEqual([
      "string",
      "json",
      "f64",
      "i64",
      "bool",
      "timestamp",
    ]);
  });

  it("refuses a schema with no columns", () => {
    expectError("ERR_SCHEMA_EMPTY", () => defineSchema({}));
  });

  it("refuses a non-object schema", () => {
    // @ts-expect-error deliberately wrong input
    expectError("ERR_SCHEMA_EMPTY", () => defineSchema(null));
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

describe("row validation", () => {
  const schema = defineSchema({
    s: { type: "string" },
    j: { type: "json" },
    f: { type: "f64" },
    i: { type: "i64" },
    b: { type: "bool" },
    t: { type: "timestamp" },
    opt: { type: "string", optional: true },
  });

  const valid = { s: "x", j: "{}", f: 1, i: 1n, b: true, t: 0 } as const;

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
    // A json column takes a string, and only a string: tavolato stores the text
    // it is handed and never serializes an object for you.
    ["j", { a: 1 }],
    ["j", 1],
    ["f", "1"],
    ["f", 1n],
    ["i", "1"],
    ["i", 1.5],
    ["i", Number.NaN],
    ["i", 2 ** 60],
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
