import { describe, expect, it } from "vitest";
import {
  createWriter,
  date,
  decimal,
  defineColumnType,
  defineSchema,
  float16,
  integer,
  json,
  readParquet,
  TavolatoError,
  time,
  timestamp,
  uuid,
} from "../src/index.ts";
import type {
  Annotation,
  AnyLogicalAdapter,
  LogicalAdapter,
  PhysicalKind,
  ReadRow,
} from "../src/index.ts";
import { expectError } from "./_errors.ts";
import { sync } from "./_sync.ts";

const NON_OBJECT_OPTIONS: readonly unknown[] = [
  null,
  false,
  0,
  1n,
  "options",
  Symbol("options"),
  () => undefined,
];

describe("adapter option bags", () => {
  it.each([
    ["date", (options: unknown) => date(options as never)],
    ["decimal", (options: unknown) => decimal(options as never)],
    ["time", (options: unknown) => time(options as never)],
    ["timestamp", (options: unknown) => timestamp(options as never)],
    ["float16", (options: unknown) => float16(options as never)],
    ["integer", (options: unknown) => integer(options as never)],
    ["json", (options: unknown) => json(options as never)],
  ])("refuses every non-object passed to %s", (_, factory) => {
    for (const options of NON_OBJECT_OPTIONS) {
      expectError("ERR_SCHEMA_COLUMN_INVALID", () => factory(options));
    }
  });

  it.each([
    ["decimal", () => decimal(undefined as never)],
    ["time", () => time(undefined as never)],
    ["timestamp", () => timestamp(undefined as never)],
    ["integer", () => integer(undefined as never)],
  ])("refuses omitted required %s options", (_, factory) => {
    expectError("ERR_SCHEMA_COLUMN_INVALID", factory);
  });
});

/**
 * Logical column types: the seam where tavolato hands a decision back rather
 * than guessing at it.
 *
 * Three things are being tested here. That the *system* holds a caller's
 * functions to their word on both sides, the way the codec hooks are held to
 * theirs. That claiming is predictable: registration order decides, built-in
 * types keep the bare physical types, and adapters own the annotations. And
 * that the in-box types earn the mapping they claim — every value that goes in
 * comes back identical, or is refused before it can come back as something
 * else.
 */

/** Writes one column of `values` and returns the file. */
function write<TIn>(type: LogicalAdapter<TIn, unknown>, values: TIn[]): Uint8Array {
  const schema = defineSchema({ v: { type } });
  const writer = createWriter(schema);
  for (const value of values) writer.append({ v: value } as never);
  return sync(writer.finish());
}

/** Writes one column, reads it back with the same type, and returns the values. */
function roundtrip<TIn, TOut>(type: LogicalAdapter<TIn, TOut>, values: TIn[]): TOut[] {
  const { rows } = readParquet(write(type, values), { types: [type as AnyLogicalAdapter] });
  return rows.map((row) => row.v as TOut);
}

/** Appends one value to a fresh writer, returning whatever it throws. */
function appendOne(type: AnyLogicalAdapter, value: unknown): () => unknown {
  const writer = createWriter(defineSchema({ v: { type } }));
  return () => writer.append({ v: value } as never);
}

/** Minimal custom type used to probe an annotation's legal storage contract. */
function annotatedAs(
  annotation: Annotation,
  physical: PhysicalKind,
  typeLength?: number,
): LogicalAdapter<unknown, unknown> {
  return defineColumnType({
    name: `${annotation.kind}-${physical}`,
    physical,
    ...(typeLength === undefined ? {} : { typeLength }),
    matches: (found) => found.kind === annotation.kind,
    annotate: () => annotation,
    read: (raw) => raw,
    write: (value) => value,
  });
}

describe("defineColumnType", () => {
  const valid = {
    name: "flag",
    physical: "bool",
    matches: (annotation: Annotation) => annotation.kind === "none",
    annotate: (): Annotation => ({ kind: "none" }),
    read: (raw: unknown) => raw as boolean,
    write: (value: boolean) => value,
  } as const;

  it("returns the column type, frozen", () => {
    const type = defineColumnType({ ...valid });
    expect(Object.isFrozen(type)).toBe(true);
    expect(type.name).toBe("flag");
  });

  it("refuses an annotation on a physical type Parquet does not permit", () => {
    const error = expectError("ERR_SCHEMA_COLUMN_INVALID", () =>
      defineColumnType({
        ...valid,
        physical: "i64",
        annotate: (): Annotation => ({ kind: "date" }),
      }),
    );
    expect(error.message).toContain("DATE");
    expect(error.message).toContain("INT32");
  });

  it.each([
    [{ kind: "string" }, "bytes"],
    [{ kind: "enum" }, "bytes"],
    [{ kind: "json" }, "bytes"],
    [{ kind: "bson" }, "bytes"],
    [{ kind: "uuid" }, "fixed", 16],
    [{ kind: "date" }, "i32"],
    [{ kind: "float16" }, "fixed", 2],
    [{ kind: "time", unit: "millis", isAdjustedToUTC: false }, "i32"],
    [{ kind: "time", unit: "micros", isAdjustedToUTC: true }, "i64"],
    [{ kind: "time", unit: "nanos", isAdjustedToUTC: false }, "i64"],
    [{ kind: "timestamp", unit: "millis", isAdjustedToUTC: true }, "i64"],
    [{ kind: "timestamp", unit: "micros", isAdjustedToUTC: false }, "i64"],
    [{ kind: "timestamp", unit: "nanos", isAdjustedToUTC: true }, "i64"],
    [{ kind: "integer", bitWidth: 8, isSigned: true }, "i32"],
    [{ kind: "integer", bitWidth: 16, isSigned: false }, "i32"],
    [{ kind: "integer", bitWidth: 32, isSigned: true }, "i32"],
    [{ kind: "integer", bitWidth: 64, isSigned: false }, "i64"],
    [{ kind: "decimal", precision: 9, scale: 2 }, "i32"],
    [{ kind: "decimal", precision: 18, scale: 2 }, "i64"],
    [{ kind: "decimal", precision: 100, scale: 2 }, "bytes"],
    [{ kind: "decimal", precision: 100, scale: 2 }, "fixed", 42],
    [{ kind: "decimal", precision: 311_620_928, scale: 2 }, "fixed", 129_397_790],
  ] as [Annotation, PhysicalKind, number?][])(
    "accepts %o on %s",
    (annotation, physical, typeLength) => {
      expect(annotatedAs(annotation, physical, typeLength).physical).toBe(physical);
    },
  );

  it.each([
    [{ kind: "string" }, "fixed", 4],
    [{ kind: "enum" }, "i32"],
    [{ kind: "json" }, "i64"],
    [{ kind: "bson" }, "fixed", 4],
    [{ kind: "uuid" }, "fixed", 15],
    [{ kind: "date" }, "i64"],
    [{ kind: "float16" }, "fixed", 4],
    [{ kind: "time", unit: "millis", isAdjustedToUTC: false }, "i64"],
    [{ kind: "time", unit: "micros", isAdjustedToUTC: true }, "i32"],
    [{ kind: "time", unit: "nanos", isAdjustedToUTC: false }, "i32"],
    [{ kind: "timestamp", unit: "millis", isAdjustedToUTC: true }, "i32"],
    [{ kind: "integer", bitWidth: 8, isSigned: true }, "i64"],
    [{ kind: "integer", bitWidth: 16, isSigned: false }, "i64"],
    [{ kind: "integer", bitWidth: 32, isSigned: true }, "i64"],
    [{ kind: "integer", bitWidth: 64, isSigned: false }, "i32"],
    [{ kind: "decimal", precision: 10, scale: 2 }, "i32"],
    [{ kind: "decimal", precision: 19, scale: 2 }, "i64"],
    [{ kind: "decimal", precision: 5, scale: 2 }, "fixed", 2],
    [{ kind: "decimal", precision: 311_620_929, scale: 2 }, "fixed", 129_397_790],
    [{ kind: "decimal", precision: 3, scale: 2 }, "f32"],
  ] as [Annotation, PhysicalKind, number?][])(
    "refuses %o on %s",
    (annotation, physical, typeLength) => {
      expectError("ERR_SCHEMA_COLUMN_INVALID", () => annotatedAs(annotation, physical, typeLength));
    },
  );

  it.each([
    ["not an object", null],
    ["a nameless type", { ...valid, name: "" }],
    ["a physical type that does not exist", { ...valid, physical: "i128" }],
    ["a fixed type with no width", { ...valid, physical: "fixed" }],
    ["a fixed type with a fractional width", { ...valid, physical: "fixed", typeLength: 1.5 }],
    ["a width on a type that is not fixed", { ...valid, typeLength: 4 }],
    ["a missing matches()", { ...valid, matches: undefined }],
    ["a missing annotate()", { ...valid, annotate: undefined }],
    ["a missing read()", { ...valid, read: undefined }],
    ["a missing write()", { ...valid, write: undefined }],
    ["a non-function acceptsPhysical", { ...valid, acceptsPhysical: ["i32"] }],
    [
      "an annotate() that throws",
      {
        ...valid,
        annotate: () => {
          throw new Error("nope");
        },
      },
    ],
    ["an annotate() that returns nothing", { ...valid, annotate: () => undefined }],
    ["an annotation nobody can write", { ...valid, annotate: () => ({ kind: "unknown", id: 99 }) }],
    ["a decimal without parameters", { ...valid, annotate: () => ({ kind: "decimal" }) }],
    [
      "a decimal of negative precision",
      {
        ...valid,
        physical: "bytes",
        annotate: () => ({ kind: "decimal", precision: -3, scale: 999 }),
      },
    ],
    [
      "a decimal precision larger than its i32 metadata field",
      {
        ...valid,
        physical: "bytes",
        annotate: () => ({ kind: "decimal", precision: 2 ** 31, scale: 0 }),
      },
    ],
    [
      "a decimal scaled past its own precision",
      {
        ...valid,
        physical: "bytes",
        annotate: () => ({ kind: "decimal", precision: 4, scale: 5 }),
      },
    ],
    ["a timestamp without a unit", { ...valid, annotate: () => ({ kind: "timestamp" }) }],
    [
      "an integer of no known width",
      {
        ...valid,
        annotate: () => ({ kind: "integer", bitWidth: 24, isSigned: true }),
      },
    ],
  ])("refuses %s", (_what, spec) => {
    // @ts-expect-error deliberately wrong input
    expectError("ERR_SCHEMA_COLUMN_INVALID", () => defineColumnType(spec));
  });

  it("refuses options the in-box types cannot honour", () => {
    expectError("ERR_SCHEMA_COLUMN_INVALID", () => decimal({ precision: 0 }));
    expectError("ERR_SCHEMA_COLUMN_INVALID", () => decimal({ precision: 2 ** 31 }));
    expectError("ERR_SCHEMA_COLUMN_INVALID", () => decimal({ precision: 4, scale: 5 }));
    expectError("ERR_SCHEMA_COLUMN_INVALID", () => decimal({ precision: 4, scale: -1 }));
    expectError("ERR_SCHEMA_COLUMN_INVALID", () =>
      time({
        // @ts-expect-error deliberately wrong input
        unit: "seconds",
        isAdjustedToUTC: false,
      }),
    );
    expectError("ERR_SCHEMA_COLUMN_INVALID", () =>
      timestamp({
        // @ts-expect-error deliberately wrong input
        unit: "seconds",
        isAdjustedToUTC: true,
      }),
    );
    // @ts-expect-error deliberately missing the required Parquet parameter
    expectError("ERR_SCHEMA_COLUMN_INVALID", () => time({ unit: "millis" }));
    // @ts-expect-error deliberately missing the required Parquet parameter
    expectError("ERR_SCHEMA_COLUMN_INVALID", () => timestamp({ unit: "millis" }));
    // @ts-expect-error deliberately wrong input
    expectError("ERR_SCHEMA_COLUMN_INVALID", () => integer({ bitWidth: 24 }));
  });

  it("stores a decimal where DuckDB stores it, by precision", () => {
    expect(decimal({ precision: 1 }).physical).toBe("i32");
    expect(decimal({ precision: 9 }).physical).toBe("i32");
    expect(decimal({ precision: 10 }).physical).toBe("i64");
    expect(decimal({ precision: 18 }).physical).toBe("i64");
    expect(decimal({ precision: 19 }).physical).toBe("fixed");
    expect(decimal({ precision: 38 }).typeLength).toBe(16);
  });
});

describe("claiming a column", () => {
  it("takes the first type that matches, in registration order", () => {
    const first = defineColumnType({
      name: "first",
      physical: "i32",
      matches: (annotation) => annotation.kind === "date",
      annotate: (): Annotation => ({ kind: "date" }),
      read: () => "first",
      write: () => 0,
    });
    const second = defineColumnType({ ...first, name: "second", read: () => "second" });
    const bytes = write(date(), [new Date(0)]);

    expect(readParquet(bytes, { types: [first, second] }).rows[0].v).toBe("first");
    expect(readParquet(bytes, { types: [second, first] }).rows[0].v).toBe("second");
    // The in-box type is just another entry in that list.
    expect(readParquet(bytes, { types: [date(), first] }).rows[0].v).toEqual(new Date(0));
  });

  it("leaves the bare physical types to the built-ins", () => {
    // integer(64) claims INTEGER(64, signed) columns, and only those: a bare
    // INT64 is what `i64` means, and no adapter takes it away.
    const writer = createWriter(defineSchema({ n: { type: "i64" }, m: { type: "i32" } }));
    writer.append({ n: 7n, m: 7 });
    const bytes = sync(writer.finish());
    const { schema, rows } = readParquet(bytes, {
      types: [integer({ bitWidth: 64 }), integer({ bitWidth: 32 })],
    });
    expect(schema.columns.map((column) => column.type)).toEqual(["i64", "i32"]);
    expect(rows[0]).toEqual({ n: 7n, m: 7 });
  });

  it("gives the annotated ones to the adapter that asked for them", () => {
    const signed32 = integer({ bitWidth: 32 });
    const bytes = write(signed32, [7]);
    // Unclaimed, the INTEGER(32, signed) annotation says no more than the bare
    // INT32 does, so the built-in reads it — that leniency stays.
    expect(readParquet(bytes).schema.columns[0].type).toBe("i32");
    // Claimed, the adapter wins.
    expect(readParquet(bytes, { types: [signed32] }).schema.columns[0].type).toBe(signed32);
  });

  it("does not claim a fixed column of a different width", () => {
    const bytes = write(uuid(), ["b3f2c1a0-1111-4222-8333-444455556666"]);
    const narrow = defineColumnType({
      name: "half-a-uuid",
      physical: "fixed",
      typeLength: 8,
      matches: (annotation) => annotation.kind === "uuid",
      annotate: (): Annotation => ({ kind: "none" }),
      read: (raw) => raw as Uint8Array,
      write: (value: Uint8Array) => value,
    });
    const error = expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(bytes, { types: [narrow] }),
    );
    expect(error.message).toContain("FIXED_LEN_BYTE_ARRAY(16)");
  });

  it("keeps exact physical matching unless a type explicitly widens it", () => {
    const annotation = { kind: "decimal", precision: 9, scale: 0 } as const;
    const canonical = write(annotatedAs(annotation, "i64"), [7n]);
    const alternate = write(annotatedAs(annotation, "i32"), [7]);
    const exact = defineColumnType({
      name: "exact",
      physical: "i64",
      matches: (found) => found.kind === "decimal" && found.precision === 9 && found.scale === 0,
      annotate: (): Annotation => annotation,
      read: (raw) => raw,
      write: (value: bigint) => value,
    });
    let widenedCalls = 0;
    const widened = defineColumnType({
      ...exact,
      name: "widened",
      acceptsPhysical: (physical: PhysicalKind, typeLength: number | undefined) => {
        widenedCalls++;
        return physical === "i32" && typeLength === undefined;
      },
    });

    expect(readParquet(canonical, { types: [exact] }).schema.columns[0].type).toBe(exact);
    expectError("ERR_READ_UNSUPPORTED", () => readParquet(alternate, { types: [exact] }));
    expect(readParquet(canonical, { types: [widened] }).schema.columns[0].type).toBe(widened);
    expect(readParquet(alternate, { types: [widened] }).schema.columns[0].type).toBe(widened);
    expect(widenedCalls).toBe(1);
  });

  it("carries values over every physical type there is", () => {
    // One column type per physical kind, each mapping to a JavaScript value
    // the built-in types would never produce.
    const kinds = {
      bool: defineColumnType({
        name: "yes-no",
        physical: "bool",
        matches: (annotation) => annotation.kind === "none",
        annotate: (): Annotation => ({ kind: "none" }),
        read: (raw) => ((raw as boolean) ? "yes" : "no"),
        write: (value: string) => value === "yes",
      }),
      f64: defineColumnType({
        name: "percent",
        physical: "f64",
        matches: (annotation) => annotation.kind === "none",
        annotate: (): Annotation => ({ kind: "none" }),
        read: (raw) => `${(raw as number) * 100}%`,
        write: (value: string) => Number.parseFloat(value) / 100,
      }),
      f32: defineColumnType({
        name: "rough",
        physical: "f32",
        matches: (annotation) => annotation.kind === "none",
        annotate: (): Annotation => ({ kind: "none" }),
        read: (raw) => `${raw as number}`,
        write: (value: string) => Number.parseFloat(value),
      }),
      bytes: defineColumnType({
        name: "hex",
        physical: "bytes",
        matches: (annotation) => annotation.kind === "bson",
        annotate: (): Annotation => ({ kind: "bson" }),
        read: (raw) =>
          [...(raw as Uint8Array)].map((byte) => byte.toString(16).padStart(2, "0")).join(""),
        write: (value: string) =>
          new Uint8Array(value.match(/../g)?.map((byte) => Number.parseInt(byte, 16)) ?? []),
      }),
    };
    const schema = defineSchema({
      flag: { type: kinds.bool },
      share: { type: kinds.f64 },
      rough: { type: kinds.f32 },
      hex: { type: kinds.bytes },
    });
    const writer = createWriter(schema);
    writer.append({ flag: "yes", share: "25%", rough: "0.5", hex: "0f10" });
    writer.append({ flag: "no", share: "50%", rough: "-2", hex: "" });
    const { rows } = readParquet(sync(writer.finish()), { types: Object.values(kinds) });

    expect(rows).toEqual([
      { flag: "yes", share: "25%", rough: "0.5", hex: "0f10" },
      { flag: "no", share: "50%", rough: "-2", hex: "" },
    ]);
  });

  it("lets a type claim an unannotated column the built-ins have no reading for", () => {
    // A raw BYTE_ARRAY has no built-in meaning, and `ReadValue`'s Uint8Array
    // member stays reserved — so this is how you get one: by saying what it is.
    const binary = defineColumnType({
      name: "binary",
      physical: "bytes",
      matches: (annotation) => annotation.kind === "none",
      annotate: (): Annotation => ({ kind: "none" }),
      read: (raw) => [...(raw as Uint8Array)],
      write: (value: number[]) => new Uint8Array(value),
    });
    const bytes = write(binary, [[1, 2, 3], []]);
    expect(readParquet(bytes, { types: [binary] }).rows.map((row) => row.v)).toEqual([
      [1, 2, 3],
      [],
    ]);

    const error = expectError("ERR_READ_UNSUPPORTED", () => readParquet(bytes));
    expect(error.message).toContain("an unannotated BYTE_ARRAY");
    expect(error.message).toContain("ReadOptions.types");
  });

  it("names an unannotated fixed-width column by its width, and offers the remedy", () => {
    const quad = defineColumnType({
      name: "quad",
      physical: "fixed",
      typeLength: 4,
      matches: (annotation) => annotation.kind === "none",
      annotate: (): Annotation => ({ kind: "none" }),
      read: (raw) => [...(raw as Uint8Array)],
      write: (value: number[]) => new Uint8Array(value),
    });
    const bytes = write(quad, [[1, 2, 3, 4]]);
    expect(readParquet(bytes, { types: [quad] }).rows[0].v).toEqual([1, 2, 3, 4]);

    // Without it there is nothing to read those four bytes *as*, and tavolato
    // hands back neither a guess nor the raw bytes.
    const error = expectError("ERR_READ_UNSUPPORTED", () => readParquet(bytes));
    expect(error.message).toContain("an unannotated FIXED_LEN_BYTE_ARRAY(4)");
    expect(error.message).toContain("pass a matching type in ReadOptions.types");
  });

  it("refuses a types option that is not made of column types", () => {
    const bytes = write(uuid(), ["b3f2c1a0-1111-4222-8333-444455556666"]);
    // @ts-expect-error deliberately wrong input
    expectError("ERR_READ_OPTION_INVALID", () => readParquet(bytes, { types: "uuid" }));
    const error = expectError("ERR_READ_OPTION_INVALID", () =>
      // @ts-expect-error deliberately wrong input
      readParquet(bytes, { types: [uuid(), { name: "half" }] }),
    );
    expect(error.message).toContain("ReadOptions.types[1]");
  });

  it("blames the option, not the file, when matches() throws", () => {
    const angry = defineColumnType({
      name: "angry",
      physical: "fixed",
      typeLength: 16,
      matches: () => {
        throw new Error("no idea");
      },
      annotate: (): Annotation => ({ kind: "uuid" }),
      read: (raw) => raw as Uint8Array,
      write: (value: Uint8Array) => value,
    });
    const bytes = write(uuid(), ["b3f2c1a0-1111-4222-8333-444455556666"]);
    const error = expectError("ERR_READ_OPTION_INVALID", () =>
      readParquet(bytes, { types: [angry] }),
    );
    expect(error.message).toContain("angry");
    expect(error.column).toBe("v");
    expect((error.cause as Error).message).toBe("no idea");
  });

  it("holds acceptsPhysical() to a boolean answer and typed failures", () => {
    const annotation = { kind: "decimal", precision: 9, scale: 0 } as const;
    const bytes = write(annotatedAs(annotation, "i32"), [7]);
    const returning = defineColumnType({
      name: "returning",
      physical: "i64",
      acceptsPhysical: () => "yes" as never,
      matches: () => true,
      annotate: (): Annotation => annotation,
      read: (raw) => raw,
      write: (value: bigint) => value,
    });
    const throwing = defineColumnType({
      ...returning,
      name: "throwing",
      acceptsPhysical: () => {
        throw new Error("no layout");
      },
    });

    const returned = expectError("ERR_READ_OPTION_INVALID", () =>
      readParquet(bytes, { types: [returning] }),
    );
    expect(returned.message).toContain('returned "yes" from acceptsPhysical()');
    const thrown = expectError("ERR_READ_OPTION_INVALID", () =>
      readParquet(bytes, { types: [throwing] }),
    );
    expect((thrown.cause as Error).message).toBe("no layout");
  });
});

describe("a column type that misbehaves", () => {
  const thrower = defineColumnType({
    name: "thrower",
    physical: "i64",
    matches: (annotation) => annotation.kind === "none",
    annotate: (): Annotation => ({ kind: "none" }),
    read: (raw) => {
      if (raw === 13n) throw new Error("unlucky");
      return raw as bigint;
    },
    write: (value: bigint) => {
      if (value === 13n) throw new Error("unlucky");
      return value;
    },
  });

  it("turns a throwing write into a typed error, and rejects only that row", () => {
    const writer = createWriter(defineSchema({ k: { type: "i64" }, v: { type: thrower } }));
    writer.append({ k: 1n, v: 1n });
    const error = expectError("ERR_ROW_VALUE_INVALID", () => writer.append({ k: 2n, v: 13n }));
    expect(error.column).toBe("v");
    expect(error.message).toContain("thrower");
    expect(error.message).toContain("unlucky");
    expect((error.cause as Error).message).toBe("unlucky");
    // The rejected row left the writer exactly as it was.
    expect(writer.rowCount).toBe(1);
    writer.append({ k: 3n, v: 3n });
    expect(readParquet(sync(writer.finish()), { types: [thrower] }).rows).toEqual([
      { k: 1n, v: 1n },
      { k: 3n, v: 3n },
    ]);
  });

  it("turns a throwing read into a malformed file, naming the column", () => {
    // The value is written through a type that does not mind it, and read
    // through one that does.
    const meek = defineColumnType({ ...thrower, name: "meek", write: (value: bigint) => value });
    const bytes = write(meek, [13n]);
    const error = expectError("ERR_READ_MALFORMED", () => readParquet(bytes, { types: [thrower] }));
    expect(error.column).toBe("v");
    expect(error.message).toContain("thrower");
    expect((error.cause as Error).message).toBe("unlucky");
  });

  it("still treats a caller's unsupported error as a failed adapter", () => {
    const typed = defineColumnType({
      ...thrower,
      name: "typed-thrower",
      read: (): bigint => {
        throw new TavolatoError("not mine", "ERR_READ_UNSUPPORTED");
      },
    });
    const bytes = write(thrower, [1n]);
    const error = expectError("ERR_READ_MALFORMED", () => readParquet(bytes, { types: [typed] }));
    expect(error.cause).toBeInstanceOf(TavolatoError);
    expect((error.cause as TavolatoError).code).toBe("ERR_READ_UNSUPPORTED");
  });

  it.each([
    ["bool", true, "a boolean"],
    ["i32", 1, "a signed 32-bit integer"],
    ["i64", 1n, "a signed 64-bit integer"],
    ["f64", 1, "a number"],
    ["f32", 1, "a number"],
    ["bytes", new Uint8Array(), "bytes"],
  ])("refuses what a %s type hands back if it is not one", (physical, _good, expected) => {
    const liar = defineColumnType({
      name: "liar",
      physical: physical as "i64",
      matches: (annotation: Annotation) => annotation.kind === "none",
      annotate: (): Annotation => ({ kind: "none" }),
      read: (raw: unknown) => raw,
      write: () => "not that at all",
    });
    const error = expectError("ERR_ROW_VALUE_INVALID", appendOne(liar, 1));
    expect(error.message).toContain(expected);
    expect(error.column).toBe("v");
  });

  it("refuses a fixed value of the wrong width", () => {
    const wrong = defineColumnType({
      name: "wrong",
      physical: "fixed",
      typeLength: 4,
      matches: (annotation) => annotation.kind === "none",
      annotate: (): Annotation => ({ kind: "none" }),
      read: (raw) => raw as Uint8Array,
      write: () => new Uint8Array(3),
    });
    const error = expectError("ERR_ROW_VALUE_INVALID", appendOne(wrong, 1));
    expect(error.message).toContain("exactly 4 bytes");
  });

  it("refuses an integer outside the physical range it claimed", () => {
    const overflowing = defineColumnType({
      name: "overflowing",
      physical: "i32",
      matches: (annotation) => annotation.kind === "none",
      annotate: (): Annotation => ({ kind: "none" }),
      read: (raw) => raw as number,
      write: (value: number) => value,
    });
    expectError("ERR_ROW_VALUE_INVALID", appendOne(overflowing, 2 ** 31));
    expectError("ERR_ROW_VALUE_INVALID", appendOne(overflowing, 1.5));
  });

  it("writes the column it buffered, even when the adapter changes underneath", () => {
    // The buffers are shaped when the writer is built and the footer is
    // written at `finish()`. An adapter that answers differently in between
    // would otherwise stamp a type the pages do not hold: a corrupt file, and
    // not one byte of it out of place enough to say so.
    let physical: PhysicalKind = "i32";
    let annotation: Annotation = { kind: "date" };
    const shifty = defineColumnType<number, number>({
      name: "shifty",
      get physical(): PhysicalKind {
        return physical;
      },
      matches: () => false,
      annotate: () => annotation,
      read: (raw) => raw as number,
      write: (value) => value,
    });

    const writer = createWriter(defineSchema({ v: { type: shifty } }));
    writer.append({ v: 1 });
    writer.append({ v: 2 });
    physical = "i64";
    annotation = { kind: "uuid" };
    const bytes = sync(writer.finish());

    // What the pages hold is an INT32 column annotated DATE, and that is what
    // the footer has to say they are.
    expect(readParquet(bytes, { types: [date()] }).rows).toEqual([
      { v: new Date(86_400_000) },
      { v: new Date(172_800_000) },
    ]);
  });

  it("reads the column it claimed, even when the adapter changes underneath", () => {
    // `matches()` runs while the footer is read; the pages are decoded after.
    // An adapter that moves its physical type in between would have the reader
    // taking eight bytes per value out of a column of four.
    const bytes = write(integer({ bitWidth: 8 }), [1, 2, 3, 4]);
    let physical: PhysicalKind = "i32";
    const shifty = defineColumnType<number, number>({
      name: "shifty",
      get physical(): PhysicalKind {
        return physical;
      },
      matches: (annotation) => {
        physical = "i64";
        return annotation.kind === "integer";
      },
      annotate: (): Annotation => ({ kind: "integer", bitWidth: 8, isSigned: true }),
      read: (raw) => raw as number,
      write: (value) => value,
    });

    expect(readParquet(bytes, { types: [shifty] }).rows.map((row) => row.v)).toEqual([1, 2, 3, 4]);
  });
});

describe("date", () => {
  const minInt32 = -(2 ** 31);
  const maxInt32 = 2 ** 31 - 1;
  const minDateDays = -100_000_000;
  const maxDateDays = 100_000_000;

  it("round-trips UTC midnights on both sides of the epoch", () => {
    const days = [
      new Date(Date.UTC(1970, 0, 1)),
      new Date(Date.UTC(1969, 6, 20)),
      new Date(Date.UTC(2026, 7, 24)),
      new Date(Date.UTC(9999, 11, 31)),
    ];
    expect(roundtrip(date(), days)).toEqual(days);
  });

  it("round-trips the full Parquet DATE domain as signed day counts", () => {
    const days = date({ as: "number" });
    expect(roundtrip(days, [minInt32, minDateDays, 0, maxDateDays, maxInt32])).toEqual([
      minInt32,
      minDateDays,
      0,
      maxDateDays,
      maxInt32,
    ]);
  });

  it("keeps the JavaScript Date boundaries representable", () => {
    const boundaries = [new Date(minDateDays * 86_400_000), new Date(maxDateDays * 86_400_000)];
    expect(roundtrip(date(), boundaries)).toEqual(boundaries);
  });

  it("preserves day counts through the returned schema", () => {
    const days = date({ as: "number" });
    const first = readParquet(write(days, [minInt32, maxInt32]), { types: [days] });
    expect(first.schema.columns[0].type).toBe(days);

    const writer = createWriter(first.schema);
    for (const row of first.rows) writer.append(row);
    expect(readParquet(sync(writer.finish()), { types: [days] }).rows).toEqual(first.rows);
  });

  it("refuses a DATE outside JavaScript's range with the lossless remedy", () => {
    const days = date({ as: "number" });
    for (const count of [minDateDays - 1, maxDateDays + 1]) {
      const error = expectError("ERR_READ_UNSUPPORTED", () =>
        readParquet(write(days, [count]), { types: [date()] }),
      );
      expect(error.column).toBe("v");
      expect(error.message).toContain(`${count} days`);
      expect(error.message).toContain("-100000000 to 100000000");
      expect(error.message).toContain('date({ as: "number" })');
    }
  });

  it("validates signed INT32 day counts", () => {
    const days = date({ as: "number" });
    for (const value of [minInt32 - 1, maxInt32 + 1, 0.5, Number.NaN, 0n, new Date(0)]) {
      const error = expectError("ERR_ROW_VALUE_INVALID", appendOne(days, value));
      expect(error.message).toContain("signed 32-bit integer");
    }
  });

  it("refuses an unknown representation", () => {
    expectError("ERR_SCHEMA_COLUMN_INVALID", () => date(null as never));
    expectError("ERR_SCHEMA_COLUMN_INVALID", () =>
      date({
        // @ts-expect-error deliberately wrong input
        as: "string",
      }),
    );
  });

  it("refuses a Date carrying a time of day, rather than truncating it", () => {
    const error = expectError(
      "ERR_ROW_VALUE_INVALID",
      appendOne(date(), new Date(Date.UTC(2026, 7, 24, 12))),
    );
    expect(error.message).toContain("UTC midnight");
    expectError("ERR_ROW_VALUE_INVALID", appendOne(date(), new Date(1)));
    expectError("ERR_ROW_VALUE_INVALID", appendOne(date(), new Date(Number.NaN)));
    expectError("ERR_ROW_VALUE_INVALID", appendOne(date(), 0));
    expectError("ERR_ROW_VALUE_INVALID", appendOne(date(), "2026-08-24"));
  });
});

describe("decimal", () => {
  it("round-trips the canonical form at every physical width", () => {
    expect(roundtrip(decimal({ precision: 9, scale: 2 }), ["0.00", "-1.25", "9999999.99"])).toEqual(
      ["0.00", "-1.25", "9999999.99"],
    );
    expect(
      roundtrip(decimal({ precision: 18, scale: 4 }), ["0.0000", "-99999999999999.9999"]),
    ).toEqual(["0.0000", "-99999999999999.9999"]);
    expect(
      roundtrip(decimal({ precision: 38, scale: 0 }), [
        "0",
        "-99999999999999999999999999999999999999",
        "99999999999999999999999999999999999999",
      ]),
    ).toEqual([
      "0",
      "-99999999999999999999999999999999999999",
      "99999999999999999999999999999999999999",
    ]);
  });

  it("writes the smallest fixed width that can carry the precision", () => {
    expect(decimal({ precision: 19 }).typeLength).toBe(9);
    expect(decimal({ precision: 21 }).typeLength).toBe(9);
    expect(decimal({ precision: 22 }).typeLength).toBe(10);
    expect(decimal({ precision: 38 }).typeLength).toBe(16);
    expect(decimal({ precision: 39 }).typeLength).toBe(17);
    expect(decimal({ precision: 100 }).typeLength).toBe(42);
  });

  it("constructs the complete metadata domain without allocating for its values", () => {
    const widest = decimal({ precision: 2 ** 31 - 1, scale: 2 ** 31 - 1 });
    expect(widest.typeLength).toBe(891_723_283);
  });

  it("reads every legal physical layout through one registered type", () => {
    const annotation = { kind: "decimal", precision: 9, scale: 2 } as const;
    const rawType = (
      physical: "i32" | "i64" | "bytes" | "fixed",
      typeLength?: number,
    ): LogicalAdapter<unknown, unknown> =>
      defineColumnType({
        name: `raw-${physical}`,
        physical,
        ...(typeLength === undefined ? {} : { typeLength }),
        matches: () => false,
        annotate: (): Annotation => annotation,
        read: (raw) => raw,
        write: (value) => value,
      });
    const files = [
      write(rawType("i32"), [125, -125]),
      write(rawType("i64"), [125n, -125n]),
      write(rawType("bytes"), [new Uint8Array([0x7d]), new Uint8Array([0x83])]),
      write(rawType("fixed", 4), [
        new Uint8Array([0, 0, 0, 0x7d]),
        new Uint8Array([0xff, 0xff, 0xff, 0x83]),
      ]),
    ];
    const money = decimal({ precision: 9, scale: 2 });

    for (const bytes of files) {
      expect(readParquet(bytes, { types: [money] }).rows).toEqual([{ v: "1.25" }, { v: "-1.25" }]);
    }
  });

  it("rewrites a noncanonical read layout through the adapter's canonical layout", () => {
    const annotation = { kind: "decimal", precision: 30, scale: 2 } as const;
    const source = write(annotatedAs(annotation, "fixed", 16), [
      new Uint8Array([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x7d]),
    ]);
    const money = decimal({ precision: 30, scale: 2 });
    const first = readParquet(source, { types: [money] });
    expect(first.schema.columns[0]).toMatchObject({ type: money, typeLength: 16 });
    expect(first.rows).toEqual([{ v: "1.25" }]);

    const writer = createWriter(first.schema);
    writer.append(first.rows[0]);
    const second = readParquet(sync(writer.finish()), { types: [money] });
    expect(second.schema.columns[0]).toMatchObject({ type: money, typeLength: 13 });
    expect(second.rows).toEqual(first.rows);
  });

  it.each([
    ["INT32", "i32", undefined, 100],
    ["INT64", "i64", undefined, 100n],
    ["BYTE_ARRAY", "bytes", undefined, new Uint8Array([0x64])],
    ["FIXED_LEN_BYTE_ARRAY", "fixed", 1, new Uint8Array([0x64])],
  ] as const)(
    "refuses a %s value larger than the declared precision",
    (_, physical, typeLength, value) => {
      const annotation = { kind: "decimal", precision: 2, scale: 0 } as const;
      const file = write(annotatedAs(annotation, physical, typeLength), [value]);
      const error = expectError("ERR_READ_MALFORMED", () =>
        readParquet(file, { types: [decimal({ precision: 2 })] }),
      );
      expect(error.column).toBe("v");
      expect(error.cause).toBeInstanceOf(TavolatoError);
      expect((error.cause as TavolatoError).message).toContain("declared precision 2");
    },
  );

  it("refuses an empty BYTE_ARRAY as a DECIMAL integer", () => {
    const annotation = { kind: "decimal", precision: 2, scale: 0 } as const;
    const file = write(annotatedAs(annotation, "bytes"), [new Uint8Array()]);
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(file, { types: [decimal({ precision: 2 })] }),
    );
    expect(error.column).toBe("v");
    expect(error.cause).toBeInstanceOf(TavolatoError);
    expect((error.cause as TavolatoError).message).toContain("at least one byte");
  });

  it("round-trips precision beyond native fixed-width integers", () => {
    const value = "9".repeat(100);
    expect(roundtrip(decimal({ precision: 100 }), [value, `-${value}`])).toEqual([
      value,
      `-${value}`,
    ]);
    expectError("ERR_ROW_VALUE_INVALID", appendOne(decimal({ precision: 100 }), "9".repeat(101)));
  });

  it("insists on exactly one spelling per value", () => {
    const money = decimal({ precision: 9, scale: 2 });
    for (const value of [
      "1.2",
      "1.234",
      "1",
      "01.00",
      "+1.00",
      " 1.00",
      "1.00 ",
      "1e2",
      "",
      "-0.00",
      "1,00",
    ]) {
      const error = expectError("ERR_ROW_VALUE_INVALID", appendOne(money, value));
      expect(error.column).toBe("v");
    }
    expectError("ERR_ROW_VALUE_INVALID", appendOne(money, 1.25));
    expectError("ERR_ROW_VALUE_INVALID", appendOne(money, 125n));
  });

  it("refuses a value the declared precision cannot hold", () => {
    expectError("ERR_ROW_VALUE_INVALID", appendOne(decimal({ precision: 4, scale: 2 }), "100.00"));
    expectError("ERR_ROW_VALUE_INVALID", appendOne(decimal({ precision: 4, scale: 2 }), "-100.00"));
    expect(roundtrip(decimal({ precision: 4, scale: 2 }), ["99.99", "-99.99"])).toEqual([
      "99.99",
      "-99.99",
    ]);
  });

  it("counts the digits it asks for, in the message as well", () => {
    const error = expectError(
      "ERR_ROW_VALUE_INVALID",
      appendOne(decimal({ precision: 4, scale: 1 }), "1.00"),
    );
    expect(error.message).toContain("exactly 1 digit after the point");
  });

  it("writes a scale of zero without a point at all", () => {
    expect(roundtrip(decimal({ precision: 5 }), ["0", "-42", "99999"])).toEqual([
      "0",
      "-42",
      "99999",
    ]);
  });
});

describe("uuid", () => {
  it("round-trips the canonical lowercase form", () => {
    const ids = [
      "00000000-0000-0000-0000-000000000000",
      "ffffffff-ffff-ffff-ffff-ffffffffffff",
      "b3f2c1a0-1111-4222-8333-444455556666",
    ];
    expect(roundtrip(uuid(), ids)).toEqual(ids);
  });

  it("refuses every other spelling, rather than handing back a different string", () => {
    for (const value of [
      "B3F2C1A0-1111-4222-8333-444455556666",
      "b3f2c1a01111422283334444555566 66",
      "b3f2c1a0111142228333444455556666",
      "b3f2c1a0-1111-4222-8333-44445555666",
      "not a uuid",
      "",
    ]) {
      expectError("ERR_ROW_VALUE_INVALID", appendOne(uuid(), value));
    }
    expectError("ERR_ROW_VALUE_INVALID", appendOne(uuid(), new Uint8Array(16)));
  });
});

describe("time and timestamp", () => {
  it("preserves the UTC flag and claims only the exact TIME annotation", () => {
    const local = time({ unit: "micros", isAdjustedToUTC: false });
    const utc = time({ unit: "micros", isAdjustedToUTC: true });

    expect(local.annotate()).toEqual({
      kind: "time",
      unit: "micros",
      isAdjustedToUTC: false,
    });
    expect(utc.annotate()).toEqual({ kind: "time", unit: "micros", isAdjustedToUTC: true });

    const bytes = write(local, [45_296_789_012n]);
    expect(readParquet(bytes, { types: [local] }).rows[0].v).toBe(45_296_789_012n);
    expectError("ERR_READ_UNSUPPORTED", () => readParquet(bytes, { types: [utc] }));
  });

  it("preserves the UTC flag and claims only the exact TIMESTAMP annotation", () => {
    const local = timestamp({ unit: "micros", isAdjustedToUTC: false });
    const utc = timestamp({ unit: "micros", isAdjustedToUTC: true });

    expect(local.annotate()).toEqual({
      kind: "timestamp",
      unit: "micros",
      isAdjustedToUTC: false,
    });
    expect(utc.annotate()).toEqual({
      kind: "timestamp",
      unit: "micros",
      isAdjustedToUTC: true,
    });

    const bytes = write(local, [1_767_225_845_123_456n]);
    const first = readParquet(bytes, { types: [local] });
    expect(first.rows[0].v).toBe(1_767_225_845_123_456n);
    expectError("ERR_READ_UNSUPPORTED", () => readParquet(bytes, { types: [utc] }));

    const writer = createWriter(first.schema);
    writer.append(first.rows[0]);
    const rewritten = sync(writer.finish());
    expect(readParquet(rewritten, { types: [local] }).rows[0].v).toBe(1_767_225_845_123_456n);
    expectError("ERR_READ_UNSUPPORTED", () => readParquet(rewritten, { types: [utc] }));
  });

  it("does not read a local millisecond timestamp as a Date", () => {
    const local = timestamp({ unit: "millis", isAdjustedToUTC: false });
    const bytes = write(local, [1_700_000_000_000n]);

    expectError("ERR_READ_UNSUPPORTED", () => readParquet(bytes));
    expect(readParquet(bytes, { types: [local] }).rows[0].v).toBe(1_700_000_000_000n);
  });

  it("carries milliseconds as a number and the finer units as bigints", () => {
    // The domain is `[0, one day)`, as Arrow and parquet-mr read it: the last
    // count of the day is the largest there is, and a full day is tomorrow.
    expect(
      roundtrip(time({ unit: "millis", isAdjustedToUTC: false }), [0, 45_296_789, 86_399_999]),
    ).toEqual([0, 45_296_789, 86_399_999]);
    expect(
      roundtrip(time({ unit: "micros", isAdjustedToUTC: false }), [0n, 86_399_999_999n]),
    ).toEqual([0n, 86_399_999_999n]);
    expect(
      roundtrip(time({ unit: "nanos", isAdjustedToUTC: false }), [0n, 86_399_999_999_999n]),
    ).toEqual([0n, 86_399_999_999_999n]);
  });

  it("refuses a count that is not a time of day", () => {
    expectError(
      "ERR_ROW_VALUE_INVALID",
      appendOne(time({ unit: "millis", isAdjustedToUTC: false }), -1),
    );
    expectError(
      "ERR_ROW_VALUE_INVALID",
      appendOne(time({ unit: "millis", isAdjustedToUTC: false }), 86_400_001),
    );
    // A whole day is not a time of day: DuckDB renders that count as 24:00:00.
    expectError(
      "ERR_ROW_VALUE_INVALID",
      appendOne(time({ unit: "millis", isAdjustedToUTC: false }), 86_400_000),
    );
    expectError(
      "ERR_ROW_VALUE_INVALID",
      appendOne(time({ unit: "micros", isAdjustedToUTC: false }), 86_400_000_000n),
    );
    expectError(
      "ERR_ROW_VALUE_INVALID",
      appendOne(time({ unit: "nanos", isAdjustedToUTC: false }), 86_400_000_000_000n),
    );
    expectError(
      "ERR_ROW_VALUE_INVALID",
      appendOne(time({ unit: "millis", isAdjustedToUTC: false }), 1.5),
    );
    expectError(
      "ERR_ROW_VALUE_INVALID",
      appendOne(time({ unit: "millis", isAdjustedToUTC: false }), 1n),
    );
    expectError(
      "ERR_ROW_VALUE_INVALID",
      appendOne(time({ unit: "micros", isAdjustedToUTC: false }), -1n),
    );
    expectError(
      "ERR_ROW_VALUE_INVALID",
      appendOne(time({ unit: "micros", isAdjustedToUTC: false }), 1),
    );
  });

  it("round-trips instants as bigints, at every resolution", () => {
    for (const unit of ["millis", "micros", "nanos"] as const) {
      const values = [0n, -1n, 1_767_225_845_123_456n, -(2n ** 63n), 2n ** 63n - 1n];
      expect(roundtrip(timestamp({ unit, isAdjustedToUTC: true }), values)).toEqual(values);
    }
    expectError(
      "ERR_ROW_VALUE_INVALID",
      appendOne(timestamp({ unit: "micros", isAdjustedToUTC: true }), 2n ** 63n),
    );
    expectError(
      "ERR_ROW_VALUE_INVALID",
      appendOne(timestamp({ unit: "micros", isAdjustedToUTC: true }), 0),
    );
  });

  it("does not claim a timestamp with a different UTC flag", () => {
    const naive = defineColumnType({
      name: "naive",
      physical: "i64",
      matches: (annotation) => annotation.kind === "timestamp",
      annotate: (): Annotation => ({ kind: "timestamp", unit: "micros", isAdjustedToUTC: false }),
      read: (raw) => raw as bigint,
      write: (value: bigint) => value,
    });
    const bytes = write(naive, [1_767_225_845_123_456n]);
    expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(bytes, {
        types: [timestamp({ unit: "micros", isAdjustedToUTC: true })],
      }),
    );
    // A different unit is a different column, and stays unclaimed.
    expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(bytes, {
        types: [timestamp({ unit: "millis", isAdjustedToUTC: false })],
      }),
    );
  });

  it("reads a millisecond timestamp as a bigint when asked to", () => {
    // The built-in reads it as a Date; registering the adapter takes it back.
    const writer = createWriter(defineSchema({ t: { type: "timestamp" } }));
    writer.append({ t: 1_700_000_000_000 });
    const bytes = sync(writer.finish());
    expect(readParquet(bytes).rows[0].t).toEqual(new Date(1_700_000_000_000));
    expect(
      readParquet(bytes, {
        types: [timestamp({ unit: "millis", isAdjustedToUTC: true })],
      }).rows[0].t,
    ).toBe(1_700_000_000_000n);
  });
});

describe("float16", () => {
  it("round-trips every value half precision holds exactly", () => {
    const values = [
      0,
      -0,
      1,
      -1,
      0.5,
      -2,
      65_504, // the largest half
      -65_504,
      6.103_515_625e-5, // the smallest normal
      5.960_464_477_539_063e-8, // the smallest subnormal
      Number.POSITIVE_INFINITY,
      Number.NEGATIVE_INFINITY,
    ];
    const read = roundtrip(float16(), values);
    expect(read).toEqual(values);
    expect(Object.is(read[1], -0)).toBe(true);
  });

  it("rounds to nearest, ties to even, and overflows to infinity", () => {
    // 2049 is exactly between two halves at that magnitude; ties go to even.
    expect(roundtrip(float16(), [2049, 2051, 65_519, 65_520, 1e39, -1e39])).toEqual([
      2048,
      2052,
      65_504,
      Number.POSITIVE_INFINITY,
      Number.POSITIVE_INFINITY,
      Number.NEGATIVE_INFINITY,
    ]);
  });

  it("carries NaN across as NaN, and refuses anything that is not a number", () => {
    expect(Number.isNaN(roundtrip(float16(), [Number.NaN])[0])).toBe(true);
    expectError("ERR_ROW_VALUE_INVALID", appendOne(float16(), "1.5"));
    expectError("ERR_ROW_VALUE_INVALID", appendOne(float16(), 1n));
  });

  it("underflows to a signed zero rather than to something else", () => {
    const read = roundtrip(float16(), [1e-10, -1e-10]);
    expect(read).toEqual([0, -0]);
    expect(Object.is(read[1], -0)).toBe(true);
  });

  it("maps 1.5 between its numeric value and exact bit pattern", () => {
    const number = float16();
    const bits = float16({ as: "bits" });
    expect(bits.read(number.write(1.5))).toBe(0x3e00);
    expect(number.read(bits.write(0x3e00))).toBe(1.5);
    expect(bits.write(0x3e00)).toEqual(new Uint8Array([0x00, 0x3e]));
  });

  it("round-trips all 65,536 binary16 encodings by identity", () => {
    const bits = float16({ as: "bits" });
    let checked = 0;
    for (let expected = 0; expected <= 0xffff; expected++) {
      const bytes = bits.write(expected) as Uint8Array;
      if (bytes[0] !== (expected & 0xff) || bytes[1] !== expected >>> 8) {
        throw new Error(`float16 bits ${expected} wrote the wrong bytes`);
      }
      if (bits.read(bytes) !== expected) {
        throw new Error(`float16 bits ${expected} did not round-trip`);
      }
      checked++;
    }
    expect(checked).toBe(65_536);
  });

  it("preserves NaN payloads, signs, infinities, subnormals and both zero encodings", () => {
    const bits = float16({ as: "bits" });
    const encodings = [
      0x0000, 0x8000, 0x0001, 0x03ff, 0x7c00, 0xfc00, 0x7c01, 0x7e00, 0x7fff, 0xfc01, 0xfe00,
      0xffff,
    ];
    expect(roundtrip(bits, encodings)).toEqual(encodings);
    expect(Object.is(float16().read(bits.write(0x8000)), -0)).toBe(true);
  });

  it("keeps the default and explicit numeric representations compatible", () => {
    const values = [1.5, -0, 65_504, Number.NaN];
    const implicit = float16();
    const empty = float16({});
    const explicit = float16({ as: "number" });
    for (const value of values) {
      expect(empty.write(value)).toEqual(implicit.write(value));
      expect(explicit.write(value)).toEqual(implicit.write(value));
    }
  });

  it("validates exact bit-pattern inputs", () => {
    const bits = float16({ as: "bits" });
    for (const value of [-1, 65_536, 0.5, Number.NaN, Infinity, "1", 1n]) {
      const error = expectError("ERR_ROW_VALUE_INVALID", appendOne(bits, value));
      expect(error.message).toContain("unsigned 16-bit integer");
    }
  });

  it("validates its options and representation", () => {
    expectError("ERR_SCHEMA_COLUMN_INVALID", () => float16(null as never));
    expectError("ERR_SCHEMA_COLUMN_INVALID", () => float16(1 as never));
    expectError("ERR_SCHEMA_COLUMN_INVALID", () => float16({ as: "raw" as never }));
  });
});

describe("integer", () => {
  it.each([
    [8, true, [-128, 127]],
    [16, true, [-32_768, 32_767]],
    [32, true, [-(2 ** 31), 2 ** 31 - 1]],
    [8, false, [0, 255]],
    [16, false, [0, 65_535]],
    [32, false, [0, 4_294_967_295]],
  ] as const)("round-trips the %i bit range (signed: %s)", (bitWidth, signed, [low, high]) => {
    const type = integer({ bitWidth, signed });
    expect(roundtrip(type, [low, 0, high])).toEqual([low, 0, high]);
    expectError("ERR_ROW_VALUE_INVALID", appendOne(type, low - 1));
    expectError("ERR_ROW_VALUE_INVALID", appendOne(type, high + 1));
    expectError("ERR_ROW_VALUE_INVALID", appendOne(type, 0.5));
    expectError("ERR_ROW_VALUE_INVALID", appendOne(type, 0n));
  });

  it("round-trips the 64 bit ranges as bigints", () => {
    const signed = integer({ bitWidth: 64 });
    expect(roundtrip(signed, [-(2n ** 63n), 0n, 2n ** 63n - 1n])).toEqual([
      -(2n ** 63n),
      0n,
      2n ** 63n - 1n,
    ]);
    expectError("ERR_ROW_VALUE_INVALID", appendOne(signed, 2n ** 63n));

    // An unsigned 64-bit column is the same bits read the other way round.
    const unsigned = integer({ bitWidth: 64, signed: false });
    expect(roundtrip(unsigned, [0n, 1n, 2n ** 64n - 1n])).toEqual([0n, 1n, 2n ** 64n - 1n]);
    expectError("ERR_ROW_VALUE_INVALID", appendOne(unsigned, -1n));
    expectError("ERR_ROW_VALUE_INVALID", appendOne(unsigned, 2n ** 64n));
    expectError("ERR_ROW_VALUE_INVALID", appendOne(unsigned, 1));
  });

  it("keeps signed and unsigned columns apart", () => {
    const bytes = write(integer({ bitWidth: 32, signed: false }), [4_294_967_295]);
    expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(bytes, { types: [integer({ bitWidth: 32 })] }),
    );
    expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(bytes, { types: [integer({ bitWidth: 16, signed: false })] }),
    );
  });
});

describe("nulls", () => {
  it("never reach a column type, on either side", () => {
    let seen = 0;
    const counting = defineColumnType({
      name: "counting",
      physical: "i32",
      matches: (annotation) => annotation.kind === "date",
      annotate: (): Annotation => ({ kind: "date" }),
      read: (raw) => {
        seen++;
        return raw as number;
      },
      write: (value: number) => {
        seen++;
        return value;
      },
    });
    const writer = createWriter(
      defineSchema({ k: { type: "i64" }, v: { type: counting, optional: true } }),
    );
    writer.append({ k: 0n, v: 1 });
    writer.append({ k: 1n, v: null });
    writer.append({ k: 2n });
    const rows: ReadRow[] = readParquet(sync(writer.finish()), { types: [counting] }).rows;

    expect(rows).toEqual([
      { k: 0n, v: 1 },
      { k: 1n, v: null },
      { k: 2n, v: null },
    ]);
    // One write and one read: the two present values, and nothing else.
    expect(seen).toBe(2);
  });
});
