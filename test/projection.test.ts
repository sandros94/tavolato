import { gunzipSync, gzipSync } from "node:zlib";
import { describe, expect, it } from "vitest";
import {
  createWriter,
  decimal,
  defineSchema,
  readParquet,
  readRowGroups,
  readSchema,
  uuid,
} from "../src/index.ts";
import type { ReaderCodec, ReadRow, ReadValue, SyncParquetRowGroups } from "../src/index.ts";
import {
  CODEC_IDS,
  CompressionCodec,
  encodeFileMetadata,
  type RowGroupMeta,
  snapshotColumn,
} from "../src/internal/format.ts";
import { sealFile, startFile, withPhysicalType, writeDataPage } from "./_build.ts";
import { expectError } from "./_errors.ts";
import { sync } from "./_sync.ts";

/**
 * Column projection: reading some of a file's columns and none of the rest.
 *
 * A column chunk is independently seekable, which is what makes this more than
 * a filter over the rows — an unselected column is never decoded, never
 * decompressed and never even resolved. That last one is the interesting part:
 * because a projection is applied before a column is turned into anything, it
 * *lifts* that column's refusals. A file with an `INT96`, a dictionary-encoded
 * chunk or an annotation nothing claims is readable as long as the offending
 * column is projected away, which is very nearly the point of having
 * projection at all.
 *
 * What it does not lift is the shape of the schema. A nested field means the
 * flat mapping from column to chunk is gone, and that mapping is what a
 * projection walks.
 */

const TYPES = [decimal({ precision: 9, scale: 2 }), uuid()];
const ID = "b3f2c1a0-1111-4222-8333-444455556666";

const schema = defineSchema({
  s: { type: "string", optional: true },
  j: { type: "json" },
  i: { type: "i64" },
  n: { type: "i32" },
  t: { type: "timestamp" },
  p: { type: TYPES[0] },
  u: { type: TYPES[1], optional: true },
});

/** One row of every column type, keyed off `index`. */
function row(index: number): Record<string, ReadValue> {
  return {
    s: index % 3 === 0 ? null : `row-${index}`,
    j: { i: index },
    i: BigInt(index),
    n: 1 - index,
    t: new Date(1_700_000_000_000 + index),
    p: `${index}.25`,
    u: index % 2 === 0 ? ID : null,
  };
}

/** `count` rows of every column type, cut into groups of `rowGroupSize`. */
function sample(
  count: number,
  rowGroupSize = 100,
  codec?: { name: "GZIP"; compress: typeof gzipSync },
): Uint8Array {
  const writer = createWriter(schema, { rowGroupSize, ...(codec === undefined ? {} : { codec }) });
  for (let index = 0; index < count; index++) {
    const { t, ...rest } = row(index);
    sync(writer.append({ ...rest, t: (t as Date).getTime() } as never));
  }
  return sync(writer.finish());
}

/** The rows `sample(count)` holds, narrowed to `names` in **file** order. */
function expected(count: number, names: readonly string[]): ReadRow[] {
  const ordered = schema.columns
    .map((column) => column.name)
    .filter((name) => names.includes(name));
  return Array.from({ length: count }, (_, index) => {
    const full = row(index);
    const narrowed: ReadRow = {};
    for (const name of ordered) narrowed[name] = full[name];
    return narrowed;
  });
}

const gzip = { name: "GZIP", compress: gzipSync } as const;
const codecs: { GZIP: ReaderCodec } = { GZIP: { decompress: (page) => gunzipSync(page) } };

describe("what a projected read hands back", () => {
  it("reads a subset, in file order rather than the order asked for", () => {
    const bytes = sample(5);
    const { schema: read, rows } = readParquet(bytes, { columns: ["n", "s", "j"], types: TYPES });

    expect(read.columns.map((column) => column.name)).toEqual(["s", "j", "n"]);
    expect(rows).toEqual(expected(5, ["s", "j", "n"]));
    expect(Object.keys(rows[0])).toEqual(["s", "j", "n"]);
  });

  it("reads a single column", () => {
    const { schema: read, rows } = readParquet(sample(4), { columns: ["i"] });
    expect(read.columns).toEqual([{ name: "i", type: "i64", optional: false }]);
    expect(rows).toEqual([{ i: 0n }, { i: 1n }, { i: 2n }, { i: 3n }]);
  });

  it("reads every column, which is the unprojected read again", () => {
    const bytes = sample(5);
    const all = schema.columns.map((column) => column.name);
    expect(readParquet(bytes, { columns: all, types: TYPES })).toEqual(
      readParquet(bytes, { types: TYPES }),
    );
    // Asked for backwards, and still the file's order.
    expect(readParquet(bytes, { columns: [...all].reverse(), types: TYPES }).rows).toEqual(
      readParquet(bytes, { types: TYPES }).rows,
    );
  });

  it("narrows the schema with the rows, and that schema writes the narrower file", () => {
    const projected = readParquet(sample(3), { columns: ["i", "s"] });
    expect(projected.schema.columns).toEqual([
      { name: "s", type: "string", optional: true },
      { name: "i", type: "i64", optional: false },
    ]);
    expect(Object.keys(projected.schema.definition)).toEqual(["s", "i"]);

    // Handed straight back to `createWriter`, as any schema a read yields is.
    const writer = createWriter(projected.schema);
    for (const projectedRow of projected.rows) writer.append(projectedRow);
    expect(readParquet(sync(writer.finish())).rows).toEqual(projected.rows);
  });

  it("leaves the row and group counts alone: a projection narrows columns, not rows", () => {
    const bytes = sample(7, 2);
    const whole = readRowGroups(bytes, { types: TYPES });
    const projected = readRowGroups(bytes, { columns: ["i"] });

    expect(projected.rowCount).toBe(whole.rowCount);
    expect(projected.groupCount).toBe(whole.groupCount);
    expect(readParquet(bytes, { columns: ["i"] }).rows).toHaveLength(7);
  });

  it("keeps a projected __proto__ column an own property", () => {
    const named = defineSchema({
      ["__proto__"]: { type: "i64" },
      n: { type: "i64" },
      s: { type: "string" },
    });
    const writer = createWriter(named);
    writer.append({ ["__proto__"]: 7n, n: 1n, s: "x" });
    const { rows } = readParquet(sync(writer.finish()), { columns: ["s", "__proto__"] });

    expect(Object.hasOwn(rows[0], "__proto__")).toBe(true);
    expect(Object.getPrototypeOf(rows[0])).toBe(Object.prototype);
    expect(Object.keys(rows[0])).toEqual(["__proto__", "s"]);
    expect(rows[0]["__proto__"]).toBe(7n);

    // And projected away, it is simply not there — not null, not inherited.
    const without = readParquet(sync(createWriter(named).finish()), { columns: ["n"] });
    expect(without.schema.columns.map((column) => column.name)).toEqual(["n"]);
  });

  it("reads nothing out of a file with no rows, but still narrows the schema", () => {
    const bytes = sync(createWriter(schema).finish());
    const { schema: read, rows } = readParquet(bytes, { columns: ["i", "n"] });
    expect(read.columns.map((column) => column.name)).toEqual(["i", "n"]);
    expect(rows).toEqual([]);
  });
});

describe("what a projection refuses", () => {
  it("refuses every non-object ReadOptions value before reading bytes", () => {
    const readers = [readParquet, readRowGroups, readSchema] as const;
    for (const read of readers) {
      for (const options of [null, false, 0, 1n, "options", Symbol("options"), () => undefined]) {
        expectError("ERR_READ_OPTION_INVALID", () =>
          (read as (bytes: Uint8Array, options: never) => unknown)(
            new Uint8Array(),
            options as never,
          ),
        );
      }
    }
  });

  it("names a column the file does not declare, and what it does", () => {
    const error = expectError("ERR_READ_OPTION_INVALID", () =>
      readParquet(sample(2), { columns: ["i", "nope"] }),
    );
    expect(error.column).toBe("nope");
    expect(error.message).toContain('"nope"');
    // Every name the file holds, so the typo is one line away from fixed.
    for (const name of ["s", "j", "i", "n", "t", "p", "u"]) {
      expect(error.message).toContain(`"${name}"`);
    }
  });

  it("refuses an empty projection rather than reading nothing", () => {
    const error = expectError("ERR_READ_OPTION_INVALID", () =>
      readParquet(sample(2), { columns: [] }),
    );
    expect(error.message).toContain("mistake rather than a request");
  });

  it("refuses a duplicate rather than deduplicating it", () => {
    const error = expectError("ERR_READ_OPTION_INVALID", () =>
      readParquet(sample(2), { columns: ["i", "n", "i"] }),
    );
    expect(error.column).toBe("i");
    expect(error.message).toContain("twice");
  });

  it("refuses an option that is not a list of names", () => {
    // @ts-expect-error deliberately wrong input
    expectError("ERR_READ_OPTION_INVALID", () => readParquet(sample(2), { columns: "i" }));
    const error = expectError("ERR_READ_OPTION_INVALID", () =>
      // @ts-expect-error deliberately wrong input
      readParquet(sample(2), { columns: ["i", 7] }),
    );
    expect(error.message).toContain("ReadOptions.columns[1]");
  });

  it("answers the option before it looks at the file at all", () => {
    // The bytes are not a Parquet file, and the answer is still about the
    // option: an option that cannot be used is the caller's mistake whatever
    // the file turns out to be.
    expectError("ERR_READ_OPTION_INVALID", () => readParquet(new Uint8Array(64), { columns: [] }));
    expectError("ERR_READ_OPTION_INVALID", () =>
      readRowGroups(new Uint8Array(64), { columns: ["a", "a"] }),
    );
  });

  it("refuses from readRowGroups too, when the file is opened", () => {
    expectError("ERR_READ_OPTION_INVALID", () => readRowGroups(sample(2), { columns: ["nope"] }));
  });
});

describe("readSchema, which projection deliberately does not touch", () => {
  it("hands back every column whatever columns says", () => {
    const bytes = sample(3);
    const whole = readSchema(bytes, { types: TYPES });
    expect(readSchema(bytes, { types: TYPES, columns: ["i"] })).toEqual(whole);
    // Not even the refusals: there is nothing to project, so nothing to refuse.
    expect(readSchema(bytes, { types: TYPES, columns: [] })).toEqual(whole);
    expect(readSchema(bytes, { types: TYPES, columns: ["nope"] })).toEqual(whole);
  });
});

describe("refusals a projection lifts", () => {
  it("reads past an INT96 column by projecting it away", () => {
    // `i` is the file's only INT64 column, so it is the one rewritten.
    const bytes = withPhysicalType(sample(3), 3);
    const refusal = expectError("ERR_READ_UNSUPPORTED", () => readParquet(bytes, { types: TYPES }));
    expect(refusal.message).toContain("INT96");
    expect(refusal.column).toBe("i");

    expect(readParquet(bytes, { columns: ["n", "s"] }).rows).toEqual(expected(3, ["s", "n"]));
    // Named in the projection, it is refused again — the column is still an
    // INT96, and projection decides what is read rather than what is legal.
    expect(
      expectError("ERR_READ_UNSUPPORTED", () => readParquet(bytes, { columns: ["i"] })).column,
    ).toBe("i");
  });

  it("reads past a physical type Parquet does not define", () => {
    const bytes = withPhysicalType(sample(3), 9);
    expectError("ERR_READ_MALFORMED", () => readParquet(bytes, { types: TYPES }));
    expect(readParquet(bytes, { columns: ["n"] }).rows).toEqual(expected(3, ["n"]));
  });

  it("reads past an annotation nothing claims, with no types registered at all", () => {
    // `p` is a DECIMAL and `u` a UUID, and neither has a built-in reading.
    const bytes = sample(3);
    const refusal = expectError("ERR_READ_UNSUPPORTED", () => readParquet(bytes));
    expect(refusal.message).toContain("ReadOptions.types");

    expect(readParquet(bytes, { columns: ["i", "t"] }).rows).toEqual(expected(3, ["i", "t"]));
    // And a projection that keeps one of them still wants its column type.
    expectError("ERR_READ_UNSUPPORTED", () => readParquet(bytes, { columns: ["p"] }));
    expect(readParquet(bytes, { columns: ["p"], types: TYPES }).rows).toEqual(expected(3, ["p"]));
  });

  it("does not lift a row group whose chunk list contradicts the schema", () => {
    // The invariant a projection stands on: a column's place in the schema is
    // its chunk's place in every group. A group holding fewer chunks than the
    // file declares columns is refused whether or not the projection would ever
    // have reached the missing one — the mapping is either sound or it is not.
    const bytes = twoColumnsOneChunk();
    expectError("ERR_READ_MALFORMED", () => readParquet(bytes));
    const error = expectError("ERR_READ_MALFORMED", () => readParquet(bytes, { columns: ["n"] }));
    expect(error.message).toContain("column chunks");
    expectError("ERR_READ_MALFORMED", () => readRowGroups(bytes, { columns: ["n"] }));
  });

  it("does not lift a nested schema, which is the shape of the file rather than a column", () => {
    // A group column breaks the one thing projection stands on: that a column's
    // place in the schema is its chunk's place in every row group.
    const bytes = nestedFile();
    const refusal = expectError("ERR_READ_UNSUPPORTED", () => readParquet(bytes));
    expect(refusal.message).toContain("flat, forever");
    const projected = expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(bytes, { columns: ["n"] }),
    );
    expect(projected.message).toContain("flat, forever");
  });
});

describe("projection and codecs", () => {
  it("validates row-group totals across unselected chunks", () => {
    const bytes = mixedCodecs({ rowGroupTotalSizeDelta: 1 });
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(bytes, { columns: ["plain"] }),
    );
    expect(error.message).toContain("total_byte_size");
  });

  it("does not inspect an unselected chunk's byte boundaries", () => {
    const bytes = mixedCodecs({ zippedCompressedSizeDelta: -1 });
    const error = expectError("ERR_READ_MALFORMED", () => readParquet(bytes, { codecs }));
    expect(error.column).toBe("zipped");
    expect(readParquet(bytes, { columns: ["plain"] }).rows).toEqual([
      { plain: 1n },
      { plain: 2n },
      { plain: 3n },
    ]);
  });

  it("does not need a codec for a column it never reads", () => {
    const bytes = mixedCodecs();
    const refusal = expectError("ERR_READ_UNSUPPORTED", () => readParquet(bytes));
    expect(refusal.message).toContain("compressed with GZIP");
    expect(refusal.column).toBe("zipped");

    // The uncompressed column alone, with nothing registered: the GZIP chunk's
    // pages are never reached, so its codec never has to exist.
    expect(readParquet(bytes, { columns: ["plain"] }).rows).toEqual([
      { plain: 1n },
      { plain: 2n },
      { plain: 3n },
    ]);
    expect(sync(readParquet(bytes, { columns: ["zipped"], codecs })).rows).toEqual([
      { zipped: 10n },
      { zipped: 20n },
      { zipped: 30n },
    ]);
  });

  it("skips the unselected chunk in the eager sweep readRowGroups does", () => {
    // The sweep is where an unregistered codec is normally answered, before a
    // step is taken. A projected column is skipped there too, or opening the
    // file would refuse what walking it never touches.
    const bytes = mixedCodecs();
    expectError("ERR_READ_UNSUPPORTED", () => readRowGroups(bytes));

    const file: SyncParquetRowGroups = readRowGroups(bytes, { columns: ["plain"] });
    expect(file.rowCount).toBe(3);
    expect([...file].flatMap((rows) => sync(rows))).toEqual([
      { plain: 1n },
      { plain: 2n },
      { plain: 3n },
    ]);
  });

  it("stays synchronous, and reads the same rows through a codec", () => {
    const bytes = sample(6, 2, gzip);
    const columns = ["s", "i", "u"];
    const options = { columns, types: TYPES, codecs };

    let deferred = false;
    queueMicrotask(() => {
      deferred = true;
    });
    const rows = sync(readParquet(bytes, options)).rows;

    expect(deferred).toBe(false);
    expect(rows).toEqual(expected(6, columns));
  });

  it("reads the same rows through an asynchronous codec", async () => {
    const bytes = sample(6, 2, gzip);
    const columns = ["j", "p"];
    const asyncCodecs = { GZIP: { decompress: async (page: Uint8Array) => gunzipSync(page) } };
    const { rows } = await readParquet(bytes, { columns, types: TYPES, codecs: asyncCodecs });
    expect(rows).toEqual(expected(6, columns));
  });
});

describe("equivalence with the lazy read", () => {
  const cases: readonly { readonly what: string; readonly columns: readonly string[] }[] = [
    { what: "one column", columns: ["i"] },
    { what: "a subset", columns: ["s", "n", "u"] },
    { what: "asked for backwards", columns: ["u", "n", "s"] },
    { what: "every column", columns: ["s", "j", "i", "n", "t", "p", "u"] },
    { what: "the first column only", columns: ["s"] },
    { what: "the last column only", columns: ["u"] },
  ];

  for (const { what, columns } of cases) {
    it(`walks to the same rows as readParquet: ${what}`, () => {
      // Every group of a multi-group, compressed, adapter-carrying file.
      const bytes = sample(7, 2, gzip);
      const options = { columns, types: TYPES, codecs };
      const eager = sync(readParquet(bytes, options));
      const lazy = readRowGroups(bytes, options);

      expect(lazy.schema).toEqual(eager.schema);
      expect(lazy.rowCount).toBe(eager.rows.length);
      expect([...lazy].flatMap((rows) => sync(rows))).toEqual(eager.rows);
      expect(eager.rows).toEqual(expected(7, columns));
    });
  }
});

/**
 * A two column file where only `zipped` is compressed — which tavolato's own
 * writer cannot produce, since a codec applies to every column it writes.
 */
function mixedCodecs(
  doctored: {
    readonly rowGroupTotalSizeDelta?: number;
    readonly zippedCompressedSizeDelta?: number;
  } = {},
): Uint8Array {
  const out = startFile();
  const plainAt = out.length;
  const plain = writeDataPage(out, [1n, 2n, 3n]);
  const zippedAt = out.length;
  const zipped = writeDataPage(out, [10n, 20n, 30n], gzipSync);

  const group: RowGroupMeta = {
    columns: [
      {
        name: "plain",
        physical: "i64",
        optional: false,
        codec: CompressionCodec.UNCOMPRESSED,
        numValues: 3,
        nullCount: 0,
        dataPageOffset: plainAt,
        totalUncompressedSize: plain.uncompressedSize,
        totalCompressedSize: plain.compressedSize,
      },
      {
        name: "zipped",
        physical: "i64",
        optional: false,
        codec: CODEC_IDS.GZIP,
        numValues: 3,
        nullCount: 0,
        dataPageOffset: zippedAt,
        totalUncompressedSize: zipped.uncompressedSize,
        totalCompressedSize: zipped.compressedSize + (doctored.zippedCompressedSizeDelta ?? 0),
      },
    ],
    numRows: 3,
    totalByteSize:
      plain.uncompressedSize + zipped.uncompressedSize + (doctored.rowGroupTotalSizeDelta ?? 0),
    totalCompressedSize: plain.compressedSize + zipped.compressedSize,
    fileOffset: plainAt,
  };
  const footer = encodeFileMetadata(
    [
      snapshotColumn({ name: "plain", type: "i64", optional: false }),
      snapshotColumn({ name: "zipped", type: "i64", optional: false }),
    ],
    [group],
    3,
    "probe",
  );
  return sealFile(out, footer);
}

/**
 * A file whose schema declares a group of two fields beside a plain column: one
 * flat level is what tavolato reads, and this is what it is not.
 */
function nestedFile(): Uint8Array {
  return twoColumnsOneChunk(withNumChildren);
}

/**
 * Two columns declared and one chunk in the row group — a footer contradicting
 * itself, and the ground {@link nestedFile} is built on.
 */
function twoColumnsOneChunk(
  patch: (footer: Uint8Array) => Uint8Array = (footer) => footer,
): Uint8Array {
  const out = startFile();
  const at = out.length;
  const page = writeDataPage(out, [1n, 2n]);

  const footer = encodeFileMetadata(
    [
      snapshotColumn({ name: "n", type: "i64", optional: false }),
      snapshotColumn({ name: "group", type: "i64", optional: false }),
    ],
    [
      {
        columns: [
          {
            name: "n",
            physical: "i64",
            optional: false,
            codec: CompressionCodec.UNCOMPRESSED,
            numValues: 2,
            nullCount: 0,
            dataPageOffset: at,
            totalUncompressedSize: page.uncompressedSize,
            totalCompressedSize: page.compressedSize,
          },
        ],
        numRows: 2,
        totalByteSize: page.uncompressedSize,
        totalCompressedSize: page.compressedSize,
        fileOffset: at,
      },
    ],
    2,
    "probe",
  );
  return sealFile(out, patch(footer));
}

/**
 * Turns the footer's **second** leaf into a group of two children, by finding
 * its `SchemaElement` and giving it a `num_children`.
 *
 * A leaf is written as `15 04 25 00 18 …`; the `num_children` field is id 5,
 * which follows the name. Rather than splice bytes into a compact struct, the
 * element is rewritten in place: the trailing name is short enough that the two
 * extra bytes fit where the next element's header begins, so the whole thing is
 * built by hand instead.
 */
function withNumChildren(footer: Uint8Array): Uint8Array {
  // `18 05 67 72 6f 75 70` is `name = "group"`; `15 04` after it is num_children
  // = 2 (zigzag), which is what turns the leaf into a group.
  const name = [0x18, 0x05, 0x67, 0x72, 0x6f, 0x75, 0x70];
  for (let offset = 0; offset + name.length <= footer.length; offset++) {
    if (!name.every((byte, index) => footer[offset + index] === byte)) continue;
    const at = offset + name.length;
    const patched = new Uint8Array(footer.length + 2);
    patched.set(footer.subarray(0, at));
    patched.set([0x15, 0x04], at); // field 5 (delta 1 from 4), i32, zigzag(2) = 4
    patched.set(footer.subarray(at), at + 2);
    return patched;
  }
  throw new Error('no schema element named "group" found');
}
