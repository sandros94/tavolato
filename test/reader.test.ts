import { describe, expect, it } from "vitest";
import {
  createWriter,
  decimal,
  defineSchema,
  isTavolatoError,
  readParquet,
  readSchema,
  TavolatoError,
  uuid,
} from "../src/index.ts";
import { expectError } from "./_errors.ts";
import { sync } from "./_sync.ts";

/**
 * The reader's failure surface: a malformed file must fail with a typed error
 * at the byte that gave it away, and a file that is real Parquet but outside
 * the subset must say which feature it tripped over.
 *
 * Nothing here may crash ungracefully — no `RangeError`, no `TypeError`, no
 * loop that never ends.
 */

/** The logical types the sample file's annotated columns need. */
const TYPES = [decimal({ precision: 9, scale: 2 }), uuid()];

/** Reads with the sample's column types registered, as its own writer would. */
function read(bytes: Uint8Array): unknown {
  return readParquet(bytes, { types: TYPES });
}

/**
 * A small but complete file: two row groups, nulls, and one column of every
 * physical type the writer can emit — the fixed-width ones and their
 * annotations included, so the sweeps below cover the annotation decoder too.
 */
function sample(): Uint8Array {
  const schema = defineSchema({
    s: { type: "string", optional: true },
    f: { type: "f64" },
    g: { type: "f32" },
    i: { type: "i64" },
    n: { type: "i32" },
    b: { type: "bool", optional: true },
    t: { type: "timestamp" },
    p: { type: TYPES[0] },
    u: { type: TYPES[1], optional: true },
  });
  const writer = createWriter(schema, { rowGroupSize: 2 });
  const id = "b3f2c1a0-1111-4222-8333-444455556666";
  writer.append({ s: "alpha", f: 1.5, g: 0.5, i: 1n, n: 1, b: true, t: 0, p: "1.25", u: id });
  writer.append({ s: null, f: -0.25, g: -1, i: 2n, n: -2, b: null, t: 1000, p: "-0.01", u: null });
  writer.append({ s: "gamma", f: 0, g: 2, i: 3n, n: 3, b: false, t: 2000, p: "0.00", u: id });
  return sync(writer.finish());
}

/** The smallest file the writer emits: one i64 column, one row. */
function minimal(): Uint8Array {
  const writer = createWriter(defineSchema({ n: { type: "i64" } }));
  writer.append({ n: 1n });
  return sync(writer.finish());
}

/**
 * Overwrites one byte, first asserting what was there. The assertion is the
 * point: if the writer's layout ever moves, these tests fail loudly instead of
 * quietly patching something harmless.
 */
function patch(bytes: Uint8Array, offset: number, from: number, to: number): Uint8Array {
  expect(bytes[offset]).toBe(from);
  const copy = bytes.slice();
  copy[offset] = to;
  return copy;
}

/** Runs `read`, requiring that whatever it throws is a `TavolatoError`. */
function caught(read: () => unknown): TavolatoError | undefined {
  try {
    read();
    return undefined;
  } catch (error) {
    if (!(error instanceof TavolatoError)) throw error;
    return error;
  }
}

describe("file envelope", () => {
  it("rejects an empty input", () => {
    expectError("ERR_READ_MALFORMED", () => readParquet(new Uint8Array()));
  });

  it("rejects an input too short to hold an envelope", () => {
    for (const length of [1, 4, 11, 12]) {
      expectError("ERR_READ_MALFORMED", () => readParquet(new Uint8Array(length)));
    }
  });

  it("rejects a missing leading magic", () => {
    const bytes = minimal();
    const error = expectError("ERR_READ_MALFORMED", () => readParquet(patch(bytes, 0, 0x50, 0x51)));
    expect(error.message).toContain("PAR1");
  });

  it("rejects a missing trailing magic", () => {
    const bytes = minimal();
    expectError("ERR_READ_MALFORMED", () =>
      readParquet(patch(bytes, bytes.length - 1, 0x31, 0x32)),
    );
  });

  it("rejects a footer length that does not fit the file", () => {
    const bytes = minimal();
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    for (const length of [0, 0xff_ff_ff_ff, bytes.length]) {
      const copy = bytes.slice();
      new DataView(copy.buffer).setUint32(copy.length - 8, length, true);
      const error = expectError("ERR_READ_MALFORMED", () => readParquet(copy));
      expect(error.message).toContain(String(length));
    }
    // The untouched file still reads, so the patches above are what broke it.
    expect(view.getUint32(bytes.length - 8, true)).toBeGreaterThan(0);
    expect(readParquet(bytes).rows).toEqual([{ n: 1n }]);
  });
});

describe("unsupported features", () => {
  /*
   * The writer's first data page header starts at byte 4 and, for a one row
   * i64 column, is exactly:
   *
   *   15 00  PageHeader.type          = DATA_PAGE
   *   15 10  uncompressed_page_size   = 8
   *   15 10  compressed_page_size     = 8
   *   2c     DataPageHeader (struct)
   *   15 02    num_values             = 1
   *   15 00    encoding               = PLAIN
   *   15 06    definition_level_enc   = RLE
   *   15 06    repetition_level_enc   = RLE
   *   00 00  two stop fields
   */
  const PAGE_TYPE_VALUE = 5;
  const ENCODING_VALUE = 14;

  it("names a data page v2", () => {
    // zigzag(3) = 6 → DATA_PAGE_V2.
    const error = expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(patch(minimal(), PAGE_TYPE_VALUE, 0x00, 0x06)),
    );
    expect(error.message).toContain("DATA_PAGE_V2");
    expect(error.column).toBe("n");
  });

  it("names a dictionary page found where a data page was promised", () => {
    // zigzag(2) = 4 → DICTIONARY_PAGE.
    const error = expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(patch(minimal(), PAGE_TYPE_VALUE, 0x00, 0x04)),
    );
    expect(error.message).toContain("DICTIONARY_PAGE");
  });

  it("names the encoding a page claims to use", () => {
    // zigzag(8) = 16 → RLE_DICTIONARY.
    const error = expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(patch(minimal(), ENCODING_VALUE, 0x00, 0x10)),
    );
    expect(error.message).toContain("RLE_DICTIONARY");
    expect(error.column).toBe("n");
  });

  it("names the encoding of a nullable column's definition levels", () => {
    const schema = defineSchema({ n: { type: "i64", optional: true } });
    const writer = createWriter(schema);
    writer.append({ n: 1n });
    // Same layout, with the level stream adding its 4 byte length prefix and
    // two bytes of RLE: the page is 14 bytes, zigzag(14) = 28, still one byte,
    // so definition_level_encoding is still the value at offset 16.
    const bytes = patch(sync(writer.finish()), 7, 0x1c, 0x1c);
    const error = expectError(
      "ERR_READ_UNSUPPORTED",
      () => readParquet(patch(bytes, 16, 0x06, 0x08)), // zigzag(4) = 8 → BIT_PACKED
    );
    expect(error.message).toContain("BIT_PACKED");
    expect(error.column).toBe("n");
  });

  it("always says that tavolato only reads what it writes", () => {
    const error = expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(patch(minimal(), ENCODING_VALUE, 0x00, 0x10)),
    );
    expect(error.message).toContain("tavolato only reads the files it writes");
  });
});

/**
 * Rewrites the physical type of `minimal()`'s single column, standing in for a
 * file no writer here can produce.
 *
 * The leaf's `SchemaElement` is `15 04 25 00 18 01 6e`: type INT64
 * (zigzag 2 = 4), repetition REQUIRED, then the name. Only the schema element
 * spells the type that way — the column chunk's copy is followed by its
 * encodings list — so the pattern is unambiguous.
 */
function withPhysicalType(bytes: Uint8Array, physical: number): Uint8Array {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const footer = bytes.length - 8 - view.getUint32(bytes.length - 8, true);
  for (let offset = footer; offset < bytes.length - 8; offset++) {
    if (bytes[offset] === 0x15 && bytes[offset + 1] === 0x04 && bytes[offset + 2] === 0x25) {
      const copy = bytes.slice();
      copy[offset + 1] = physical * 2; // zigzag of a small positive number
      return copy;
    }
  }
  throw new Error("no INT64 schema element found");
}

describe("types the reader refuses outright", () => {
  it("refuses INT96 permanently, and says why", () => {
    const error = expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(withPhysicalType(minimal(), 3)),
    );
    expect(error.message).toContain("INT96");
    expect(error.message).toContain("deprecated");
    // No remedy: this one does not lift for any option.
    expect(error.message).not.toContain("ReadOptions");
    expect(error.column).toBe("n");
  });

  it("refuses a physical type Parquet does not define", () => {
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(withPhysicalType(minimal(), 9)),
    );
    expect(error.message).toContain("9");
    expect(error.column).toBe("n");
  });

  it("refuses a FIXED_LEN_BYTE_ARRAY with no type_length", () => {
    // Physical type changed to FIXED_LEN_BYTE_ARRAY, but the element carries no
    // width, so the column's values have no length at all.
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(withPhysicalType(minimal(), 7)),
    );
    expect(error.message).toContain("type_length");
    expect(error.column).toBe("n");
  });

  it("names the annotation, with its parameters, and the remedy", () => {
    const schema = defineSchema({ p: { type: decimal({ precision: 9, scale: 2 }) } });
    const writer = createWriter(schema);
    writer.append({ p: "1.25" });
    const bytes = sync(writer.finish());

    const error = expectError("ERR_READ_UNSUPPORTED", () => readParquet(bytes));
    expect(error.message).toContain("DECIMAL(precision=9, scale=2)");
    expect(error.message).toContain("INT32");
    expect(error.message).toContain("pass a matching type in ReadOptions.types");
    expect(error.column).toBe("p");

    // A type that claims a *different* decimal does not claim this one.
    expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(bytes, { types: [decimal({ precision: 9, scale: 4 })] }),
    );
  });

  it("names a fixed-width column by the width it declares", () => {
    const writer = createWriter(defineSchema({ u: { type: uuid() } }));
    writer.append({ u: "b3f2c1a0-1111-4222-8333-444455556666" });
    const error = expectError("ERR_READ_UNSUPPORTED", () => readParquet(sync(writer.finish())));
    expect(error.message).toContain("FIXED_LEN_BYTE_ARRAY(16) annotated UUID");
  });
});

describe("truncation", () => {
  it("rejects every truncation of a real file", () => {
    // Every prefix loses the trailing magic, so every one of them must be
    // refused — and must be refused quickly: the suite's timeout is the proof
    // that nothing loops.
    const bytes = sample();
    expect(bytes.length).toBeGreaterThan(200);
    const codes = new Set<string>();
    const survived: number[] = [];
    for (let length = 0; length < bytes.length; length++) {
      const error = caught(() => read(bytes.subarray(0, length)));
      if (error === undefined) survived.push(length);
      else codes.add(error.code);
    }
    expect(survived).toEqual([]);
    expect([...codes]).toEqual(["ERR_READ_MALFORMED"]);
  });

  it("rejects a file whose middle has been cut out", () => {
    // Envelope intact, page bytes gone: the footer's offsets now point past
    // the end, or into the wrong bytes.
    const bytes = sample();
    for (let removed = 1; removed < bytes.length - 20; removed += 3) {
      const copy = new Uint8Array(bytes.length - removed);
      copy.set(bytes.subarray(0, 8), 0);
      copy.set(bytes.subarray(8 + removed), 8);
      expect(caught(() => read(copy))).toBeInstanceOf(TavolatoError);
    }
  });

  it("survives every single-byte corruption of a real file", () => {
    // Not every flip is detectable — some land in a string's bytes or a value —
    // so the contract is weaker but stricter in kind: read it, or throw a
    // TavolatoError. Never a RangeError, never a hang.
    const bytes = sample();
    let thrown = 0;
    for (let offset = 0; offset < bytes.length; offset++) {
      for (const mask of [0x01, 0x80, 0xff]) {
        const copy = bytes.slice();
        copy[offset] ^= mask;
        if (caught(() => read(copy)) !== undefined) thrown++;
      }
    }
    // Most corruptions are structural and must be caught, not shrugged off.
    expect(thrown).toBeGreaterThan(bytes.length);
  });

  it("survives every single-byte corruption with no column types registered", () => {
    // The same sweep with the annotated columns unclaimed: every read now ends
    // in a refusal rather than a value, and `caught` still insists that every
    // one of them is a TavolatoError.
    const bytes = sample();
    const codes = new Set<string>();
    for (let offset = 0; offset < bytes.length; offset++) {
      for (const mask of [0x01, 0xff]) {
        const copy = bytes.slice();
        copy[offset] ^= mask;
        const error = caught(() => readParquet(copy));
        if (error !== undefined) codes.add(error.code);
      }
    }
    expect([...codes].sort()).toEqual(["ERR_READ_MALFORMED", "ERR_READ_UNSUPPORTED"]);
  });
});

describe("readSchema", () => {
  it("rejects a malformed envelope just like readParquet", () => {
    expectError("ERR_READ_MALFORMED", () => readSchema(new Uint8Array(20)));
  });

  it("reports an unsupported schema without reading a page", () => {
    // The page bytes are destroyed, but the schema is still readable: proof
    // that readSchema stops at the footer.
    const bytes = sample();
    const copy = bytes.slice();
    copy.fill(0xff, 4, 40);
    expect(readSchema(copy, { types: TYPES }).columns.map((column) => column.name)).toEqual([
      "s",
      "f",
      "g",
      "i",
      "n",
      "b",
      "t",
      "p",
      "u",
    ]);
    expect(caught(() => read(copy))).toBeInstanceOf(TavolatoError);
  });

  it("claims columns with the same types readParquet would", () => {
    const bytes = sample();
    expect(readSchema(bytes, { types: TYPES })).toEqual(
      (readParquet(bytes, { types: TYPES }) as { schema: unknown }).schema,
    );
    // Without them the annotated columns have no meaning, and it says so.
    const error = expectError("ERR_READ_UNSUPPORTED", () => readSchema(bytes));
    expect(error.column).toBe("p");
  });
});

describe("error identity", () => {
  it("throws TavolatoError, narrowable by code", () => {
    const error = caught(() => readParquet(new Uint8Array(4)));
    expect(error).toBeInstanceOf(TavolatoError);
    expect(isTavolatoError(error, "ERR_READ_MALFORMED")).toBe(true);
    expect(isTavolatoError(error, "ERR_READ_UNSUPPORTED")).toBe(false);
  });
});
