import { gunzipSync, gzipSync } from "node:zlib";
import { describe, expect, it } from "vitest";
import {
  createWriter,
  decimal,
  defineSchema,
  readParquet,
  readRowGroups,
  uuid,
} from "../src/index.ts";
import type {
  ParquetRowGroups,
  ReaderCodec,
  ReadRow,
  ReadValue,
  SyncParquetRowGroups,
  WriterCodec,
} from "../src/index.ts";
import {
  CODEC_IDS,
  CompressionCodec,
  encodeFileMetadata,
  type RowGroupMeta,
  snapshotColumn,
} from "../src/internal/format.ts";
import { sealFile, startFile, writeDataPage } from "./_build.ts";
import { expectError, expectRejection } from "./_errors.ts";
import { sync } from "./_sync.ts";

/**
 * Reading a file one row group at a time.
 *
 * A row group is an independently decodable slice of a Parquet file, and this
 * is the surface that says so: the footer is read once, up front, and each step
 * of the walk decodes exactly one group. What is being tested is that the lazy
 * read is the *same* read — same values, same errors, same synchronous fast
 * path — only spread over as many steps as the file has groups.
 */

/** The logical types the sample file's annotated columns need. */
const TYPES = [decimal({ precision: 9, scale: 2 }), uuid()];

const ID = "b3f2c1a0-1111-4222-8333-444455556666";

const schema = defineSchema({
  s: { type: "string", optional: true },
  j: { type: "json" },
  f: { type: "f64" },
  g: { type: "f32" },
  i: { type: "i64" },
  n: { type: "i32" },
  b: { type: "bool", optional: true },
  t: { type: "timestamp" },
  p: { type: TYPES[0] },
  u: { type: TYPES[1], optional: true },
});

/** One row of every column type there is, keyed off `index`. */
function row(index: number): Record<string, ReadValue> {
  return {
    s: index % 3 === 0 ? null : `row-${index}`,
    j: `{"i":${index}}`,
    f: index + 0.5,
    g: index * 0.5,
    i: BigInt(index),
    n: 1 - index, // signed, and never a negative zero: an i32 reads back as 0
    b: index % 2 === 0 ? true : null,
    t: new Date(1_700_000_000_000 + index),
    p: `${index}.25`,
    u: index % 2 === 0 ? ID : null,
  };
}

/** Writes `count` rows of every column type, cut into groups of `rowGroupSize`. */
function sample(count: number, rowGroupSize: number, codec?: WriterCodec): Uint8Array {
  const writer = createWriter(schema, { rowGroupSize, ...(codec === undefined ? {} : { codec }) });
  for (let index = 0; index < count; index++) {
    const { t, ...rest } = row(index);
    sync(writer.append({ ...rest, t: (t as Date).getTime() } as never));
  }
  return sync(writer.finish());
}

/** The rows `sample(count, …)` holds, sliced into the groups it was written in. */
function expected(count: number, rowGroupSize: number): ReadRow[][] {
  const groups: ReadRow[][] = [];
  for (let index = 0; index < count; index++) {
    if (index % rowGroupSize === 0) groups.push([]);
    groups.at(-1)?.push(row(index) as ReadRow);
  }
  return groups;
}

/** A one column file of `count` rows, one row group each: the smallest thing with groups. */
function counting(count: number): Uint8Array {
  const writer = createWriter(defineSchema({ n: { type: "i64" } }), { rowGroupSize: 1 });
  for (let index = 0; index < count; index++) writer.append({ n: BigInt(index) });
  return sync(writer.finish());
}

/**
 * Where each of `counting()`'s data page headers starts.
 *
 * Every page is the same seventeen bytes — `DATA_PAGE`, then two sizes of 8,
 * then the `DataPageHeader` struct — so the header itself is the marker. The
 * caller asserts how many were found, which is what keeps this honest if the
 * writer's layout ever moves.
 */
function pageStarts(bytes: Uint8Array): number[] {
  const header = [0x15, 0x00, 0x15, 0x10, 0x15, 0x10, 0x2c];
  const starts: number[] = [];
  for (let offset = 0; offset + header.length <= bytes.length; offset++) {
    if (header.every((byte, index) => bytes[offset + index] === byte)) starts.push(offset);
  }
  return starts;
}

/** Rewrites the `index`th page into a v2 one, which the reader refuses by name. */
function breakPage(bytes: Uint8Array, index: number, pages: number): Uint8Array {
  const starts = pageStarts(bytes);
  expect(starts).toHaveLength(pages);
  const copy = bytes.slice();
  copy[starts[index] + 1] = 0x06; // zigzag(3) → DATA_PAGE_V2
  return copy;
}

/** Pulls one step, asserting the walk is not over. */
function step(iterator: Iterator<ReadRow[] | Promise<ReadRow[]>>): ReadRow[] | Promise<ReadRow[]> {
  const next = iterator.next();
  expect(next.done).toBe(false);
  return next.value as ReadRow[] | Promise<ReadRow[]>;
}

/**
 * Walks a read to the end, insisting every step stayed synchronous — which is
 * the assertion for a read with no codec *and* for one with a synchronous
 * decompressor, since neither may defer.
 */
function walk(file: ParquetRowGroups): ReadRow[][] {
  const groups: ReadRow[][] = [];
  for (const rows of file) {
    // Not a promise, and not a thenable of any other shape either.
    expect((rows as { then?: unknown }).then).toBeUndefined();
    groups.push(sync(rows));
  }
  return groups;
}

const gzip: WriterCodec = { name: "GZIP", compress: gzipSync };
const syncCodecs: { GZIP: ReaderCodec } = { GZIP: { decompress: (page) => gunzipSync(page) } };
const asyncCodecs: { GZIP: ReaderCodec } = {
  GZIP: { decompress: async (page) => gunzipSync(page) },
};

describe("what readRowGroups reads eagerly", () => {
  it("hands back the footer whole, before a page is touched", () => {
    const bytes = sample(5, 2);
    const file = readRowGroups(bytes, { types: TYPES });

    expect(file.rowCount).toBe(5);
    expect(file.groupCount).toBe(3); // 2 + 2 + 1
    expect(file.schema).toEqual(readParquet(bytes, { types: TYPES }).schema);
  });

  it("reads that footer without reading a page", () => {
    // The page bytes are destroyed and the counts still come back: proof that
    // opening the file stops at the footer, exactly as readSchema does.
    const bytes = sample(4, 2);
    const wrecked = bytes.slice();
    wrecked.fill(0xff, 4, 60);
    const file = readRowGroups(wrecked, { types: TYPES });

    expect(file.rowCount).toBe(4);
    expect(file.groupCount).toBe(2);
    expect(file.schema.columns.map((column) => column.name)).toEqual([
      "s",
      "j",
      "f",
      "g",
      "i",
      "n",
      "b",
      "t",
      "p",
      "u",
    ]);
  });

  it("refuses a malformed envelope from the call itself", () => {
    const bytes = counting(2);
    expectError("ERR_READ_MALFORMED", () => readRowGroups(new Uint8Array()));
    expectError("ERR_READ_MALFORMED", () => readRowGroups(new Uint8Array(20)));
    expectError("ERR_READ_MALFORMED", () => readRowGroups(bytes.subarray(0, bytes.length - 1)));
  });

  it("refuses a schema outside the subset from the call itself", () => {
    // Nothing claims the annotated columns, and the refusal names the first of
    // them — from `readRowGroups`, with not one step taken.
    const error = expectError("ERR_READ_UNSUPPORTED", () => readRowGroups(sample(4, 2)));
    expect(error.column).toBe("p");
    expect(error.message).toContain("ReadOptions.types");
  });

  it("refuses a chunk nothing can decode from the call itself, not from a step", () => {
    // A codec nobody registered is a property of the footer, not of a page, so
    // it is answered when the file is opened rather than three groups in.
    const bytes = sample(4, 2, gzip);
    const error = expectError("ERR_READ_UNSUPPORTED", () => readRowGroups(bytes, { types: TYPES }));
    expect(error.message).toContain("compressed with GZIP");
    // With one registered, the same file opens and walks.
    expect(walk(readRowGroups(bytes, { types: TYPES, codecs: syncCodecs }))).toEqual(
      expected(4, 2),
    );
  });

  it("refuses a footer whose counts contradict each other", () => {
    const bytes = counting(3);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const footer = bytes.length - 8 - view.getUint32(bytes.length - 8, true);
    // `FileMetaData.num_rows` is field 3, an i64, written straight after the
    // schema list: a delta of one over the list's field 2 makes the header
    // byte 0x16, and zigzag(3) makes the value 6.
    const at = indexOfPair(bytes, footer, 0x16, 0x06);
    const copy = bytes.slice();
    copy[at + 1] = 0x08; // zigzag(4): one row too many
    const error = expectError("ERR_READ_MALFORMED", () => readRowGroups(copy));
    expect(error.message).toContain("row groups add up to");
  });
});

/** Finds a two byte sequence at or after `from`, insisting it is there exactly once. */
function indexOfPair(bytes: Uint8Array, from: number, first: number, second: number): number {
  const found: number[] = [];
  for (let offset = from; offset < bytes.length - 1; offset++) {
    if (bytes[offset] === first && bytes[offset + 1] === second) found.push(offset);
  }
  expect(found).toHaveLength(1);
  return found[0];
}

describe("walking the groups", () => {
  it("yields one group per step, in file order, with every column type", () => {
    // Typed as the synchronous shape: column types are pure value transforms,
    // so a read that registers no codec cannot defer and does not pretend to.
    const file: SyncParquetRowGroups = readRowGroups(sample(5, 2), { types: TYPES });
    const groups = walk(file);
    expect(groups).toEqual(expected(5, 2));
    expect(groups.map((rows) => rows.length)).toEqual([2, 2, 1]);
  });

  it("yields a single group for a file that holds one", () => {
    const file = readRowGroups(sample(3, 10), { types: TYPES });
    expect(file.groupCount).toBe(1);
    expect(walk(file)).toEqual([expected(3, 10)[0]]);
  });

  it("yields nothing at all for a file with no rows", () => {
    const bytes = sync(createWriter(schema).finish());
    const file = readRowGroups(bytes, { types: TYPES });

    expect(file.rowCount).toBe(0);
    expect(file.groupCount).toBe(0);
    expect(file.schema.columns).toHaveLength(10);
    expect(walk(file)).toEqual([]);
    expect([...file]).toEqual([]);
  });

  it("starts every walk at the first group again", () => {
    const file = readRowGroups(sample(4, 2), { types: TYPES });
    const first = walk(file);
    const second = walk(file);

    expect(second).toEqual(first);
    // Fresh rows every time: nothing is kept between walks, which is the same
    // reason nothing is kept between steps.
    expect(second[0][0]).not.toBe(first[0][0]);
  });

  it("hands back an iterator that is itself iterable", () => {
    const iterator = readRowGroups(sample(4, 2), { types: TYPES })[Symbol.iterator]();
    expect(iterator[Symbol.iterator]()).toBe(iterator);
    // Half-walked, then finished through the for-of: the state is the
    // iterator's, so it picks up where it was left.
    expect(step(iterator)).toEqual(expected(4, 2)[0]);
    expect([...iterator]).toEqual([expected(4, 2)[1]]);
  });

  it("keeps two walks of the same file apart", () => {
    const file = readRowGroups(sample(6, 2), { types: TYPES });
    const groups = expected(6, 2);
    const first = file[Symbol.iterator]();
    const second = file[Symbol.iterator]();

    // Pulled alternately, and neither one moves the other along.
    expect(step(first)).toEqual(groups[0]);
    expect(step(second)).toEqual(groups[0]);
    expect(step(first)).toEqual(groups[1]);
    expect(step(first)).toEqual(groups[2]);
    expect(step(second)).toEqual(groups[1]);
    expect(first.next().done).toBe(true);
    expect(step(second)).toEqual(groups[2]);
    expect(second.next().done).toBe(true);
  });

  it("keeps two walks apart even while both are waiting on a codec", async () => {
    const bytes = sample(6, 2, gzip);
    const groups = expected(6, 2);
    const file = readRowGroups(bytes, { types: TYPES, codecs: asyncCodecs });
    const first = file[Symbol.iterator]();
    const second = file[Symbol.iterator]();

    // Three groups in flight at once, over one file, from two walks.
    const a0 = step(first);
    const b0 = step(second);
    const a1 = step(first);
    expect(await a0).toEqual(groups[0]);
    expect(await b0).toEqual(groups[0]);
    expect(await a1).toEqual(groups[1]);
  });
});

describe("what a step can throw", () => {
  it("throws from the step that reaches the broken group, having yielded the ones before", () => {
    const bytes = breakPage(counting(3), 1, 3);
    const file = readRowGroups(bytes);
    const iterator = file[Symbol.iterator]();

    // The footer is intact, so opening the file said nothing.
    expect(file.groupCount).toBe(3);
    expect(step(iterator)).toEqual([{ n: 0n }]);

    const error = expectError("ERR_READ_UNSUPPORTED", () => iterator.next());
    expect(error.message).toContain("DATA_PAGE_V2");
    expect(error.column).toBe("n");

    // A step that throws has consumed its group: the walk carries on with the
    // next one, and still ends after `groupCount` steps.
    expect(step(iterator)).toEqual([{ n: 2n }]);
    expect(iterator.next().done).toBe(true);
  });

  it("throws from the first step when it is the first group that is broken", () => {
    const file = readRowGroups(breakPage(counting(2), 0, 2));
    const iterator = file[Symbol.iterator]();
    expectError("ERR_READ_UNSUPPORTED", () => iterator.next());
    expect(step(iterator)).toEqual([{ n: 1n }]);
  });

  it("rejects the step that a codec fails on, and no other", async () => {
    const bytes = sample(4, 2, gzip);
    let calls = 0;
    const failing: { GZIP: ReaderCodec } = {
      GZIP: {
        decompress: async (page, size) => {
          calls++;
          if (calls > 10) throw new Error("worker died");
          return gunzipSync(page, { maxOutputLength: size });
        },
      },
    };
    const iterator = readRowGroups(bytes, { types: TYPES, codecs: failing })[Symbol.iterator]();

    // Ten chunks make the first group; the second is where the codec gives up.
    expect(await step(iterator)).toEqual(expected(4, 2)[0]);
    await expectRejection("ERR_READ_MALFORMED", step(iterator));
  });
});

describe("staying synchronous", () => {
  it("never yields a thenable, and never crosses a microtask", () => {
    const file = readRowGroups(sample(6, 2), { types: TYPES });

    // If any step deferred, this microtask would run before the walk ended.
    let deferred = false;
    queueMicrotask(() => {
      deferred = true;
    });
    const groups = walk(file);

    expect(deferred).toBe(false);
    expect(groups).toEqual(expected(6, 2));
  });

  it("stays synchronous through a synchronous decompressor too", () => {
    const bytes = sample(6, 2, gzip);
    let deferred = false;
    queueMicrotask(() => {
      deferred = true;
    });
    const groups = walk(readRowGroups(bytes, { types: TYPES, codecs: syncCodecs }));

    expect(deferred).toBe(false);
    expect(groups).toEqual(expected(6, 2));
  });
});

describe("with an asynchronous decompressor", () => {
  it("yields a promise per step, and the very same rows", async () => {
    const bytes = sample(7, 3, gzip);
    const file: ParquetRowGroups = readRowGroups(bytes, { types: TYPES, codecs: asyncCodecs });
    const groups: ReadRow[][] = [];
    for (const rows of file) {
      expect(rows).toBeInstanceOf(Promise);
      groups.push(await rows);
    }

    expect(groups).toEqual(expected(7, 3));
    // And the same file read through the synchronous codec is the same rows.
    expect(groups).toEqual(walk(readRowGroups(bytes, { types: TYPES, codecs: syncCodecs })));
  });
});

/**
 * The reader's page loop, which no file tavolato writes can reach: one page per
 * column chunk is what its writer emits, several is what everybody else emits
 * (`parquet-mr` and Arrow cut a page roughly every megabyte). A chunk of two
 * pages is decoded *across* the await a codec introduces, which is exactly
 * where a cursor shared between steps comes apart.
 */
function twoPageFile(
  groupCount: number,
  rowsPerPage: number,
  compress?: (body: Uint8Array) => Uint8Array,
): { readonly bytes: Uint8Array; readonly groups: ReadRow[][] } {
  const out = startFile();
  const rowGroups: RowGroupMeta[] = [];
  const groups: ReadRow[][] = [];
  let value = 0n;

  for (let group = 0; group < groupCount; group++) {
    const dataPageOffset = out.length;
    const rows: ReadRow[] = [];
    let uncompressed = 0;
    let compressed = 0;
    for (let page = 0; page < 2; page++) {
      const values: bigint[] = [];
      for (let index = 0; index < rowsPerPage; index++) values.push(value++);
      const written = writeDataPage(out, values, compress);
      uncompressed += written.uncompressedSize;
      compressed += written.compressedSize;
      for (const n of values) rows.push({ n });
    }
    groups.push(rows);
    rowGroups.push({
      columns: [
        {
          name: "n",
          physical: "i64",
          optional: false,
          codec: compress === undefined ? CompressionCodec.UNCOMPRESSED : CODEC_IDS.GZIP,
          numValues: rows.length,
          nullCount: 0,
          dataPageOffset,
          totalUncompressedSize: uncompressed,
          totalCompressedSize: compressed,
        },
      ],
      numRows: rows.length,
      totalByteSize: uncompressed,
      totalCompressedSize: compressed,
      fileOffset: dataPageOffset,
    });
  }

  const footer = encodeFileMetadata(
    [snapshotColumn({ name: "n", type: "i64", optional: false })],
    rowGroups,
    groups.reduce((total, rows) => total + rows.length, 0),
    "probe",
  );
  return { bytes: sealFile(out, footer), groups };
}

describe("a column chunk of more than one page", () => {
  it("reads every page of every group, one step at a time", () => {
    const { bytes, groups } = twoPageFile(3, 2);
    const file = readRowGroups(bytes);

    expect(file.groupCount).toBe(3);
    expect(file.rowCount).toBe(12);
    expect(walk(file)).toEqual(groups);
    // The eager read of the same file agrees, page loop and all.
    expect(sync(readParquet(bytes)).rows).toEqual(groups.flat());
  });

  it("decodes steps that are all in flight at once", async () => {
    const { bytes, groups } = twoPageFile(3, 2, gzipSync);
    const file = readRowGroups(bytes, { codecs: asyncCodecs });

    // Collecting every step and awaiting them together is the natural idiom,
    // and nothing forbids it: each step's synchronous prologue runs before any
    // of them has finished decompressing a page, so a step sharing its cursor
    // with the others would read their pages as its own.
    expect(await Promise.all([...file])).toEqual(groups);

    // Awaiting each step before pulling the next says the same thing.
    const sequential: ReadRow[][] = [];
    for (const rows of readRowGroups(bytes, { codecs: asyncCodecs })) sequential.push(await rows);
    expect(sequential).toEqual(groups);
  });

  it("keeps two walks apart while four multi-page decodes are in flight", async () => {
    const { bytes, groups } = twoPageFile(2, 3, gzipSync);
    const file = readRowGroups(bytes, { codecs: asyncCodecs });
    const first = file[Symbol.iterator]();
    const second = file[Symbol.iterator]();

    const pending = [step(first), step(second), step(first), step(second)];
    expect(await Promise.all(pending)).toEqual([groups[0], groups[0], groups[1], groups[1]]);
  });
});

describe("equivalence with readParquet", () => {
  /** Each case is a file and the options it takes. */
  const cases: readonly { readonly what: string; readonly bytes: Uint8Array }[] = [
    { what: "many groups", bytes: sample(7, 2) },
    { what: "one group", bytes: sample(7, 100) },
    { what: "one row per group", bytes: sample(4, 1) },
    { what: "exactly full groups", bytes: sample(6, 3) },
    { what: "no rows", bytes: sync(createWriter(schema).finish()) },
    { what: "one row", bytes: sample(1, 10) },
    { what: "compressed", bytes: sample(5, 2, gzip) },
  ];

  for (const { what, bytes } of cases) {
    it(`reads the same rows as readParquet: ${what}`, () => {
      const options = { types: TYPES, codecs: syncCodecs };
      const eager = sync(readParquet(bytes, options));
      const lazy = readRowGroups(bytes, options);

      expect(lazy.schema).toEqual(eager.schema);
      expect(lazy.rowCount).toBe(eager.rows.length);
      expect(walk(lazy).flat()).toEqual(eager.rows);
    });
  }
});
