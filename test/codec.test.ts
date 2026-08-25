import { gunzipSync, gzipSync } from "node:zlib";
import { describe, expect, expectTypeOf, it } from "vitest";
import {
  createWriter,
  defineSchema,
  isTavolatoError,
  readParquet,
  uuid,
  TavolatoError,
} from "../src/index.ts";
import type { ParquetFile, ReaderCodec, ReadOptions, Row, WriterCodec } from "../src/index.ts";
import { errorCode, expectError, expectRejection } from "./_errors.ts";
import { sync } from "./_sync.ts";

/**
 * The compression hooks, from both ends.
 *
 * tavolato ships no compressor and never will; what it ships is the promise
 * that whatever you hand it is called correctly, held to its word, and — when
 * it lies, throws or hangs on a byte — turned into the same typed error as any
 * other bad input. These are the tests for that promise, not for zlib's.
 *
 * The other half of the story lives in `roundtrip.test.ts`, which replays the
 * entire value suite through GZIP and ZSTD, and in `duckdb.test.ts`, which
 * checks the resulting files against a reader that is not tavolato.
 */

const schema = defineSchema({ n: { type: "i64" }, s: { type: "string", optional: true } });

/** Twenty rows, enough to make compression do something. */
function rows(count = 20): Row<typeof schema.definition>[] {
  return Array.from({ length: count }, (_, index) => ({
    n: BigInt(index),
    s: index % 3 === 0 ? null : `value-${index}`,
  }));
}

const gzip: WriterCodec = {
  name: "GZIP",
  compress: gzipSync,
  decompress: (page) => gunzipSync(page),
};

/** The same codec, one microtask later. */
const asyncGzip: WriterCodec = {
  name: "GZIP",
  compress: async (page) => gzipSync(page),
  decompress: async (page) => gunzipSync(page),
};

function readerCodecs(codec: WriterCodec): { GZIP: ReaderCodec } {
  return { GZIP: { decompress: (page, size) => codec.decompress!(page, size) } };
}

/** Writes `rows()` with a synchronous codec and returns the file. */
function compressed(codec: WriterCodec = gzip, rowGroupSize?: number): Uint8Array {
  const writer = createWriter(schema, {
    codec,
    ...(rowGroupSize === undefined ? {} : { rowGroupSize }),
  });
  sync(writer.appendAll(rows()));
  return sync(writer.finish());
}

describe("codec option", () => {
  it("refuses anything that cannot compress", () => {
    for (const codec of [null, "GZIP", {}, { name: "GZIP" }, { name: "GZIP", compress: 1 }]) {
      expectError("ERR_WRITER_OPTION_INVALID", () =>
        // @ts-expect-error deliberately wrong input
        createWriter(schema, { codec }),
      );
    }
  });

  it("refuses a name that stands for no codec", () => {
    for (const name of ["gzip", "UNCOMPRESSED", "DEFLATE", 2]) {
      const error = expectError("ERR_WRITER_OPTION_INVALID", () =>
        // @ts-expect-error deliberately wrong input
        createWriter(schema, { codec: { name, compress: gzipSync } }),
      );
      expect(error.message).toContain("LZ4_RAW");
    }
  });
});

describe("synchronous codecs", () => {
  it("never returns a promise, anywhere", () => {
    const writer = createWriter(schema, { codec: gzip, rowGroupSize: 4 });
    const plainWriter = createWriter(schema);
    const plainRead = (bytes: Uint8Array) => readParquet(bytes);
    const typedRead = (bytes: Uint8Array) => readParquet(bytes, { types: [uuid()] });
    const hookedRead = (bytes: Uint8Array) => readParquet(bytes, { codecs: readerCodecs(gzip) });
    const dual = {
      name: "GZIP",
      compress: gzipSync,
      decompress: (page: Uint8Array) => gunzipSync(page),
    } as const;

    expectTypeOf<ReturnType<typeof plainWriter.append>>().toEqualTypeOf<void>();
    expectTypeOf<ReturnType<typeof plainWriter.finish>>().toEqualTypeOf<Uint8Array>();
    expectTypeOf<ReturnType<typeof writer.append>>().toEqualTypeOf<void | Promise<void>>();
    expectTypeOf<ReturnType<typeof writer.finish>>().toEqualTypeOf<
      Uint8Array | Promise<Uint8Array>
    >();
    expectTypeOf<ReturnType<typeof plainRead>>().toEqualTypeOf<ParquetFile>();
    expectTypeOf<ReturnType<typeof typedRead>>().toEqualTypeOf<ParquetFile>();
    expectTypeOf<ReturnType<typeof hookedRead>>().toEqualTypeOf<
      ParquetFile | Promise<ParquetFile>
    >();
    expectTypeOf(dual).toExtend<WriterCodec>();
    expectTypeOf(dual).toExtend<ReaderCodec>();
    expectTypeOf<{
      name: "GZIP";
      decompress(page: Uint8Array): Uint8Array;
    }>().not.toExtend<WriterCodec>();
    expectTypeOf<{ compress(page: Uint8Array): Uint8Array }>().not.toExtend<ReaderCodec>();
    expectTypeOf<"DEFLATE">().not.toExtend<keyof NonNullable<ReadOptions["codecs"]>>();
    for (const row of rows()) expect(writer.append(row)).not.toBeInstanceOf(Promise);
    const bytes = writer.finish();
    expect(bytes).not.toBeInstanceOf(Promise);
    expect(readParquet(bytes as Uint8Array, { codecs: readerCodecs(gzip) })).not.toBeInstanceOf(
      Promise,
    );
  });

  it("actually shrinks the pages", () => {
    // A thousand repetitive strings: if the codec were being skipped, the two
    // files would be the same size.
    const many = Array.from({ length: 1000 }, (_, index) => ({
      n: BigInt(index),
      s: "the same string over and over",
    }));
    const plain = createWriter(schema);
    plain.appendAll(many);
    const packed = createWriter(schema, { codec: gzip });
    sync(packed.appendAll(many));
    expect(sync(packed.finish()).length).toBeLessThan(sync(plain.finish()).length / 2);
  });
});

describe("asynchronous codecs", () => {
  it("defers only where a row group is flushed", async () => {
    const writer = createWriter(schema, { codec: asyncGzip, rowGroupSize: 4 });
    const deferred: number[] = [];
    for (const [index, row] of rows(8).entries()) {
      const pending = writer.append(row);
      if (pending instanceof Promise) {
        deferred.push(index);
        await pending;
      }
    }
    // Rows 3 and 7 close their row group; the other six never leave the stack.
    expect(deferred).toEqual([3, 7]);
    // Nothing is buffered, so the final flush is a no-op and finish stays sync.
    expect(writer.finish()).not.toBeInstanceOf(Promise);
  });

  it("produces exactly the bytes the synchronous codec would", async () => {
    const writer = createWriter(schema, { codec: asyncGzip, rowGroupSize: 6 });
    await writer.appendAll(rows());
    expect(await writer.finish()).toEqual(compressed(gzip, 6));
  });

  it("drives a whole iterable through appendAll", async () => {
    const writer = createWriter(schema, { codec: asyncGzip, rowGroupSize: 1 });
    const pending = writer.appendAll(rows(5));
    expect(pending).toBeInstanceOf(Promise);
    await pending;
    expect(writer.rowCount).toBe(5);
    const file = await readParquet(await writer.finish(), { codecs: readerCodecs(asyncGzip) });
    expect(file.rows).toEqual(rows(5).map((row) => ({ n: row.n, s: row.s ?? null })));
  });

  it("closes the iterable when appendAll's deferred append rejects", async () => {
    const boom = new Error("worker died");
    const writer = createWriter(schema, {
      rowGroupSize: 1,
      codec: { name: "GZIP", compress: () => Promise.reject(boom) },
    });
    let closed = false;
    function* input() {
      try {
        yield { n: 1n };
        yield { n: 2n };
      } finally {
        closed = true;
      }
    }

    const error = await expectRejection("ERR_WRITER_CODEC_FAILED", writer.appendAll(input()));
    expect(error.cause).toBe(boom);
    expect(closed).toBe(true);
    expect(writer.rowCount).toBe(1);
  });

  it("preserves a deferred append failure when closing the iterable also throws", async () => {
    const boom = new Error("worker died");
    const writer = createWriter(schema, {
      rowGroupSize: 1,
      codec: { name: "GZIP", compress: () => Promise.reject(boom) },
    });
    let closed = false;
    const input: Iterable<Row<typeof schema.definition>> = {
      [Symbol.iterator]() {
        return {
          next: () => ({ done: false, value: { n: 1n } }),
          return: () => {
            closed = true;
            throw new Error("close failed");
          },
        };
      },
    };

    const error = await expectRejection("ERR_WRITER_CODEC_FAILED", writer.appendAll(input));
    expect(error.cause).toBe(boom);
    expect(closed).toBe(true);
  });

  it("refuses re-entry while a row group is still being compressed", async () => {
    const writer = createWriter(schema, { codec: asyncGzip, rowGroupSize: 1 });
    const pending = writer.append({ n: 1n });
    expect(pending).toBeInstanceOf(Promise);

    expectError("ERR_WRITER_BUSY", () => writer.append({ n: 2n }));
    const error = expectError("ERR_WRITER_BUSY", () => writer.finish());
    expect(error.message).toContain("await");

    // Awaiting hands the writer back, unharmed.
    await pending;
    await writer.append({ n: 2n });
    const file = await readParquet(await writer.finish(), { codecs: readerCodecs(asyncGzip) });
    expect(file.rows.map((row) => row.n)).toEqual([1n, 2n]);
  });

  it("reads back through an asynchronous decompressor", async () => {
    const file = await readParquet(compressed(), { codecs: readerCodecs(asyncGzip) });
    expect(file.rows).toEqual(rows().map((row) => ({ n: row.n, s: row.s ?? null })));
  });
});

/**
 * A writer mid-row-group is a torn thing: the column buffers are already
 * detached, the page offsets are already taken, and the bytes are not written
 * yet. Nothing may run against it in that state — not a caller who forgot to
 * await, not a codec that calls back in. The guard therefore covers a whole
 * operation, footer included, whether it sits on the stack or on a microtask.
 */
describe("re-entrancy", () => {
  it("stays busy for the whole of an asynchronous finish, footer included", async () => {
    const writer = createWriter(schema, { codec: asyncGzip });
    sync(writer.appendAll(rows(3))); // default rowGroupSize: nothing flushed yet
    expect(writer.rowCount).toBe(3);

    const pending = writer.finish();
    expect(pending).toBeInstanceOf(Promise);

    // Poke on every microtask until the writer reports itself finished. The gap
    // this closes is the one between the last page settling and the footer
    // being written, and it is exactly one microtask wide.
    const attempts: string[] = [];
    const poke = (): void => {
      if (writer.finished || attempts.length > 200) return;
      attempts.push(errorCode(() => writer.append({ n: 99n })));
      attempts.push(errorCode(() => writer.finish()));
      queueMicrotask(poke);
    };
    queueMicrotask(poke);

    const bytes = await pending;
    expect(attempts.length).toBeGreaterThanOrEqual(2);
    expect([...new Set(attempts)]).toEqual(["ERR_WRITER_BUSY"]);

    // And the file the original call promised is whole.
    const file = await readParquet(bytes, { codecs: readerCodecs(asyncGzip) });
    expect(file.rows).toEqual(rows(3).map((row) => ({ n: row.n, s: row.s ?? null })));
  });

  it("refuses a codec that calls back into the writer mid-flush", () => {
    let reentry: string | undefined;
    const single = defineSchema({ n: { type: "i64" } });
    const writer = createWriter(single, {
      rowGroupSize: 1,
      codec: {
        name: "GZIP",
        compress: (page) => {
          reentry ??= errorCode(() => writer.append({ n: 999n }));
          return gzipSync(page);
        },
      },
    });
    sync(writer.append({ n: 1n }));
    sync(writer.append({ n: 2n }));
    const bytes = sync(writer.finish());

    // The refused row changed nothing — it never reached a buffer — so the file
    // holds exactly the two rows that were accepted, in order.
    expect(reentry).toBe("ERR_WRITER_BUSY");
    expect(writer.rowCount).toBe(2);
    expect(sync(readParquet(bytes, { codecs: readerCodecs(gzip) })).rows).toEqual([
      { n: 1n },
      { n: 2n },
    ]);
  });

  it("poisons the writer when a codec lets that refusal escape", () => {
    const single = defineSchema({ n: { type: "i64" } });
    const writer = createWriter(single, {
      rowGroupSize: 1,
      codec: {
        name: "GZIP",
        compress: (page) => {
          void writer.append({ n: 999n }); // throws, and this codec does not catch
          return gzipSync(page);
        },
      },
    });
    const error = expectError("ERR_WRITER_CODEC_FAILED", () => writer.append({ n: 1n }));
    expect(isTavolatoError(error.cause, "ERR_WRITER_BUSY")).toBe(true);
    expectError("ERR_WRITER_CODEC_FAILED", () => writer.finish());
  });
});

describe("a thenable that is not a promise", () => {
  /**
   * Callback-style: `then` runs the callback but returns nothing to chain on,
   * as a decade of pre-Promise libraries did. Thenables are recognised by duck
   * typing, so without a check the `undefined` this hands back would travel on
   * in place of the page bytes.
   */
  function sloppy(value: Uint8Array): Promise<Uint8Array> {
    return {
      // eslint-disable-next-line unicorn/no-thenable -- being one is the point
      then(onFulfilled: (value: Uint8Array) => unknown) {
        onFulfilled(value);
      },
    } as unknown as Promise<Uint8Array>;
  }

  it("is refused by the writer, which poisons rather than write nothing", () => {
    const writer = createWriter(schema, {
      rowGroupSize: 1,
      codec: { name: "GZIP", compress: (page) => sloppy(gzipSync(page)) },
    });
    const error = expectError("ERR_WRITER_CODEC_FAILED", () => writer.append({ n: 1n }));
    expect(error.message).toContain("then");
    // The row group's rows are already detached, so there is no going back.
    expectError("ERR_WRITER_CODEC_FAILED", () => writer.finish());
  });

  it("is refused by the reader as a malformed page", () => {
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(compressed(), {
        codecs: { GZIP: { decompress: (page) => sloppy(gunzipSync(page)) } },
      }),
    );
    expect(error.message).toContain("then");
    expect(error.column).toBe("n");
  });
});

describe("a codec that fails while writing", () => {
  /** Builds a writer whose codec misbehaves in `compress`, with one row buffered. */
  function writerWith(compress: WriterCodec["compress"]) {
    const writer = createWriter(schema, { codec: { name: "GZIP", compress }, rowGroupSize: 2 });
    // One row of two: nothing is flushed yet, so nothing has reached the codec.
    sync(writer.append({ n: 1n }));
    return writer;
  }

  it("surfaces a throw, with the original as the cause", () => {
    const boom = new Error("no compressor here");
    const writer = writerWith(() => {
      throw boom;
    });
    const error = expectError("ERR_WRITER_CODEC_FAILED", () => writer.append({ n: 2n }));
    expect(error.cause).toBe(boom);
    expect(error.message).toContain("GZIP");
  });

  it("surfaces a rejection, with the original as the cause", async () => {
    const boom = new Error("worker died");
    const writer = writerWith(() => Promise.reject(boom));
    const error = await expectRejection("ERR_WRITER_CODEC_FAILED", writer.append({ n: 2n }));
    expect(error.cause).toBe(boom);
  });

  it("refuses a compressed page that is not bytes", () => {
    // @ts-expect-error deliberately wrong output
    const writer = writerWith(() => "compressed, honest");
    const error = expectError("ERR_WRITER_CODEC_FAILED", () => writer.append({ n: 2n }));
    expect(error.message).toContain("Uint8Array");
  });

  it("refuses a compressed page too large for a page header", () => {
    // A real 2 GiB buffer would cost 2 GiB; a subclass that only *claims* the
    // length exercises the same guard for four bytes.
    class Oversized extends Uint8Array {
      override get length(): number {
        return 0x7f_ff_00_01;
      }
    }
    const writer = writerWith(() => new Oversized(4));
    const error = expectError("ERR_WRITER_CODEC_FAILED", () => writer.append({ n: 2n }));
    expect(error.message).toContain("2147418113");
  });

  it("leaves the writer unusable, saying so every time", async () => {
    const writer = writerWith(() => Promise.reject(new Error("gone")));
    await expectRejection("ERR_WRITER_CODEC_FAILED", writer.append({ n: 2n }));
    // Not ERR_WRITER_BUSY: the flush settled, it just settled badly.
    const again = expectError("ERR_WRITER_CODEC_FAILED", () => writer.append({ n: 3n }));
    expect(again.message).toContain("unusable");
    expectError("ERR_WRITER_CODEC_FAILED", () => writer.finish());
  });
});

describe("a file compressed with a codec that is not registered", () => {
  it("is refused by name, and says how to read it anyway", () => {
    const error = expectError("ERR_READ_UNSUPPORTED", () => readParquet(compressed()));
    expect(error.message).toContain("compressed with GZIP");
    expect(error.message).toContain("tavolato only reads the files it writes");
    expect(error.message).toContain("register a decompressor for GZIP in ReadOptions.codecs");
    expect(error.column).toBe("n");
  });

  it("is refused just the same when the codecs object holds no entry for it", () => {
    expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(compressed(), { codecs: { ZSTD: { decompress: (page) => page } } }),
    );
  });

  it("names a codec tavolato has no name for, and offers no remedy", () => {
    // Two one-column files that differ in nothing but the codec id locate the
    // byte that holds it; patching it to an id no Parquet release defines is
    // then a one-byte edit rather than a hand-built footer.
    const identity: WriterCodec["compress"] = (page) => page;
    const stamped = (name: WriterCodec["name"]): Uint8Array => {
      const writer = createWriter(defineSchema({ n: { type: "i64" } }), {
        codec: { name, compress: identity },
      });
      sync(writer.append({ n: 1n }));
      return sync(writer.finish());
    };
    const lzo = stamped("LZO");
    const brotli = stamped("BROTLI");
    expect(lzo.length).toBe(brotli.length);
    const differing = [...lzo.keys()].filter((offset) => lzo[offset] !== brotli[offset]);
    expect(differing).toHaveLength(1);

    const patched = lzo.slice();
    patched[differing[0]] = 18; // zigzag(9): one past LZ4_RAW
    const error = expectError("ERR_READ_UNSUPPORTED", () => readParquet(patched));
    expect(error.message).toContain("codec 9");
    expect(error.message).not.toContain("register a decompressor");
  });
});

describe("a decompressor that fails while reading", () => {
  /** Reads a GZIP file through `decompress`, whatever it does. */
  function read(decompress: ReaderCodec["decompress"]): ParquetFile | Promise<ParquetFile> {
    return readParquet(compressed(), { codecs: { GZIP: { decompress } } });
  }

  it("surfaces a throw as a malformed file, with the original as the cause", () => {
    const boom = new Error("not a gzip member");
    const error = expectError("ERR_READ_MALFORMED", () =>
      read(() => {
        throw boom;
      }),
    );
    expect(error.cause).toBe(boom);
    expect(error.column).toBe("n");
  });

  it("surfaces a rejection the same way", async () => {
    const boom = new Error("worker died");
    const error = await expectRejection(
      "ERR_READ_MALFORMED",
      read(() => Promise.reject(boom)),
    );
    expect(error.cause).toBe(boom);
  });

  it("refuses a result of the wrong length", () => {
    const error = expectError("ERR_READ_MALFORMED", () =>
      read((page, size) => gunzipSync(page).subarray(0, size - 1)),
    );
    expect(error.message).toContain("uncompressed bytes");
  });

  it("refuses a result that is not bytes at all", () => {
    // @ts-expect-error deliberately wrong output
    expectError("ERR_READ_MALFORMED", () => read(() => "here you go"));
  });

  it("is never called for a file that was never compressed", () => {
    const plain = createWriter(schema);
    plain.appendAll(rows());
    const file = sync(
      readParquet(sync(plain.finish()), {
        codecs: {
          GZIP: {
            decompress: () => {
              throw new Error("must not be called");
            },
          },
        },
      }),
    );
    expect(file.rows).toEqual(rows().map((row) => ({ n: row.n, s: row.s ?? null })));
  });
});

describe("corruption inside a compressed page", () => {
  it("comes back as a typed error, never as a bare zlib one", () => {
    const single = defineSchema({ n: { type: "i64" } });
    const writer = createWriter(single, { codec: gzip });
    for (let index = 0; index < 200; index++) sync(writer.append({ n: BigInt(index) }));
    const bytes = sync(writer.finish());

    // The page body is a whole RFC 1952 member, so its signature marks where it
    // starts; ten bytes in is past the gzip header and well inside the deflate
    // stream, which is the part no reader can sanity-check for itself.
    const start = bytes.indexOf(0x1f, 4);
    expect([bytes[start], bytes[start + 1], bytes[start + 2]]).toEqual([0x1f, 0x8b, 0x08]);

    const options = { codecs: readerCodecs(gzip) };
    const codes = new Set<string>();
    let causes = 0;
    let read = 0;
    for (let offset = start + 10; offset < start + 150; offset++) {
      const copy = bytes.slice();
      copy[offset] ^= 0xff;
      try {
        sync(readParquet(copy, options));
        read++;
      } catch (error) {
        if (!(error instanceof TavolatoError)) throw error;
        codes.add(error.code);
        if (error.cause !== undefined) causes++;
      }
    }

    // Every flip is caught — a GZIP member carries its own CRC — and every one
    // of them arrives as tavolato's error, not zlib's.
    expect([...codes]).toEqual(["ERR_READ_MALFORMED"]);
    expect(read).toBe(0);
    expect(causes).toBeGreaterThan(0);
  });
});
