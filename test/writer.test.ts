import { describe, expect, it } from "vitest";
import { createWriter, defineColumnType, defineSchema, readParquet } from "../src/index.ts";
import type { Annotation, LogicalAdapter } from "../src/index.ts";
import { fixedPageBodySize } from "../src/writer.ts";
import { expectError } from "./_errors.ts";
import { sync } from "./_sync.ts";

const MAGIC = "PAR1";
const MAX_PAGE_BYTES = 0x7f_ff_00_00;

function fixedType(
  typeLength: number,
  write: (value: number) => Uint8Array,
): LogicalAdapter<number, Uint8Array> {
  return defineColumnType({
    name: `fixed-${typeLength}`,
    physical: "fixed",
    typeLength,
    matches: (annotation) => annotation.kind === "none",
    annotate: (): Annotation => ({ kind: "none" }),
    read: (raw) => raw as Uint8Array,
    write,
  });
}

function ascii(bytes: Uint8Array, start: number, end: number): string {
  return String.fromCodePoint(...bytes.subarray(start, end));
}

function footerLength(bytes: Uint8Array): number {
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(
    bytes.length - 8,
    true,
  );
}

describe("file envelope", () => {
  const schema = defineSchema({ n: { type: "i64" } });

  it("opens and closes with the PAR1 magic", () => {
    const writer = createWriter(schema);
    writer.append({ n: 1n });
    const bytes = sync(writer.finish());
    expect(ascii(bytes, 0, 4)).toBe(MAGIC);
    expect(ascii(bytes, bytes.length - 4, bytes.length)).toBe(MAGIC);
  });

  it("stores the footer length in the last four bytes before the trailing magic", () => {
    const writer = createWriter(schema);
    writer.append({ n: 1n });
    const bytes = sync(writer.finish());
    const length = footerLength(bytes);
    expect(length).toBeGreaterThan(0);
    expect(length).toBeLessThan(bytes.length - 12);
  });

  it("produces a file that is nothing but magic and footer when empty", () => {
    // Leading magic (4) + footer + footer length (4) + trailing magic (4).
    const bytes = sync(createWriter(schema).finish());
    expect(footerLength(bytes)).toBe(bytes.length - 12);
  });
});

describe("lifecycle", () => {
  const schema = defineSchema({ n: { type: "i64" } });

  it("counts appended rows", () => {
    const writer = createWriter(schema);
    expect(writer.rowCount).toBe(0);
    writer.appendAll([{ n: 1n }, { n: 2n }, { n: 3n }]);
    expect(writer.rowCount).toBe(3);
  });

  it("reports whether it has been finished", () => {
    const writer = createWriter(schema);
    expect(writer.finished).toBe(false);
    writer.finish();
    expect(writer.finished).toBe(true);
  });

  it("refuses appends after finish", () => {
    const writer = createWriter(schema);
    writer.finish();
    expectError("ERR_WRITER_FINISHED", () => writer.append({ n: 1n }));
  });

  it("refuses a second finish", () => {
    const writer = createWriter(schema);
    writer.finish();
    expectError("ERR_WRITER_FINISHED", () => writer.finish());
  });

  it("exposes the schema it was built from", () => {
    const writer = createWriter(schema);
    expect(writer.schema).toBe(schema);
  });

  it("closes the iterable when appendAll rejects a row", () => {
    const writer = createWriter(schema);
    let closed = false;
    function* rows() {
      try {
        yield { n: 1n };
        yield { n: "not an integer" };
        yield { n: 3n };
      } finally {
        closed = true;
      }
    }

    expectError("ERR_ROW_VALUE_INVALID", () =>
      // @ts-expect-error the invalid row is the failure under test
      writer.appendAll(rows()),
    );
    expect(closed).toBe(true);
    expect(writer.rowCount).toBe(1);
  });

  it("preserves the append failure when closing the iterable also throws", () => {
    const writer = createWriter(schema);
    const closeFailure = new Error("close failed");
    let closed = false;
    const rows: Iterable<{ n: bigint }> = {
      [Symbol.iterator]() {
        return {
          next: () => ({ done: false, value: { n: 1n, extra: true } }),
          return: () => {
            closed = true;
            throw closeFailure;
          },
        };
      },
    };

    const error = expectError("ERR_ROW_UNKNOWN_COLUMN", () => writer.appendAll(rows));
    expect(error).not.toBe(closeFailure);
    expect(closed).toBe(true);
  });

  it("does not close the iterable when reading its next value throws", () => {
    const writer = createWriter(schema);
    const failure = new Error("value failed");
    let closed = false;
    const rows: Iterable<{ n: bigint }> = {
      [Symbol.iterator]() {
        return {
          next() {
            return {
              done: false as const,
              get value(): never {
                throw failure;
              },
            };
          },
          return() {
            closed = true;
            return { done: true as const, value: undefined };
          },
        };
      },
    };

    expect(() => writer.appendAll(rows)).toThrow(failure);
    expect(closed).toBe(false);
  });

  it("closes the iterable when attaching to a thenable throws", async () => {
    const writer = createWriter(schema);
    const failure = new Error("then failed");
    let reads = 0;
    // eslint-disable-next-line unicorn/no-thenable -- failing attachment is the point
    const pending = Object.defineProperty({}, "then", {
      get() {
        reads++;
        if (reads === 2) throw failure;
        return (): void => undefined;
      },
    }) as Promise<void>;
    writer.append = () => pending;

    let closes = 0;
    const rows: Iterable<{ n: bigint }> = {
      [Symbol.iterator]() {
        return {
          next: () => ({ done: false, value: { n: 1n } }),
          return: () => {
            closes++;
            return { done: true, value: undefined };
          },
        };
      },
    };

    await expect(writer.appendAll(rows)).rejects.toBe(failure);
    expect(closes).toBe(1);
  });

  it("closes once when a synchronous thenable continuation throws", async () => {
    const writer = createWriter(schema);
    const append = writer.append.bind(writer);
    let appends = 0;
    writer.append = (row) => {
      appends++;
      if (appends !== 1) return append(row);
      return {
        // eslint-disable-next-line unicorn/no-thenable -- synchronous settlement is the point
        then(onFulfilled: () => void) {
          onFulfilled();
        },
      } as Promise<void>;
    };

    let index = 0;
    let closes = 0;
    const rows: Iterable<{ n: bigint }> = {
      [Symbol.iterator]() {
        return {
          next: () =>
            index++ === 0
              ? { done: false, value: { n: 1n } }
              : { done: false, value: { n: 2n, extra: true } },
          return: () => {
            closes++;
            return { done: true, value: undefined };
          },
        };
      },
    };

    await expect(writer.appendAll(rows)).rejects.toMatchObject({ code: "ERR_ROW_UNKNOWN_COLUMN" });
    expect(closes).toBe(1);
  });

  it("does not close when a deferred continuation cannot read the next value", async () => {
    const writer = createWriter(schema);
    const append = writer.append.bind(writer);
    let appends = 0;
    writer.append = (row) => {
      appends++;
      if (appends !== 1) return append(row);
      return {
        // eslint-disable-next-line unicorn/no-thenable -- synchronous settlement is the point
        then(onFulfilled: () => void) {
          onFulfilled();
        },
      } as Promise<void>;
    };

    const failure = new Error("value failed");
    let index = 0;
    let closes = 0;
    const rows: Iterable<{ n: bigint }> = {
      [Symbol.iterator]() {
        return {
          next() {
            if (index++ === 0) return { done: false as const, value: { n: 1n } };
            return {
              done: false as const,
              get value(): never {
                throw failure;
              },
            };
          },
          return() {
            closes++;
            return { done: true as const, value: undefined };
          },
        };
      },
    };

    await expect(writer.appendAll(rows)).rejects.toBe(failure);
    expect(closes).toBe(0);
  });
});

describe("row groups", () => {
  const schema = defineSchema({ n: { type: "i64" } });

  it("grows the file as row groups are flushed", () => {
    const small = createWriter(schema, { rowGroupSize: 1 });
    const large = createWriter(schema, { rowGroupSize: 100 });
    for (let index = 0; index < 20; index++) {
      small.append({ n: BigInt(index) });
      large.append({ n: BigInt(index) });
    }
    // One page (and one chunk of metadata) per row group makes the
    // row-group-per-row file strictly larger.
    expect(sync(small.finish()).length).toBeGreaterThan(sync(large.finish()).length);
  });
});

describe("single-page fixed-width values", () => {
  it("projects the exact flush boundary without allocating a fixed value", () => {
    const levelCount = 504;
    const definitionLevelBytes = 4 + 2 * Math.ceil(levelCount / 8);
    const typeLength = (MAX_PAGE_BYTES - definitionLevelBytes) / 2;

    expect(fixedPageBodySize(typeLength, 2, levelCount, true)).toBe(MAX_PAGE_BYTES);
    expect(fixedPageBodySize(typeLength + 1, 2, levelCount, true)).toBeGreaterThan(MAX_PAGE_BYTES);
  });

  it.each([
    ["required", false, MAX_PAGE_BYTES],
    ["optional", true, MAX_PAGE_BYTES - 6],
  ] as const)("checks the %s boundary before invoking the adapter", (_, optional, largestWidth) => {
    const reached = new Error("adapter reached");
    let exactCalls = 0;
    const exact = fixedType(largestWidth, () => {
      exactCalls++;
      throw reached;
    });
    const exactWriter = createWriter(defineSchema({ v: { type: exact, optional } }));
    const adapted = expectError("ERR_ROW_VALUE_INVALID", () => exactWriter.append({ v: 1 }));
    expect(adapted.cause).toBe(reached);
    expect(exactCalls).toBe(1);

    let oversizedCalls = 0;
    const oversized = fixedType(largestWidth + 1, () => {
      oversizedCalls++;
      throw new Error("must not run");
    });
    const writer = createWriter(defineSchema({ v: { type: oversized, optional } }));
    const error = expectError("ERR_ROW_VALUE_INVALID", () => writer.append({ v: 1 }));
    expect(error.column).toBe("v");
    expect(error.message).toContain("cannot fit a Parquet data page");
    expect(oversizedCalls).toBe(0);
    expect(writer.rowCount).toBe(0);
  });

  it("keeps empty and all-null maximum-width files usable without invoking the adapter", () => {
    let calls = 0;
    const widest = fixedType(0x7f_ff_ff_ff, () => {
      calls++;
      throw new Error("must not run");
    });

    const empty = createWriter(defineSchema({ v: { type: widest } }));
    expect(readParquet(sync(empty.finish()), { types: [widest] }).rows).toEqual([]);

    const nullable = createWriter(defineSchema({ v: { type: widest, optional: true } }));
    nullable.append({ v: null });
    nullable.append({});
    expect(readParquet(sync(nullable.finish()), { types: [widest] }).rows).toEqual([
      { v: null },
      { v: null },
    ]);
    expect(calls).toBe(0);
  });

  it("remains usable after refusing an oversized nullable fixed value", () => {
    const oversized = fixedType(MAX_PAGE_BYTES, () => {
      throw new Error("must not run");
    });
    const writer = createWriter(defineSchema({ v: { type: oversized, optional: true } }));

    expectError("ERR_ROW_VALUE_INVALID", () => writer.append({ v: 1 }));
    writer.append({ v: null });

    expect(writer.rowCount).toBe(1);
    expect(readParquet(sync(writer.finish()), { types: [oversized] }).rows).toEqual([{ v: null }]);
  });
});
