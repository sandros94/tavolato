import { describe, expect, it } from "vitest";
import { createWriter, defineSchema } from "../src/index.ts";
import { expectError } from "./_errors.ts";
import { sync } from "./_sync.ts";

const MAGIC = "PAR1";

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
