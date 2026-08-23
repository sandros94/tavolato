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
