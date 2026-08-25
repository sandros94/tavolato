import { describe, expect, expectTypeOf, it } from "vitest";
import { roundTripInvoice } from "../playgrounds/adapters.ts";
import { roundTripCompressed } from "../playgrounds/compression.ts";
import { roundTripAmount } from "../playgrounds/custom-column.ts";
import { storeEvents } from "../playgrounds/object-store.ts";
import { roundTripEvents } from "../playgrounds/quick-start.ts";
import { sumOneGroupAtATime } from "../playgrounds/row-groups.ts";
import { FakeS3 } from "./_store.ts";

describe("README playgrounds", () => {
  it("round-trips the quick start", () => {
    const file = roundTripEvents();
    const { rows } = file;

    expectTypeOf(file).toHaveProperty("rows");
    expect(rows).toEqual([
      { at: new Date("2026-08-25T00:00:00Z"), host: "web-1", count: 42n },
      { at: new Date("2026-08-25T01:00:00Z"), host: null, count: 7n },
    ]);
  });

  it("round-trips built-in adapters", () => {
    const id = "123e4567-e89b-42d3-a456-426614174000";
    expect(roundTripInvoice(id).rows).toEqual([
      { id, issued: new Date("2026-08-25T00:00:00Z"), total: "19.99" },
    ]);
  });

  it("round-trips a custom column", () => {
    expect(roundTripAmount(19.99)).toBe(19.99);
  });

  it("round-trips GZIP compression", async () => {
    await expect(roundTripCompressed()).resolves.toBe("compressed");
  });

  it("reads one row group at a time", () => {
    expect(sumOneGroupAtATime()).toBe(6n);
  });

  it("writes and selectively reads an object", async () => {
    const result = await storeEvents(new FakeS3());

    expect(result.head).toMatchObject({ rowCount: 1, groupCount: 1 });
    expect(result.rows).toEqual([{ count: 1n }]);
  });
});
