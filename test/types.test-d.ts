import { createWriter, defineSchema, readParquet } from "../src/index.ts";
import type { ReadRowOf } from "../src/index.ts";

/**
 * Compile-time contract of `Row<S>`: required columns are mandatory and
 * non-null, optional columns may be omitted or null, every column only accepts
 * the inputs its type declares, and unknown columns are rejected.
 *
 * This file is checked by `tsc --noEmit` (the `@ts-expect-error` lines fail the
 * build if the corresponding assignment ever becomes legal); vitest never
 * executes it.
 */

const schema = defineSchema({
  s: { type: "string" },
  f: { type: "f64" },
  i: { type: "i64" },
  b: { type: "bool" },
  t: { type: "timestamp" },
  opt: { type: "string", optional: true },
});

const writer = createWriter(schema);

// Accepted shapes.
writer.append({ s: "x", f: 1, i: 1n, b: true, t: 0 }); // optional column omitted
writer.append({ s: "x", f: 1, i: 1, b: true, t: new Date(), opt: null });
writer.append({ s: "x", f: 1, i: 1n, b: false, t: Date.now(), opt: "y" });
writer.append({ s: "x", f: 1, i: 1n, b: false, t: 0, opt: undefined });

// @ts-expect-error a required column must not be omitted
writer.append({ f: 1, i: 1n, b: true, t: 0 });
// @ts-expect-error a required column must not be null
writer.append({ s: null, f: 1, i: 1n, b: true, t: 0 });
// @ts-expect-error string columns take strings
writer.append({ s: 1, f: 1, i: 1n, b: true, t: 0 });
// @ts-expect-error f64 columns take numbers, not bigints
writer.append({ s: "x", f: 1n, i: 1n, b: true, t: 0 });
// @ts-expect-error i64 columns take bigints or numbers, not strings
writer.append({ s: "x", f: 1, i: "1", b: true, t: 0 });
// @ts-expect-error bool columns take booleans
writer.append({ s: "x", f: 1, i: 1n, b: 1, t: 0 });
// @ts-expect-error timestamp columns take Dates or epoch milliseconds
writer.append({ s: "x", f: 1, i: 1n, b: true, t: "2026-01-01" });
// @ts-expect-error unknown columns are rejected
writer.append({ s: "x", f: 1, i: 1n, b: true, t: 0, zz: 1 });
// @ts-expect-error optional columns still reject wrong value types
writer.append({ s: "x", f: 1, i: 1n, b: true, t: 0, opt: 2 });

/**
 * The read side. `readParquet` cannot know a file's schema, so its rows are
 * loosely typed; `ReadRowOf` is what to assert onto them when the schema is
 * known, and it narrows each column to a single type — no `bigint | number`.
 */
const read = readParquet(new Uint8Array()).rows as ReadRowOf<typeof schema.definition>[];

const first = read[0];
const asString: string = first.s;
const asNumber: number = first.f;
const asBigint: bigint = first.i;
const asBoolean: boolean = first.b;
const asDate: Date = first.t;
const asNullable: string | null = first.opt;
void [asString, asNumber, asBigint, asBoolean, asDate, asNullable];

// @ts-expect-error i64 reads back as a bigint, never a number
const notANumber: number = first.i;
// @ts-expect-error timestamp reads back as a Date, never epoch millis
const notMillis: number = first.t;
// @ts-expect-error an optional column may be null
const notNullable: string = first.opt;
// @ts-expect-error a required column is never null
const notNull: null = first.s;
void [notANumber, notMillis, notNullable, notNull];
