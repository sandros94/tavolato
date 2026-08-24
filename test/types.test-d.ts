import { gunzipSync, gzipSync } from "node:zlib";
import {
  createWriter,
  date,
  decimal,
  defineColumnType,
  defineSchema,
  float16,
  integer,
  readParquet,
  readRowGroups,
  time,
  timestamp,
  uuid,
} from "../src/index.ts";
import type {
  ParquetFile,
  ParquetRowGroups,
  ReadRow,
  ReadRowOf,
  ReadValue,
  SyncParquetRowGroups,
} from "../src/index.ts";

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
  j: { type: "json" },
  f: { type: "f64" },
  g: { type: "f32" },
  i: { type: "i64" },
  n: { type: "i32" },
  b: { type: "bool" },
  t: { type: "timestamp" },
  opt: { type: "string", optional: true },
});

const writer = createWriter(schema);

// Accepted shapes.
writer.append({ s: "x", j: "{}", f: 1, g: 1, i: 1n, n: 1, b: true, t: 0 }); // optional column omitted
writer.append({ s: "x", j: "{}", f: 1, g: 1, i: 1, n: 1, b: true, t: new Date(), opt: null });
writer.append({ s: "x", j: "{}", f: 1, g: 1, i: 1n, n: 1, b: false, t: Date.now(), opt: "y" });
writer.append({ s: "x", j: "{}", f: 1, g: 1, i: 1n, n: 1, b: false, t: 0, opt: undefined });

// @ts-expect-error a required column must not be omitted
writer.append({ j: "{}", f: 1, g: 1, i: 1n, n: 1, b: true, t: 0 });
// @ts-expect-error a required column must not be null
writer.append({ s: null, j: "{}", f: 1, g: 1, i: 1n, n: 1, b: true, t: 0 });
// @ts-expect-error string columns take strings
writer.append({ s: 1, j: "{}", f: 1, g: 1, i: 1n, n: 1, b: true, t: 0 });
// @ts-expect-error json columns take the document as a string, not as an object
writer.append({ s: "x", j: { a: 1 }, f: 1, g: 1, i: 1n, n: 1, b: true, t: 0 });
// @ts-expect-error f64 columns take numbers, not bigints
writer.append({ s: "x", j: "{}", f: 1n, g: 1, i: 1n, n: 1, b: true, t: 0 });
// @ts-expect-error f32 columns take numbers, not bigints
writer.append({ s: "x", j: "{}", f: 1, g: 1n, i: 1n, n: 1, b: true, t: 0 });
// @ts-expect-error i64 columns take bigints or numbers, not strings
writer.append({ s: "x", j: "{}", f: 1, g: 1, i: "1", n: 1, b: true, t: 0 });
// @ts-expect-error i32 columns take plain numbers, not bigints
writer.append({ s: "x", j: "{}", f: 1, g: 1, i: 1n, n: 1n, b: true, t: 0 });
// @ts-expect-error bool columns take booleans
writer.append({ s: "x", j: "{}", f: 1, g: 1, i: 1n, n: 1, b: 1, t: 0 });
// @ts-expect-error timestamp columns take Dates or epoch milliseconds
writer.append({ s: "x", j: "{}", f: 1, g: 1, i: 1n, n: 1, b: true, t: "2026-01-01" });
// @ts-expect-error unknown columns are rejected
writer.append({ s: "x", j: "{}", f: 1, g: 1, i: 1n, n: 1, b: true, t: 0, zz: 1 });
// @ts-expect-error optional columns still reject wrong value types
writer.append({ s: "x", j: "{}", f: 1, g: 1, i: 1n, n: 1, b: true, t: 0, opt: 2 });

/*
 * Maybe-promise surface. Without a codec nothing defers, but the *type* cannot
 * know that, so `append` and `finish` are unions and callers narrow or await.
 * `readParquet` is the exception: its no-options overload keeps the plain
 * return type every existing caller relies on.
 */
// A writer without a codec cannot defer, and is typed accordingly.
const stillVoid: void = writer.append({ s: "x", j: "{}", f: 1, g: 1, i: 1n, n: 1, b: true, t: 0 });
const stillBytes: Uint8Array = writer.finish();
void [stillVoid, stillBytes];

// A codec object is usable on both sides at once.
const both = {
  name: "GZIP",
  compress: gzipSync,
  decompress: (page: Uint8Array) => gunzipSync(page),
} as const;

const packing = createWriter(schema, { codec: both });
const appended: void | Promise<void> = packing.append({
  s: "x",
  j: "{}",
  f: 1,
  g: 1,
  i: 1n,
  n: 1,
  b: true,
  t: 0,
});
const finished: Uint8Array | Promise<Uint8Array> = packing.finish();
void [appended, finished];

// @ts-expect-error a writer with a codec may defer
const notBytes: Uint8Array = createWriter(schema, { codec: both }).finish();
void notBytes;

// Only a codec can make a read defer, and only that overload says so. Column
// types are pure value transforms, so a read that only registers them does not.
const plain: ParquetFile = readParquet(new Uint8Array());
const typed: ParquetFile = readParquet(new Uint8Array(), { types: [uuid()] });
const hooked: ParquetFile | Promise<ParquetFile> = readParquet(new Uint8Array(), {
  codecs: { GZIP: both },
});
void [plain, typed, hooked];

// @ts-expect-error with codecs registered the result may be a promise
const notAFile: ParquetFile = readParquet(new Uint8Array(), { codecs: { GZIP: both } });
void notAFile;

/*
 * The lazy read follows the same overload rule, one step at a time: without a
 * codec a step is plainly the rows, with one it is a maybe-promise. The object
 * itself is never one — the footer is read eagerly either way.
 */
const lazy: SyncParquetRowGroups = readRowGroups(new Uint8Array());
const lazyTyped: SyncParquetRowGroups = readRowGroups(new Uint8Array(), { types: [uuid()] });
const lazyHooked: ParquetRowGroups = readRowGroups(new Uint8Array(), { codecs: { GZIP: both } });
const groupCount: number = lazy.groupCount;
const declaredRows: number = lazy.rowCount;
void [lazyTyped, groupCount, declaredRows];

for (const group of lazy) {
  const rows: ReadRow[] = group; // no codec: a step is the rows themselves
  const known = rows as ReadRowOf<typeof schema.definition>[];
  const asCount: bigint = known[0].i;
  void [rows, asCount];
}

for (const group of lazyHooked) {
  const rows: ReadRow[] | Promise<ReadRow[]> = group;
  void rows;
}

for (const group of lazyHooked) {
  // @ts-expect-error with codecs registered a step may be a promise
  const rows: ReadRow[] = group;
  void rows;
}

// @ts-expect-error a lazy read with codecs is not the synchronous shape
const notSync: SyncParquetRowGroups = readRowGroups(new Uint8Array(), { codecs: { GZIP: both } });
void notSync;

// @ts-expect-error the counts the footer declared are read-only
lazy.rowCount = 1;

// @ts-expect-error a writer codec must be able to compress
void createWriter(schema, { codec: { name: "GZIP", decompress: (page: Uint8Array) => page } });
// @ts-expect-error a reader codec must be able to decompress
void readParquet(new Uint8Array(), { codecs: { GZIP: { compress: gzipSync } } });
const notACodec = { decompress: (page: Uint8Array) => page };
// @ts-expect-error only the codecs Parquet names can be registered
void readParquet(new Uint8Array(), { codecs: { DEFLATE: notACodec } });

// `ReadValue` carries the raw-binary member reserved for a future column type.
const binary: ReadValue = new Uint8Array();
void binary;

/**
 * The read side. `readParquet` cannot know a file's schema, so its rows are
 * loosely typed; `ReadRowOf` is what to assert onto them when the schema is
 * known, and it narrows each column to a single type — no `bigint | number`.
 */
const read = readParquet(new Uint8Array()).rows as ReadRowOf<typeof schema.definition>[];

const first = read[0];
const asString: string = first.s;
const asJson: string = first.j; // a json column is a string in and a string out
const asNumber: number = first.f;
const asSingle: number = first.g;
const asBigint: bigint = first.i;
const asInt32: number = first.n;
const asBoolean: boolean = first.b;
const asDate: Date = first.t;
const asNullable: string | null = first.opt;
void [asString, asJson, asNumber, asSingle, asBigint, asInt32, asBoolean, asDate, asNullable];

// @ts-expect-error i64 reads back as a bigint, never a number
const notANumber: number = first.i;
// @ts-expect-error i32 reads back as a number, never a bigint
const notABigint: bigint = first.n;
void notABigint;
// @ts-expect-error timestamp reads back as a Date, never epoch millis
const notMillis: number = first.t;
// @ts-expect-error an optional column may be null
const notNullable: string = first.opt;
// @ts-expect-error a required column is never null
const notNull: null = first.s;
void [notANumber, notMillis, notNullable, notNull];

/*
 * Column types carry their own pair of JavaScript types, and both halves flow
 * through `defineSchema` without a single generic parameter appearing on
 * `ParquetSchema`, `Row` or the writer.
 */
const money = decimal({ precision: 12, scale: 2 });
const ratio = defineColumnType({
  name: "ratio",
  physical: "i64",
  matches: (annotation) => annotation.kind === "decimal" && annotation.precision === 18,
  annotate: () => ({ kind: "decimal", precision: 18, scale: 6 }),
  read: (raw) => Number(raw as bigint) / 1e6,
  write: (value: number) => BigInt(Math.round(value * 1e6)),
});

const logical = defineSchema({
  when: { type: date() },
  price: { type: money },
  id: { type: uuid() },
  clock: { type: time({ unit: "millis" }) },
  precise: { type: time({ unit: "micros" }) },
  at: { type: timestamp({ unit: "micros" }) },
  half: { type: float16() },
  small: { type: integer({ bitWidth: 8 }) },
  big: { type: integer({ bitWidth: 64, signed: false }) },
  share: { type: ratio },
  maybe: { type: uuid(), optional: true },
});

const logicalWriter = createWriter(logical);
const priced = {
  when: new Date(0),
  price: "1.00",
  id: "b3f2c1a0-1111-4222-8333-444455556666",
  clock: 0,
  precise: 0n,
  at: 0n,
  half: 1.5,
  small: -1,
  big: 1n,
  share: 0.25,
  maybe: null,
};
logicalWriter.append(priced);

// @ts-expect-error a decimal column takes its canonical string, not a number
logicalWriter.append({ ...priced, price: 1 });
// @ts-expect-error a date column takes a Date, not epoch milliseconds
logicalWriter.append({ ...priced, when: 0 });
// @ts-expect-error a uuid column takes its canonical string, not bytes
logicalWriter.append({ ...priced, id: new Uint8Array(16) });
// @ts-expect-error time(millis) is a number, not a bigint
logicalWriter.append({ ...priced, clock: 0n });
// @ts-expect-error time(micros) is a bigint, not a number
logicalWriter.append({ ...priced, precise: 0 });
// @ts-expect-error timestamp(micros) is a bigint, not a Date
logicalWriter.append({ ...priced, at: new Date() });
// @ts-expect-error integer(8) is a number, not a bigint
logicalWriter.append({ ...priced, small: 1n });
// @ts-expect-error integer(64) is a bigint, not a number
logicalWriter.append({ ...priced, big: 1 });
// @ts-expect-error a hand-written type maps from whatever its write() takes
logicalWriter.append({ ...priced, share: "0.25" });

const logicalRows = readParquet(new Uint8Array(), { types: [money] }).rows as ReadRowOf<
  typeof logical.definition
>[];
const logicalRow = logicalRows[0];
const asPrice: string = logicalRow.price;
const asDay: Date = logicalRow.when;
const asUuid: string = logicalRow.id;
const asMillis: number = logicalRow.clock;
const asMicros: bigint = logicalRow.precise;
const asInstant: bigint = logicalRow.at;
const asHalf: number = logicalRow.half;
const asTiny: number = logicalRow.small;
const asUnsigned: bigint = logicalRow.big;
// A hand-written type maps between two *different* JavaScript types, and the
// read side gets the one its `read` returns.
const asShare: number = logicalRow.share;
const asMaybeUuid: string | null = logicalRow.maybe;
void [
  asPrice,
  asDay,
  asUuid,
  asMillis,
  asMicros,
  asInstant,
  asHalf,
  asTiny,
  asUnsigned,
  asShare,
  asMaybeUuid,
];

// @ts-expect-error a decimal column reads back as its canonical string
const notPrice: number = logicalRow.price;
// @ts-expect-error a uuid column reads back as a string, never as bytes
const notRawBytes: Uint8Array = logicalRow.id;
// @ts-expect-error an optional adapter column may be null
const notMaybe: string = logicalRow.maybe;
void [notPrice, notRawBytes, notMaybe];
