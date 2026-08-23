import { gunzipSync, gzipSync } from "node:zlib";
import { createWriter, defineSchema, readParquet } from "../src/index.ts";
import type { ParquetFile, ReadRowOf, ReadValue } from "../src/index.ts";

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
  i: { type: "i64" },
  b: { type: "bool" },
  t: { type: "timestamp" },
  opt: { type: "string", optional: true },
});

const writer = createWriter(schema);

// Accepted shapes.
writer.append({ s: "x", j: "{}", f: 1, i: 1n, b: true, t: 0 }); // optional column omitted
writer.append({ s: "x", j: "{}", f: 1, i: 1, b: true, t: new Date(), opt: null });
writer.append({ s: "x", j: "{}", f: 1, i: 1n, b: false, t: Date.now(), opt: "y" });
writer.append({ s: "x", j: "{}", f: 1, i: 1n, b: false, t: 0, opt: undefined });

// @ts-expect-error a required column must not be omitted
writer.append({ j: "{}", f: 1, i: 1n, b: true, t: 0 });
// @ts-expect-error a required column must not be null
writer.append({ s: null, j: "{}", f: 1, i: 1n, b: true, t: 0 });
// @ts-expect-error string columns take strings
writer.append({ s: 1, j: "{}", f: 1, i: 1n, b: true, t: 0 });
// @ts-expect-error json columns take the document as a string, not as an object
writer.append({ s: "x", j: { a: 1 }, f: 1, i: 1n, b: true, t: 0 });
// @ts-expect-error f64 columns take numbers, not bigints
writer.append({ s: "x", j: "{}", f: 1n, i: 1n, b: true, t: 0 });
// @ts-expect-error i64 columns take bigints or numbers, not strings
writer.append({ s: "x", j: "{}", f: 1, i: "1", b: true, t: 0 });
// @ts-expect-error bool columns take booleans
writer.append({ s: "x", j: "{}", f: 1, i: 1n, b: 1, t: 0 });
// @ts-expect-error timestamp columns take Dates or epoch milliseconds
writer.append({ s: "x", j: "{}", f: 1, i: 1n, b: true, t: "2026-01-01" });
// @ts-expect-error unknown columns are rejected
writer.append({ s: "x", j: "{}", f: 1, i: 1n, b: true, t: 0, zz: 1 });
// @ts-expect-error optional columns still reject wrong value types
writer.append({ s: "x", j: "{}", f: 1, i: 1n, b: true, t: 0, opt: 2 });

/*
 * Maybe-promise surface. Without a codec nothing defers, but the *type* cannot
 * know that, so `append` and `finish` are unions and callers narrow or await.
 * `readParquet` is the exception: its no-options overload keeps the plain
 * return type every existing caller relies on.
 */
// A writer without a codec cannot defer, and is typed accordingly.
const stillVoid: void = writer.append({ s: "x", j: "{}", f: 1, i: 1n, b: true, t: 0 });
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
  i: 1n,
  b: true,
  t: 0,
});
const finished: Uint8Array | Promise<Uint8Array> = packing.finish();
void [appended, finished];

// @ts-expect-error a writer with a codec may defer
const notBytes: Uint8Array = createWriter(schema, { codec: both }).finish();
void notBytes;

const plain: ParquetFile = readParquet(new Uint8Array());
const hooked: ParquetFile | Promise<ParquetFile> = readParquet(new Uint8Array(), {
  codecs: { GZIP: both },
});
void [plain, hooked];

// @ts-expect-error with codecs registered the result may be a promise
const notAFile: ParquetFile = readParquet(new Uint8Array(), {});
void notAFile;

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
const asBigint: bigint = first.i;
const asBoolean: boolean = first.b;
const asDate: Date = first.t;
const asNullable: string | null = first.opt;
void [asString, asJson, asNumber, asBigint, asBoolean, asDate, asNullable];

// @ts-expect-error i64 reads back as a bigint, never a number
const notANumber: number = first.i;
// @ts-expect-error timestamp reads back as a Date, never epoch millis
const notMillis: number = first.t;
// @ts-expect-error an optional column may be null
const notNullable: string = first.opt;
// @ts-expect-error a required column is never null
const notNull: null = first.s;
void [notANumber, notMillis, notNullable, notNull];
