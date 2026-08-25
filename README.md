# tavolato

[![npm version](https://npmx.dev/api/registry/badge/version/tavolato?name=true)](https://npmx.dev/package/tavolato)
[![npm downloads](https://npmx.dev/api/registry/badge/downloads/tavolato)](https://npmx.dev/package/tavolato)
[![bundle size](https://npmx.dev/api/registry/badge/size/tavolato)](https://npmx.dev/package/tavolato)

A small [Apache Parquet](https://parquet.apache.org/) writer — and a reader for the files it writes — that works anywhere. The core imports nothing at all: no `node:*`, no dependencies, so the same code runs on Node, Deno, Bun, Cloudflare Workers, Deno Deploy and browsers.

_Tavolato_ is Italian for wooden planking — flat boards, laid side by side. That is exactly the shape of the data it handles: named columns, no nesting.

- **Flat schemas** — eight built-in column types, in-box adapters for the common annotated ones (`date`, `decimal`, `uuid`, `time`, `timestamp`, `float16`, `integer`, `json`), and `defineColumnType` for the rest.
- **Synchronous unless you make it otherwise** — `append`, `finish` and `readParquet` only return a promise when a compression hook hands them one.
- **Bring your own codec** — no compressor is shipped; the hook takes whatever your runtime already has, on both sides.
- **Verified against DuckDB** — every file the test suite writes is read back with the DuckDB CLI, and files DuckDB writes are fed to the reader in turn.

## Install

```sh
# Auto-detect package manager (npm, yarn, pnpm, deno, bun)
npx nypm install tavolato
```

## Quick start

```ts
import { createWriter, defineSchema, readParquet } from "tavolato";

const schema = defineSchema({
  at: { type: "timestamp" },
  host: { type: "string", optional: true },
  n: { type: "i64" },
});

const writer = createWriter(schema);
writer.append({ at: Date.now(), host: "web-1", n: 42n });
writer.append({ at: new Date(), host: null, n: 7 });

const bytes = writer.finish(); // PAR1 … pages … footer … PAR1
const { rows } = readParquet(bytes);
rows[0]; // { at: Date, host: "web-1", n: 42n }
```

`append` validates the whole row against the schema before anything is buffered, so a rejected row leaves the writer exactly as it was. `appendAll(rows)` takes any iterable, in order, pulled lazily. `finish()` flushes the pending row group, appends the footer, and makes the writer unusable; a writer that never saw a row still produces a valid file, with the schema present and `num_rows = 0`.

`createWriter(schema, options)` takes three options:

| option         | default      |                                                                                                                                                                                    |
| -------------- | ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `rowGroupSize` | `10_000`     | Rows buffered before a row group is flushed, which is also when the column buffers are released. A group is cut early if a column chunk would outgrow Parquet's ~2 GiB page limit. |
| `createdBy`    | `"tavolato"` | The footer's `created_by` string.                                                                                                                                                  |
| `codec`        | none         | Page compression — see [Compression](#compression).                                                                                                                                |

The writer also exposes `schema`, `rowCount` and `finished`.

## Column types

Eight built-ins, each owning a **bare** physical type — the one a file carries with no annotation at all. Where the writer accepts two inputs, the reader picks one and always picks it:

| `type`      | write                  | read back   | Parquet                            |
| ----------- | ---------------------- | ----------- | ---------------------------------- |
| `string`    | `string`               | `string`    | `BYTE_ARRAY` / `STRING`            |
| `json`      | `JsonValue`            | `JsonValue` | `BYTE_ARRAY` / `JSON`              |
| `f64`       | `number`               | `number`    | `DOUBLE`                           |
| `f32`       | `number`               | `number`    | `FLOAT`                            |
| `i64`       | `bigint`, safe integer | `bigint`    | `INT64`                            |
| `i32`       | `number` (integer)     | `number`    | `INT32`                            |
| `bool`      | `boolean`              | `boolean`   | `BOOLEAN`                          |
| `timestamp` | `Date`, epoch millis   | `Date`      | `INT64` / `TIMESTAMP(UTC, MILLIS)` |

`optional: true` makes a column nullable — real Parquet `OPTIONAL` repetition with definition levels, not a sentinel. `null`, `undefined` and an absent key all write a null; a null reads back as `null` with the key always present. Omitting a value for a required column throws.

Three built-ins are strict on the way in, so that what you read back is what you wrote: `i32` refuses a non-integer or anything outside −2³¹ … 2³¹−1 rather than wrapping it; `timestamp` refuses epoch millis past ±8.64 × 10¹⁵, the furthest a `Date` reaches, rather than storing a value that could only read back as an Invalid Date; and `f32` rounds to single precision **once**, on write, because single precision is what the column is.

`ColumnType` is a union that may gain members in a minor version — `json` joined `string` in one — so give any `switch` over it a `default` arm.

### Adapters

Parquet stores a column twice over: a **physical type**, which says how the bytes are laid out, and an **annotation**, which says what they mean. The annotation does not determine a JavaScript type — a `DECIMAL(38, 4)` is sixteen bytes of two's complement, and a `string`, a `bigint` and somebody's arbitrary-precision object are all defensible readings. So tavolato names what it found and stops. An adapter is you answering.

Each in-box factory maps by one rule: the value must survive the round trip unchanged.

| factory                                                | JavaScript  | Parquet                                           |
| ------------------------------------------------------ | ----------- | ------------------------------------------------- |
| `date()` or `date({ as: "date" })`                     | `Date`      | `INT32` / `DATE`                                  |
| `date({ as: "number" })`                               | `number`    | `INT32` / `DATE`                                  |
| `decimal({ precision, scale })`                        | `string`    | `INT32`, `INT64` or minimal `FLBA(n)` / `DECIMAL` |
| `uuid()`                                               | `string`    | `FLBA(16)` / `UUID`                               |
| `time({ unit: "millis", isAdjustedToUTC })`            | `number`    | `INT32` / `TIME(MILLIS, …)`                       |
| `time({ unit: "micros" \| "nanos", isAdjustedToUTC })` | `bigint`    | `INT64` / `TIME(…, …)`                            |
| `timestamp({ unit, isAdjustedToUTC })`                 | `bigint`    | `INT64` / `TIMESTAMP(…, …)`                       |
| `float16()`                                            | `number`    | `FLBA(2)` / `FLOAT16`                             |
| `integer({ bitWidth: 8 \| 16 \| 32, signed })`         | `number`    | `INT32` / `INTEGER(…)`                            |
| `integer({ bitWidth: 64, signed })`                    | `bigint`    | `INT64` / `INTEGER(64, …)`                        |
| `json({ reviver, replacer })`                          | `JsonValue` | `BYTE_ARRAY` / `JSON`                             |

```ts
date(); // new Date(Date.UTC(2026, 7, 24)) — exactly UTC midnight, never a truncated instant
date({ as: "date" }); // the same mapping, with the representation explicit
date({ as: "number" }); // 20_689 — signed days since the Unix epoch, across the full INT32 domain
decimal({ precision: 12, scale: 2 }); // "19.99" — canonical, exactly `scale` digits, one spelling per value
uuid(); // crypto.randomUUID() — canonical lowercase 8-4-4-4-12, and only that
time({ unit: "millis", isAdjustedToUTC: false }); // 43_200_000 — local time since midnight
timestamp({ unit: "micros", isAdjustedToUTC: true }); // 1_756_000_000_000_000n — an instant
float16(); // 1.5 — rounded to half precision once, on write
integer({ bitWidth: 8, signed: false }); // 255 — a domain, range-checked on the way in
json<Payload>(); // your document type, with the reviver and replacer opened up
```

`decimal()` covers Parquet's complete positive signed-i32 precision domain. It writes the smallest canonical layout for that precision and, when registered in `ReadOptions.types`, reads the same annotation from every legal `INT32`, `INT64`, `BYTE_ARRAY` or fixed-width layout.

The same object serves both sides: in a schema it decides how a column is written, and in `ReadOptions.types` it claims the columns it recognises.

Parquet `DATE` reaches far beyond JavaScript's ±100,000,000-day `Date` range. The default adapter refuses those otherwise-valid values with `ERR_READ_UNSUPPORTED`; use `date({ as: "number" })` for a lossless signed day count across the complete Parquet `INT32` domain. The representation is stable per adapter—never selected from the value's magnitude.

`TIME` and `TIMESTAMP` carry `unit` and `isAdjustedToUTC` as separate required Parquet parameters. Adapters match and re-emit both exactly. The built-in `timestamp` type maps only `TIMESTAMP(MILLIS, true)` to `Date`; use `timestamp({ unit: "millis", isAdjustedToUTC: false })` to preserve a local timestamp as its raw `bigint` count.

```ts
import { createWriter, decimal, defineSchema, readParquet, uuid } from "tavolato";

const price = decimal({ precision: 12, scale: 2 });
const schema = defineSchema({ id: { type: uuid() }, price: { type: price } });

const writer = createWriter(schema);
writer.append({ id: crypto.randomUUID(), price: "19.99" });

const { rows } = readParquet(writer.finish(), { types: [uuid(), price] });
rows[0].price; // "19.99" — the string you wrote, exactly
```

Two rules decide who claims what, and they do not overlap. **Built-in types own the bare physical types**: an unannotated `INT64` is `i64` whatever you register, and no in-box adapter claims an unannotated column. **Adapters own the annotations**, and are consulted first — so `integer({ bitWidth: 32 })` takes an `INTEGER(32, signed)` column that `i32` would otherwise have read. `types` is tried **in order**, so put the more specific one first. A claimed column carries the adapter object itself as its `type` in the schema the reader returns, which keeps `readParquet(bytes).schema` valid `createWriter` input.

### Writing your own

`defineColumnType` validates the spec — a physical kind that exists (`bool`, `i32`, `i64`, `f32`, `f64`, `bytes`, `fixed`), a `typeLength` exactly where `fixed` needs one, callable hooks, an annotation that can actually be written — and freezes it:

```ts
import { defineColumnType } from "tavolato";

const centi = defineColumnType({
  name: "centi", // used in errors and wherever a schema is displayed
  physical: "i64",
  matches: (annotation) =>
    annotation.kind === "decimal" && annotation.precision === 18 && annotation.scale === 2,
  annotate: () => ({ kind: "decimal", precision: 18, scale: 2 }),
  read: (raw) => Number(raw as bigint) / 100,
  write: (value: number) => BigInt(Math.round(value * 100)),
});
```

`physical` and `typeLength` are the layout an adapter writes, and that exact layout is always accepted. Define `acceptsPhysical(physical, typeLength)` only to add other layouts that the same logical representation safely decodes; it must return a boolean, and `matches()` still decides whether the annotation belongs to the adapter.

Both halves are **synchronous** — an adapter is a pure value transform, and the one place tavolato defers is the codec seam. **Nulls never reach one**: an `optional` column is handled by the definition-level machinery on both sides. A `bytes` or `fixed` `write()` must return a **fresh** `Uint8Array` every time, since the writer holds what you hand it by reference until the row group is flushed.

Your functions are held to their word. A `write` that throws, or that hands back something other than the physical value it promised, is `ERR_ROW_VALUE_INVALID` naming the column; a `read` that throws is `ERR_READ_MALFORMED`; `matches()` or `acceptsPhysical()` throwing, or `acceptsPhysical()` returning a non-boolean, is your option misbehaving rather than the file, and says so with `ERR_READ_OPTION_INVALID`.

This is also the way to read a column the built-ins have no reading for at all — an unannotated `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY`. tavolato will not hand you raw bytes and call it a value; declare what those bytes are, and it will.

## json documents

A flat schema has no room for something semi-structured. `json` is the way out: one column, one document per row, annotated so the engine reading it knows what the bytes are.

```ts
const schema = defineSchema({ at: { type: "timestamp" }, payload: { type: "json" } });
writer.append({ at: Date.now(), payload: { user: 1, tags: ["a"] } });
// reads back as { user: 1, tags: ["a"] }
```

The value is the **document itself**, in and out: `JSON.stringify` on the way in, `JSON.parse` on the way out, and the stored form is the JSON string Parquet's `JSON` annotation describes. Which means the round-trip semantics are **JSON's, not tavolato's** — `NaN` and the infinities become `null`, an `undefined` property vanishes, a `Date` becomes its ISO string and stays a string, a `Map` becomes `{}`, and `toJSON()` is honoured. The two things JSON cannot express at all are typed errors instead: a `bigint` anywhere in the document, and a value that serializes to nothing. A top-level `null` is not a document — it means the column is null, which is what `optional` is for.

Parsing uses a sanitizing reviver that drops `__proto__`, `prototype` and `constructor` keys from the documents it parses. That is a different layer from a **column** named `__proto__`, which round-trips faithfully. `jsonReviver` is exported to compose with, and `json({ reviver, replacer })` takes both hooks — a custom reviver **replaces** the default rather than running after it:

```ts
import { json, jsonReviver } from "tavolato";

const ISO = /^\d{4}-\d{2}-\d{2}T/;

const dated = json({
  reviver: (key, value) => {
    const safe = jsonReviver(key, value);
    return typeof safe === "string" && ISO.test(safe) ? new Date(safe) : safe;
  },
});
```

What the annotation buys is the other side. DuckDB reads the column as native `JSON`, so its operators just work — though it is still a flat column, so a query engine cannot prune on what is inside it. Promote a field to its own column when you want to filter on it.

```sql
SELECT payload->>'$.user' AS user, count(*) FROM read_parquet('events/*.parquet') GROUP BY user;
```

## Compression

A Parquet v1 data page is compressed as **one opaque body** — the RLE definition levels live inside it — so a codec is nothing but a byte transform. tavolato ships no compressor and never will; it ships the hook, and you fill it with what your runtime already has. One object can serve both sides:

```ts
import { gunzipSync, gzipSync } from "node:zlib";

const GZIP = {
  name: "GZIP",
  compress: gzipSync,
  decompress: (page: Uint8Array) => gunzipSync(page),
} as const; // `as const` so `name` stays the literal "GZIP", not `string`

const writer = createWriter(schema, { codec: GZIP });
writer.append({ at: Date.now(), n: 1n });
const bytes = await writer.finish();

const { rows } = await readParquet(bytes, { codecs: { GZIP } });
```

`compress` and `decompress` may each be synchronous or asynchronous, and tavolato is exactly as asynchronous as they are — no more. With **no codec**, or a **synchronous** one, nothing defers at runtime: `append` and `finish` return outright, and so does `readParquet`. With an **asynchronous** one, `append` returns a promise when appending that row closed a row group, `finish` when the final group is still being compressed, and `readParquet` when a page has to be inflated; **await it before touching the writer again**, or the next call throws `ERR_WRITER_BUSY` rather than interleaving two row groups into the same offsets.

The **types** are drawn one step wider, which is why the example above awaits: `createWriter(schema)` without a `codec` hands back a `SyncParquetWriter`, and `readParquet(bytes)` — with no options, or with `types` alone, since a column type cannot defer — is plainly a `ParquetFile`. The moment `codec` or `codecs` is passed the result widens to a maybe-promise, whichever half of it your codec turns out to be. Awaiting a value that is already there costs a microtask and nothing else.

### One line per runtime

|                   | compress                               | decompress                                                                                     |
| ----------------- | -------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Node ≥ 23.8       | `zstdCompressSync` from `node:zlib`    | `zstdDecompressSync`                                                                           |
| Node (any)        | `gzipSync` from `node:zlib`            | `gunzipSync`                                                                                   |
| Bun               | `Bun.zstdCompressSync`, `Bun.gzipSync` | `Bun.zstdDecompressSync`, `Bun.gunzipSync`                                                     |
| Deno              | same as Node, via `node:zlib`          | same                                                                                           |
| Workers, browsers | a userland sync library                | [`fflate`](https://github.com/101arrowz/fflate), [`fzstd`](https://github.com/101arrowz/fzstd) |

Workers and browsers do have `DecompressionStream`, and it is usable here since the hooks accept promises — but it is asynchronous and it only knows `gzip`, `deflate` and `deflate-raw`, none of which is `ZSTD` or `SNAPPY`. For anything it does not cover, reach for a userland synchronous decoder, or a wasm one you initialise once up front and then call synchronously.

`decompress` is called as `decompress(page, uncompressedSize)`. That second argument is deliberate — a decoder that wants to preallocate its output buffer needs it — and it is why `node:zlib`'s functions are wrapped rather than passed by reference: their own second parameter is an options object, and a number does not belong in it.

### Which container

The page body is the codec's own standard container, not a bare stream. tavolato stamps whichever `name` you give it and never inspects the bytes, so picking the wrong one produces a file other readers will reject.

| `name`    | body is                                                     |
| --------- | ----------------------------------------------------------- |
| `GZIP`    | an RFC 1952 member — use `gunzip`, **not** a raw `inflate`  |
| `ZSTD`    | an RFC 8878 frame                                           |
| `SNAPPY`  | the **raw block** format, not the framed one                |
| `LZ4_RAW` | a raw LZ4 block — the modern id, and the one to write       |
| `LZ4`     | the older, ambiguous Hadoop framing that `LZ4_RAW` replaced |
| `BROTLI`  | a Brotli stream                                             |
| `LZO`     | LZO1X                                                       |

## Reading

```ts
import { readParquet } from "tavolato";

const { schema, rows } = readParquet(bytes);

schema.columns; // [{ name: "at", type: "timestamp", optional: false }, …]
rows; // [{ at: Date, host: null, n: 42n }, …]
```

`schema` is derived from the file and has the shape `defineSchema` produces, so it can be handed straight to `createWriter` to make another file with the same columns. `rows` holds every row, in row group order and, within a group, in file order.

Rows are typed loosely, since a file's schema is only known at runtime. When you do know it, `ReadRowOf` is the read-side twin of the writer's row type:

```ts
import type { ReadRowOf } from "tavolato";

const rows = readParquet(bytes).rows as ReadRowOf<typeof schema.definition>[];
rows[0].n; // bigint
```

### Reading some of the columns

`ReadOptions.columns` narrows a read to a projection. A column chunk is independently seekable, so the columns left out are not decoded, not decompressed, and not even resolved:

```ts
const { schema, rows } = readParquet(bytes, { columns: ["at", "n"] });
```

Rows and the returned `schema` carry only those columns, in the **file's** order rather than the order asked for. Because an unselected column is never resolved, a projection **lifts that column's refusals**: a file with an `INT96`, a dictionary-encoded chunk, an unregistered codec or an annotation nothing claims still reads once the offending column is projected away. What it does not lift is the shape of the schema — a nested field is refused whole-file. A projection narrows columns, never rows.

An unknown name, a duplicate, or an empty list is `ERR_READ_OPTION_INVALID`.

### Just the schema

```ts
import { readSchema } from "tavolato";

readSchema(bytes); // the footer only; not a single page is touched
readSchema(bytes, { types: [price] }); // a column is claimed in the footer, so types apply here too
```

`readSchema` deliberately ignores `columns`: inspecting what a file holds is the one case where the answer should not have been narrowed first.

### One row group at a time

A Parquet file is sliced horizontally into **row groups**, each an independently decodable segment carrying all of the columns for its slice of the rows. `readParquet` materializes every row of every group at once; `readRowGroups` is the same read, one group per step.

```ts
import { readRowGroups } from "tavolato";

const file = readRowGroups(bytes);

file.schema; // from the footer, same shape readParquet returns
file.rowCount; // total rows the footer declares
file.groupCount; // number of row groups

for (const rows of file) {
  // one group in memory at a time — `rows` is a ReadRow[]
  for (const row of rows) total += row.n as bigint;
}
```

Memory drops from `O(all declared rows)` to `O(the rows of one row group)`. What that is worth depends on the writer rather than on you: DuckDB writes about 122 000 rows per group by default, and some writers put a single group in each file.

The **footer is still read up front**, eagerly and in full — that is where the schema and the groups' locations live. This is lazy _decoding_ over bytes you already hold, not streaming input; `bytes` stays referenced for as long as you use the result. That split is also where errors land: anything the footer can answer on its own throws from the `readRowGroups` call, and only page-level problems wait for the step whose group they are in, so a file whose second group is corrupt still yields its first.

Steps follow the same maybe-promise rule as everything else. One group is one maybe-promise, never a mix:

```ts
for (const rows of readRowGroups(bytes, { codecs })) {
  for (const row of await rows) total += row.n as bigint;
}
```

The state of a walk lives in its iterator, not in the object, so every `for…of` starts again at the first group and two walks can run at once. Steps are independent of one another as well — each owns its cursor over the bytes — so they may be pulled without being awaited, which decodes the groups **concurrently** and, naturally, gives the memory bound back up:

```ts
const groups = await Promise.all([...readRowGroups(bytes, { codecs })]);
```

A step that throws or rejects has consumed its group: the next step moves on, and the walk still ends after `groupCount` steps. `readParquet(bytes).rows` is exactly the concatenation of the steps, which the test suite checks file by file.

## Remote objects

The optional `tavolato/uns3` subpath puts Parquet in the middle of an [`uns3`](https://github.com/sandros94/uns3) client. It is a separate entry point, so importing `tavolato` never pulls it in, and `uns3` is an **optional peer dependency used for its types alone** — nothing here imports it at runtime, it only calls the five methods of the client you hand over.

```ts
import { S3Client } from "uns3";
import { createWriter, defineSchema } from "tavolato";
import { createParquetStore } from "tavolato/uns3";

const store = createParquetStore(new S3Client({/* … */}), { bucket: "metrics" });
const schema = defineSchema({ at: { type: "timestamp" }, n: { type: "i64" } });

await store.put("events/2026-08-24.parquet", { schema, rows: [{ at: Date.now(), n: 1n }] });

// …or hand over a writer you have been appending to — the store finishes it.
const writer = createWriter(schema);
writer.append({ at: Date.now(), n: 2n });
await store.put("events/2026-08-25.parquet", writer);

const { size, rowCount, groupCount } = await store.head("events/2026-08-24.parquet");
const { rows } = await store.get("events/2026-08-24.parquet", { columns: ["n"], groups: [0] });

await store.list({ prefix: "events/" });
await store.del("events/2026-08-24.parquet");
```

`put` takes finished bytes, a writer to finish, or `{ schema, rows }` to build a file from. `head` answers what an object _is_ — `size`, `etag`, `schema`, `rowCount`, `groupCount` — without downloading a page, and `groupCount` is the range `groups` indexes. `del` and `list` pass straight through to the client.

**`get` is the reason this exists.** Ask for nothing in particular and it is one plain GET of the whole object. Ask for `columns`, `groups`, or both, and it becomes a **ranged** read: the footer is fetched from the object's tail, the byte spans of the selected column chunks are computed from it, adjacent spans are coalesced, and only those ranges are transferred. Two columns of a forty column file cost two columns of bandwidth. The rows that come back are exactly what a local `readParquet` of the whole object under the same options would have produced.

The factory takes defaults; every one of them is overridable per call, and `codecs` / `types` are replaced wholesale rather than merged.

| default     |                                                                                                                                                       |
| ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bucket`    | Bucket for every call, as `uns3`'s own `defaultBucket` is for a client.                                                                               |
| `codecs`    | `ReadOptions.codecs` for `get`.                                                                                                                       |
| `types`     | `ReadOptions.types` for `get` and `head`.                                                                                                             |
| `writer`    | `WriterOptions` for a `put` handed `{ schema, rows }`.                                                                                                |
| `tailBytes` | Bytes read from the end of an object when a read needs the footer. Defaults to 64 KiB; a larger footer costs one extra request, never a wrong answer. |

Per call, `get` also takes `uns3`'s own get parameters (minus `key` and `range`, which the store owns) and `put` takes `uns3`'s put parameters plus `writer`. An index the file does not have, a repeated one, or an empty `groups` list is `ERR_READ_OPTION_INVALID`.

For a single upload with no store around it, `putParquet` is the one call that predates the store:

```ts
import { putParquet } from "tavolato/uns3";

await putParquet(client, { bucket: "metrics", key: "events/2026-08-24.parquet" }, writer);
```

It accepts either a writer (which it finishes for you) or raw bytes, defaults `contentType` to `application/vnd.apache.parquet` (exported as `PARQUET_CONTENT_TYPE`; pass your own, or `false` to send none), and passes every other `uns3` put parameter straight through.

## Errors

```ts
import { isTavolatoError } from "tavolato";

try {
  writer.append({ at: Date.now(), n: "oops" });
} catch (error) {
  if (isTavolatoError(error, "ERR_ROW_VALUE_INVALID")) {
    console.error(error.column); // "n"
  }
}
```

Every error thrown by the library is a `TavolatoError` carrying a `code` and, where the problem belongs to one column, its `column`. New codes may be added in a minor version, so match on the ones you handle rather than assuming the list is closed.

| family                    | thrown by                                                                            |
| ------------------------- | ------------------------------------------------------------------------------------ |
| `ERR_SCHEMA_*`            | `defineSchema`, `defineColumnType`                                                   |
| `ERR_ROW_*`               | `append`, `appendAll`                                                                |
| `ERR_WRITER_*`            | the writer's lifecycle, its options, its codec                                       |
| `ERR_READ_MALFORMED`      | bytes that are not a well-formed Parquet file                                        |
| `ERR_READ_UNSUPPORTED`    | valid Parquet, outside the subset tavolato writes — named, never guessed at          |
| `ERR_READ_OPTION_INVALID` | a `ReadOptions` value that cannot be used as given, as opposed to an unreadable file |
| `ERR_STORE_*`             | `tavolato/uns3` talking to object storage                                            |

An `ERR_READ_UNSUPPORTED` names the feature it found, and names the remedy where there is one: a compressed column says to register a decompressor in `ReadOptions.codecs`, an annotated one names the annotation with its parameters and says to pass a matching type in `ReadOptions.types`.

```
Cannot read column "price", a INT64 annotated DECIMAL(precision=18, scale=2): tavolato only
reads the files it writes — … — pass a matching type in ReadOptions.types to read it anyway
```

**Malformed input is a typed throw, never a hang or a bare `RangeError`** — wrong magic, a truncated stream, a length that does not fit, a footer that contradicts itself. That covers the _contents_ of any real `Uint8Array`, however hostile. Two carve-outs, and both are code that is not tavolato's:

- **A `Uint8Array` subclass that lies about itself** — one whose `byteLength` or `length` getter returns something other than the memory it has. Nothing can be validated before such an object is measured, and the measurement is the lie, so it may surface as a platform `RangeError`. Pass the bytes you were given, not a proxy for them, and this cannot arise.
- **A registered codec.** A third-party decoder can throw something else, loop, or trap in wasm, and nothing here can stop it. What tavolato does hold to on every page: the compressed length is bounded against the file **before** your hook sees a byte; whatever the hook throws or rejects with becomes `ERR_READ_MALFORMED` with the original as `cause`; and what it returns must be a `Uint8Array` of **exactly** the length the page header declared, or it is `ERR_READ_MALFORMED` too. On the write side a codec that throws, rejects, or returns something unusable raises `ERR_WRITER_CODEC_FAILED` and leaves the writer **unusable** — the row group it was compressing is already detached, so every later call throws the same error rather than quietly producing a file missing rows it accepted.

## Scope

> **tavolato writes flat Parquet files, and reads the ones it writes.**

That is the default, not a vow: the hooks widen it in both directions, symmetrically, and the column type list has grown once already. The rule that stays is a different one — **no single-use features**. What is frozen is the _shape_.

- **Flat schemas, forever.** One level of named scalar columns; repetition levels are always zero. No lists, no maps, no structs — ever. This half of the promise is absolute, and [`json`](#json-documents) is the escape valve for anything semi-structured.
- **What lands in the file.** Data page v1, one page per column chunk per row group; `PLAIN` values and `RLE` definition levels; `UNCOMPRESSED` unless you pass a codec; `null_count` statistics only, so no `column_orders`; no dictionary pages and no page CRCs, which is what keeps the library free of a hashing dependency.
- **Two permanent refusals**, with no hook and no way to opt in. **`INT96`** is the named example: deprecated in the format, never written here — and anything else the format deprecates goes the same way. **Dictionary-encoded columns** are the other wall; `parquet-rs` writers dictionary-encode by default and a codec hook does not unlock those files. Use DuckDB for them.
- **The whole file is a `Uint8Array`.** The reader does not stream, and `readParquet` returns every row at once, so its memory is `O(rows declared in the footer)`, **not** `O(bytes)` — definition levels are RLE compressed, so a six byte run can legitimately declare millions of nulls, and nothing distinguishes such a file from a sparse one somebody meant to write. [`readRowGroups`](#one-row-group-at-a-time) is the mitigation, and the one to reach for whenever the file is bigger than a mouthful. For **untrusted** input, cap the byte length you accept and check `readSchema` — or `readRowGroups`' counts, which come off the footer — against your own row limit before committing to a decode.

`readParquet` is for getting your own rows back: an S3 round trip, a test, a small aggregation in a worker. For predicate pushdown, files that do not fit in memory, or files written by something else, **DuckDB remains the recommended reader** — and anything that reads Parquet will read these files.

```sql
-- `at` is quoted because SQL reserves it; tavolato does not care what a column is called.
SELECT host, count(*) AS n, max("at") AS last_seen
FROM read_parquet('events/*.parquet')
WHERE host IS NOT NULL
GROUP BY host ORDER BY n DESC;

SELECT name, type, repetition_type, converted_type FROM parquet_schema('events/*.parquet');
SELECT num_rows, num_row_groups, created_by FROM parquet_file_metadata('events/*.parquet');
```

## Development

<details>

<summary>local development</summary>

- Clone this repository
- Install the latest LTS version of [Node.js](https://nodejs.org/en/)
- Enable [Corepack](https://github.com/nodejs/corepack) using `corepack enable`
- Install dependencies using `pnpm install`, then `pnpm dev:prepare`
- Run tests using `pnpm test` — the cross-read suites drive the [DuckDB CLI](https://duckdb.org/docs/installation/), which acts as the executable specification for the output, so it has to be on `PATH`
- Check with `pnpm lint` and `pnpm typecheck`

</details>

## License

<!-- automd:contributors license=MIT -->

Published under the [MIT](https://github.com/sandros94/tavolato/blob/main/LICENSE) license.
Made by [community](https://github.com/sandros94/tavolato/graphs/contributors) 💛
<br><br>
<a href="https://github.com/sandros94/tavolato/graphs/contributors">
<img src="https://contrib.rocks/image?repo=sandros94/tavolato" />
</a>

<!-- /automd -->

<!-- automd:with-automd -->

---

_🤖 auto updated with [automd](https://automd.unjs.io)_

<!-- /automd -->
