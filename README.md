# tavolato

[![npm version](https://npmx.dev/api/registry/badge/version/tavolato?name=true)](https://npmx.dev/package/tavolato)
[![npm downloads](https://npmx.dev/api/registry/badge/downloads/tavolato)](https://npmx.dev/package/tavolato)
[![bundle size](https://npmx.dev/api/registry/badge/size/tavolato)](https://npmx.dev/package/tavolato)

A small [Apache Parquet](https://parquet.apache.org/) writer — and a reader for
the files it writes — that works anywhere.

_Tavolato_ is Italian for wooden planking — flat boards, laid side by side. That
is exactly the shape of the data it handles: named columns, no nesting.

## Why

Most Parquet libraries assume Node, a native addon, or a compression codec you
have to ship. `tavolato` assumes none of that. The core imports **nothing** —
no `node:*`, no dependencies — and runs on any JavaScript runtime that has
`Uint8Array`, `DataView`, `TextEncoder`, `TextDecoder` and `BigInt`. That is
Node, Deno, Bun, Cloudflare Workers, Deno Deploy, browsers, and anything else
with a modern JavaScript engine.

The output is verified against DuckDB: every file the test suite produces is
written to disk and read back with the DuckDB CLI, which acts as the executable
specification. The reader is held to the same files from the other side — every
one of those fixtures is also read back by `tavolato` itself and compared value
for value, and files DuckDB writes are fed to it in turn.

## Scope

This is a design promise, not a roadmap gap.

> **`tavolato` writes flat Parquet files, and reads the ones it writes. It will
> never write nested ones, and it will never be a general Parquet reader.**
>
> - **Flat schemas, forever.** Named columns of `string`, `json`, `f64`, `f32`,
>   `i64`, `i32`, `bool` and `timestamp`, plus any [logical column
>   type](#column-types--adapters) you declare. No nesting, no lists, no maps,
>   no structs — ever. Repetition levels are always zero. This half of the
>   promise is absolute.
> - **Reads what it writes.** The reader accepts what the writer emits: a flat
>   schema of those types, PLAIN values, v1 data pages, RLE definition levels,
>   one or many row groups, zero rows included.
> - **Every column can be nullable.** Nullability is real Parquet
>   `OPTIONAL`/`REQUIRED` repetition with proper definition levels, not a
>   sentinel value.
> - **PLAIN encoding.** No dictionary encoding, no `RLE_DICTIONARY`, no delta
>   encodings.
> - **No compressor is shipped.** The default output is `UNCOMPRESSED`.
>   Compression is a [hook](#compression) you fill with whatever your runtime
>   already has — on both sides.
> - **No page checksums.** Parquet page CRCs are optional in the format, so
>   `tavolato` omits them and stays free of any hashing dependency.
>
> Keeping the surface this small is what keeps the library maintainable, and
> what lets the whole format-facing core be read in one sitting.

"Reads what it writes" is the default scope, not a vow. The codec hooks widen it
in both directions on purpose — write side and read side, symmetrically — and
the column type list has grown once already. The rule that stays is a different
one: **no single-use features.** Anything that would only ever serve one
project's file belongs in that project, behind a hook, not in here. What is
frozen is the _shape_: one flat level of scalar columns.

Because that list grows, treat `ColumnType` as a union that may gain members in
a minor version, and give any `switch` over it a `default` arm.

### What the reader refuses

Anything outside that subset is refused by name, never guessed at:

```ts
readParquet(someOtherWritersFile);
// TavolatoError: Cannot read column "host", which is dictionary encoded:
// tavolato only reads the files it writes — flat schemas of string, json, f64,
// f32, i64, i32, bool and timestamp columns, PLAIN encoded, UNCOMPRESSED, in v1
// data pages
```

That is a `TavolatoError` with code `ERR_READ_UNSUPPORTED` and, where the
problem belongs to one column, its `column`. It fires for a nested or `REPEATED`
schema, dictionary encoding, data page v2, any encoding other than `PLAIN` (or
`RLE` for definition levels), a compression codec you have not registered, and
any physical type or annotation nothing has claimed — an unannotated
`BYTE_ARRAY`, a `DECIMAL`, a `UUID`, a `TIMESTAMP` in microseconds, and so on.

Refusals you can lift say so. A compressed column names the remedy:

```
// … in v1 data pages — register a decompressor for ZSTD in ReadOptions.codecs
// to read it anyway
```

So does an annotated one, which names the annotation with its parameters:

```
// Cannot read column "price", a INT64 annotated DECIMAL(precision=18, scale=2):
// … — pass a matching type in ReadOptions.types to read it anyway
```

One value-level refusal names the same remedy. The built-in `timestamp` column
type is a `Date`, and a `Date` stops at ±8.64 × 10¹⁵ milliseconds while an
`INT64` does not: a `TIMESTAMP(MILLIS)` past that is refused rather than handed
back as an Invalid Date, and `timestamp({ unit: "millis" })` reads the count
itself. `tavolato`'s own writer will not produce such a value in the first
place.

Some cases stay out of reach whatever you register:

- **Dictionary-encoded columns.** `parquet-rs` writers — `celld`'s telemetry
  Parquet among them — dictionary-encode by default, so a codec hook alone does
  not unlock those files. Use DuckDB for them.
- **Anything the Parquet format itself has deprecated.** `tavolato` may refuse
  it outright, with no hook and no way to opt in. **`INT96` is the named
  example**: deprecated in the format, never written here, permanently refused.

Bytes that are not a well-formed Parquet file at all — wrong magic, a truncated
stream, a length that does not fit, a footer that contradicts itself — raise
`ERR_READ_MALFORMED` instead. Neither ever crashes ungracefully: malformed input
is a typed throw, not a hang or a `RangeError`.

That promise covers the _contents_ of any real `Uint8Array`, however hostile.
What it does not cover is a `Uint8Array` **subclass that lies about itself** — one
whose `byteLength` or `length` getter returns something other than the memory it
has. Nothing can be validated before such an object is measured, and the
measurement is the lie, so it may surface as a bare `RangeError` from the
platform rather than as a `TavolatoError`. Pass the bytes you were given, not a
proxy for them, and this cannot arise. (The [codec hooks](#what-the-guarantee-is-worth)
carry a similar caveat, for the same reason: code that is not `tavolato`'s.)

Two leniencies are allowed, and neither changes a single value: an `INT32` or
`INT64` annotated `INT_32` / `INT_64` (or the equivalent `INTEGER(32, signed)`)
reads as `i32` and `i64`, because that annotation says nothing the bare physical
type does not already say; and a `TIMESTAMP(MILLIS)` reads as a `Date` whether or
not it is marked `isAdjustedToUTC`, because the milliseconds are the same either
way and a `Date` is an instant. Together they are what let DuckDB's own
`COPY … (FORMAT PARQUET)` output be read directly, which the test suite checks.

## Install

```sh
# Auto-detect package manager (npm, yarn, pnpm, deno, bun)
npx nypm install tavolato
```

## Usage

```ts
import { createWriter, defineSchema } from "tavolato";

const schema = defineSchema({
  at: { type: "timestamp" },
  host: { type: "string", optional: true },
  n: { type: "i64" },
});

const writer = createWriter(schema, { rowGroupSize: 10_000 });

writer.append({ at: Date.now(), host: null, n: 42n });
writer.append({ at: new Date(), host: "web-1", n: 7 });

const bytes: Uint8Array = writer.finish();
// PAR1 … pages … footer … PAR1 — the writer is unusable after this.
```

`writer.append` validates the row against the schema and throws a typed
`TavolatoError` on anything it does not like. Validation happens for the whole
row before anything is buffered, so a rejected row leaves the writer exactly as
it was.

`writer.appendAll(rows)` appends an iterable in order; its re-entry guard is
**per row** by design, so an append that interleaves with an unawaited one lands
whole or draws `ERR_WRITER_BUSY` — never half a row, and never a torn file. A
writer that never saw a row still produces a valid file: schema present, zero row
groups, `num_rows = 0`.

### Column types

| `type`      | Accepts                | Parquet physical | Parquet logical          |
| ----------- | ---------------------- | ---------------- | ------------------------ |
| `string`    | `string`               | `BYTE_ARRAY`     | `STRING` (`UTF8`)        |
| `json`      | `string`               | `BYTE_ARRAY`     | `JSON`                   |
| `f64`       | `number`               | `DOUBLE`         | —                        |
| `f32`       | `number`               | `FLOAT`          | —                        |
| `i64`       | `bigint`, safe integer | `INT64`          | —                        |
| `i32`       | `number` (integer)     | `INT32`          | —                        |
| `bool`      | `boolean`              | `BOOLEAN`        | —                        |
| `timestamp` | `Date`, epoch millis   | `INT64`          | `TIMESTAMP(UTC, MILLIS)` |

These eight are the types that own a **bare** physical type — the one a file
carries with no annotation at all. Everything a column can additionally _mean_ is
an annotation, and annotations belong to [column
types](#column-types--adapters) you declare.

`timestamp` is UTC-normalised, which is what `TIMESTAMP_MILLIS` means in the
format. Readers surface it as an instant: DuckDB, for instance, reports
`TIMESTAMP WITH TIME ZONE`. It is a `Date` on both sides, so epoch milliseconds
past ±8.64 × 10¹⁵ — the furthest a `Date` reaches — are `ERR_ROW_VALUE_INVALID`
rather than a value that could only ever read back as an Invalid Date.

`i32` is range-checked on the way in: a non-integer, or anything outside
−2³¹ … 2³¹−1, is `ERR_ROW_VALUE_INVALID` rather than a silently wrapped value.

`f32` is the one built-in type whose value changes on the way in, and it has to:
single precision is what the column _is_, so writing rounds **once**, there and
then. Everything after that is exact — what you read back is the stored single,
and writing that value again reproduces the same four bytes.

Add `optional: true` to a column to make it nullable. `null`, `undefined` and an
absent key all write a null. Omitting a value for a required column throws.

#### `json`: the flat-schema escape valve

A flat schema has no room for something semi-structured. `json` is the way out:
one column, one document per row, annotated so the engine reading it knows what
the bytes are.

```ts
const schema = defineSchema({ at: { type: "timestamp" }, payload: { type: "json" } });
writer.append({ at: Date.now(), payload: JSON.stringify({ user: 1, tags: ["a"] }) });
```

The value is a **string in and a string out**. `tavolato` never calls
`JSON.parse` or `JSON.stringify` for you, never validates the document, and
stores exactly the bytes you hand it — whitespace and key order included. You
choose when to pay for parsing, and a round trip is byte-exact.

What the annotation buys is the other side. DuckDB reads the column as native
`JSON`, so its operators just work:

```sql
SELECT payload->>'$.user' AS user, count(*) FROM read_parquet('events/*.parquet') GROUP BY user;
```

It is still a flat column, so a query engine cannot prune on what is inside it.
Promote a field to its own column when you want to filter on it.

### Column types & adapters

Parquet stores a column twice over: a **physical type**, which says how the bytes
are laid out, and an **annotation**, which says what they mean. The annotation
does not determine a JavaScript type. A `DECIMAL(38, 4)` is sixteen bytes of
two's complement — a `string`, a `bigint` and somebody's arbitrary-precision
object are all defensible readings of it, and picking one for you would be the
same overreach as silently reading an `INT96`.

So `tavolato` names what it found and stops. An adapter is how you answer:

> **An adapter is the user resolving an ambiguity `tavolato` refuses to guess at
> — which JavaScript type an annotated column should become. It is the same
> principle as the typed refusals, not a departure from them.**

```ts
import { createWriter, decimal, defineSchema, readParquet, uuid } from "tavolato";

const price = decimal({ precision: 12, scale: 2 });

const schema = defineSchema({
  id: { type: uuid() },
  price: { type: price },
  at: { type: "timestamp" },
});

const writer = createWriter(schema);
writer.append({ id: crypto.randomUUID(), price: "19.99", at: Date.now() });

const { rows } = readParquet(writer.finish(), { types: [uuid(), price] });
rows[0].price; // "19.99" — the string you wrote, exactly
```

The same object serves both sides: in a schema it decides how a column is
written, and in `ReadOptions.types` it claims the columns it recognises.

#### The types in the box

Each maps by one rule — the value must survive the round trip unchanged. A
`number` where that is lossless, a `bigint` for the 64-bit widths, a `string`
where a JavaScript number would lie, and a `Date` only where the mapping is
exact.

| Column type                                    | JavaScript | Parquet                                    |
| ---------------------------------------------- | ---------- | ------------------------------------------ |
| `date()`                                       | `Date`     | `INT32` / `DATE`                           |
| `decimal({ precision, scale })`                | `string`   | `INT32`, `INT64` or `FLBA(16)` / `DECIMAL` |
| `uuid()`                                       | `string`   | `FLBA(16)` / `UUID`                        |
| `time({ unit: "millis" })`                     | `number`   | `INT32` / `TIME(MILLIS)`                   |
| `time({ unit: "micros" \| "nanos" })`          | `bigint`   | `INT64` / `TIME(…)`                        |
| `timestamp({ unit })`                          | `bigint`   | `INT64` / `TIMESTAMP(UTC, …)`              |
| `float16()`                                    | `number`   | `FLBA(2)` / `FLOAT16`                      |
| `integer({ bitWidth: 8 \| 16 \| 32, signed })` | `number`   | `INT32` / `INTEGER(…)`                     |
| `integer({ bitWidth: 64, signed })`            | `bigint`   | `INT64` / `INTEGER(64, …)`                 |

- **`date()`** takes a `Date` that is exactly UTC midnight, because a Parquet
  `DATE` has no time of day at all. A `Date` carrying one is refused rather than
  truncated — throwing away hours you handed over is not this library's call.
  `new Date(Date.UTC(y, m, d))` is how you make one.
- **`decimal()`** is a `string` because nothing else can hold the value: a
  `number` starts lying at 2⁵³, and a `bigint` would drop the point. The form is
  canonical — exactly `scale` digits after it, `"12.3400"` — and _strict_ on the
  way in, so `"12.34"` in a `scale: 4` column, a leading zero, or a `-0.00` are
  refused rather than reinterpreted. One spelling per value is what makes the
  round trip exact. The physical type follows precision, exactly as DuckDB's
  writer chooses it: `INT32` to 9 digits, `INT64` to 18, `FIXED_LEN_BYTE_ARRAY(16)`
  to 38. On the way **in** that also decides what it claims: a fixed-width
  decimal is claimed only at exactly 16 bytes. `parquet-mr`, Arrow and Spark
  write the _minimum_ width instead — 9 bytes for a `DECIMAL(19, 2)` — and those
  columns stay refused even with `decimal()` registered. Write your own column
  type for one of those files; there is no width option yet.
- **`uuid()`** takes the canonical lowercase 8-4-4-4-12 form and only that.
  `crypto.randomUUID()` already produces it.
- **`time()`** is a count since midnight, not a `Date`: a time of day is not an
  instant, and every `Date` this could produce would carry a date invented here.
  The domain is `[0, one day)` — a whole day's worth of units is the next
  midnight, not a time of day, and is refused as Arrow and `parquet-mr` refuse
  it.
- **`timestamp()`** is the raw count since the epoch as a `bigint`. The built-in
  `timestamp` _column type_ is milliseconds as a `Date`, which is exactly what a
  `Date` holds; microseconds and nanoseconds are not, so this hands back the
  count and loses nothing. It is also the single most common annotated column in
  the wild — `TIMESTAMP_MICROS` is what DuckDB writes by default.
- **`float16()`** rounds to half precision once, on write, because half precision
  is what the column is. Reading gives the stored value back exactly, and writing
  that value again reproduces the same two bytes.
- **`integer()`** is a _domain_, not a layout: Parquet stores all the narrow
  widths in an `INT32`, and the annotation says how much of it is meant. Values
  are range-checked on the way in, so an `INTEGER(8, true)` column cannot come to
  hold 300.

#### Who claims what

Two rules, and they do not overlap:

- **Built-in types own the bare physical types.** An unannotated `INT64` is `i64`
  and stays `i64`, whatever you register. None of the in-box adapters claims an
  unannotated column.
- **Adapters own the annotations.** They are consulted **before** the built-in
  types, so registering `integer({ bitWidth: 32 })` takes an
  `INTEGER(32, signed)` column that the `i32` leniency would otherwise have read.

`ReadOptions.types` is tried **in order**, and the first adapter whose physical
type, byte width and `matches()` all agree claims the column. Order is the tie
break, so put the more specific type first.

A claimed column carries the **adapter object itself** as its `type` in the
schema the reader returns, which keeps the property that
`readParquet(bytes).schema` is valid `createWriter` input — no registry to
rebuild on the way back.

#### Writing your own

`defineColumnType` validates the spec (a physical kind that exists, a
`typeLength` exactly where `"fixed"` needs one, four callable halves, an
annotation that can actually be written) and freezes it:

```ts
import { defineColumnType } from "tavolato";

const centi = defineColumnType({
  name: "centi", // used in errors and wherever a schema is displayed
  physical: "i64", // bool | i32 | i64 | f32 | f64 | bytes | fixed
  matches: (annotation) =>
    annotation.kind === "decimal" && annotation.precision === 18 && annotation.scale === 2,
  annotate: () => ({ kind: "decimal", precision: 18, scale: 2 }),
  read: (raw) => Number(raw as bigint) / 100,
  write: (value: number) => BigInt(Math.round(value * 100)),
});
```

Both halves are **synchronous** — an adapter is a pure value transform, and the
one place `tavolato` defers is the codec seam. **Nulls never reach one**: an
`optional` column is handled by the definition-level machinery on both sides, so
`read` and `write` only ever see values that are present.

A `bytes` or `fixed` `write()` must return a **fresh** `Uint8Array` every time.
The writer holds what you hand it by reference until the row group is flushed, so
a reused scratch buffer would rewrite every row already buffered with the latest
row's bytes.

Your functions are held to their word the way a codec is. A `write` that throws
becomes `ERR_ROW_VALUE_INVALID` naming the column, with the original as `cause`,
and the rejected row leaves the writer exactly as it was; one that hands back
something other than the physical value it promised is refused the same way. A
`read` that throws becomes `ERR_READ_MALFORMED`, again naming the column. A
`matches` that throws is your option misbehaving rather than the file, and says
so with `ERR_READ_OPTION_INVALID`.

This is also the way to read a column the built-ins have no reading for at all —
an unannotated `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY`. `tavolato` will not hand
you raw bytes and call it a value; declare what those bytes are, and it will.

### Reading it back

```ts
import { readParquet } from "tavolato";

const { schema, rows } = readParquet(bytes);

schema.columns; // [{ name: "at", type: "timestamp", optional: false }, …]
rows; // [{ at: Date, host: null, n: 42n }, …]
```

`schema` is derived from the file and has the same shape `defineSchema`
produces, so it can be handed straight to `createWriter` to make another file
with the same columns. `rows` holds every row, in row group order and, within a
group, in file order — the order they were appended.

Values come back in **one** JavaScript type per column type. Where the writer
accepts two inputs, the reader picks one and always picks it:

| `type`      | Written from           | Read back as |
| ----------- | ---------------------- | ------------ |
| `string`    | `string`               | `string`     |
| `json`      | `string`               | `string`     |
| `f64`       | `number`               | `number`     |
| `f32`       | `number`               | `number`     |
| `i64`       | `bigint`, safe integer | `bigint`     |
| `i32`       | `number`               | `number`     |
| `bool`      | `boolean`              | `boolean`    |
| `timestamp` | `Date`, epoch millis   | `Date`       |

An adapter column reads back as whatever its `read` returns — the in-box ones are
in the table [above](#the-types-in-the-box).

`i64` is **always** a `bigint`, even for values a `number` would hold exactly.
The writer's `bigint | number` is a convenience on the way in; on the way out
consistency wins, because a column whose type depended on its values would be
unusable. `timestamp` is likewise always a `Date` — and because it always is, a
count beyond the range a `Date` can represent is
[refused by name](#what-the-reader-refuses) rather than handed back as an
Invalid Date, with `timestamp({ unit: "millis" })` there to read the count
itself.

A null in an optional column reads back as `null`, and the key is always
present: a row that omitted an optional column entirely still comes back with
that column set to `null`.

`readSchema(bytes)` parses only the footer and returns the same schema without
touching a single page — useful to see what a file holds before deciding to
read it. It takes the same `types` as `readParquet`, since a column is claimed in
the footer: `readSchema(bytes, { types: [price] })`.

Rows are typed loosely, since a file's schema is only known at runtime. When you
do know it, `ReadRowOf` is the read-side twin of the writer's row type:

```ts
import type { ReadRowOf } from "tavolato";

const rows = readParquet(bytes).rows as ReadRowOf<typeof schema.definition>[];
rows[0].n; // bigint
```

### Reading one row group at a time

A Parquet file is sliced horizontally into **row groups**, and each one is an
independently decodable segment carrying all of the columns for its slice of the
rows. `readParquet` materializes every row of every group at once, which is the
documented cost of being this small. `readRowGroups` is the same read, one group
per step:

```ts
import { readRowGroups } from "tavolato";

const file = readRowGroups(bytes);

file.schema; // decoded from the footer, same shape readParquet returns
file.rowCount; // total rows the footer declares
file.groupCount; // number of row groups

for (const rows of file) {
  // One group in memory at a time — `rows` is a ReadRow[]
  for (const row of rows) total += row.n as bigint;
}
```

Memory drops from `O(all declared rows)` to `O(the rows of one row group)`.
DuckDB writes about 122 000 rows per group by default, and some writers — the
`parquet-rs` telemetry files among them — put a single group in each file, so
what the bound is worth depends on the writer rather than on you.

The **footer is still read up front**, eagerly and in full: that is where the
schema and the groups' locations live, so there is nothing to be lazy about
there. This is lazy _decoding_ over bytes you already hold, not streaming input —
`bytes` stays referenced for as long as you use the result.

That split is also where errors land. Anything the footer can answer on its own
throws from the `readRowGroups` call: a bad envelope, a schema outside the
subset, an annotation nothing claims, a chunk that contradicts the schema, a
compression codec you have not registered. Only page-level problems wait, and
those throw from the step whose group they are in — a file whose second group is
corrupt still yields its first.

Steps follow the same maybe-promise rule as everything else here. With no codec
or a synchronous one, a step _is_ the rows array and the whole walk allocates no
promises and crosses no microtask; with an asynchronous decompressor, that
step's value is a `Promise<ReadRow[]>` to await. One group is one maybe-promise,
never a mix:

```ts
for (const rows of readRowGroups(bytes, { codecs })) {
  for (const row of await rows) total += row.n as bigint;
}
```

The state of a walk lives in its iterator, not in the object, so every
`for…of` starts again at the first group and two walks can run at once without
disturbing each other. Steps are independent of one another as well — each owns
its cursor over the bytes — so they may be pulled without being awaited, which
decodes the groups **concurrently**:

```ts
const groups = await Promise.all([...readRowGroups(bytes, { codecs })]);
```

That one gives the memory bound back up, naturally: it is there for when the
decoding, rather than the memory, is what you wanted spread out.

A step that throws or rejects has consumed its group: the next step moves on to
the following one, and the walk still ends after `groupCount` steps.

`readRowGroups(bytes)` and `readRowGroups(bytes, { types })` are typed
`SyncParquetRowGroups`, whose steps are plainly `ReadRow[]`; only the overload
that takes `codecs` widens a step to `ReadRow[] | Promise<ReadRow[]>` — the same
rule `readParquet` follows for its result. Both entry points share one decoding
path, so the rows are identical: `readParquet(bytes).rows` is exactly the
concatenation of the steps, which the test suite checks file by file.

### Compression

A Parquet v1 data page is compressed as **one opaque body** — the RLE definition
levels live inside it — so a codec is nothing but a byte transform. `tavolato`
ships no compressor and never will; it ships the hook, and you fill it with what
your runtime already has.

```ts
import { gzipSync, gunzipSync } from "node:zlib";

const writer = createWriter(schema, { codec: { name: "GZIP", compress: gzipSync } });
// …
const { rows } = readParquet(bytes, {
  codecs: { GZIP: { decompress: (page) => gunzipSync(page) } },
});
```

One object can serve both sides:

```ts
const GZIP = {
  name: "GZIP",
  compress: gzipSync,
  decompress: (page: Uint8Array) => gunzipSync(page),
} as const; // `as const` so `name` stays the literal "GZIP", not `string`

createWriter(schema, { codec: GZIP });
readParquet(bytes, { codecs: { GZIP } });
```

`compress` and `decompress` may each be synchronous or asynchronous, and
`tavolato` is exactly as asynchronous as they are — no more:

- With **no codec**, or a **synchronous** one, nothing changes. `append` and
  `finish` return outright, and so does `readParquet`.
- With an **asynchronous** one, `append` returns a promise when appending that
  row closed a row group, `finish` returns one when the final row group is still
  being compressed, and `readParquet` returns one when a page has to be
  inflated. **Await it before touching the writer again** — a second call while
  a flush is in flight throws `ERR_WRITER_BUSY` rather than interleaving two row
  groups into the same offsets.

`readParquet(bytes)` with no options is still typed `ParquetFile`, and so is a
read that only registers `types` — a column type is a pure value transform and
cannot defer. Only the overload that takes `codecs` widens to
`ParquetFile | Promise<ParquetFile>`. Likewise `createWriter(schema)` without a
`codec` hands back a `SyncParquetWriter`, whose `append` and `finish` do not
return promises at all.

#### One line per runtime

|                   | compress                               | decompress                                                                                     |
| ----------------- | -------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Node ≥ 23.8       | `zstdCompressSync` from `node:zlib`    | `zstdDecompressSync`                                                                           |
| Node (any)        | `gzipSync` from `node:zlib`            | `gunzipSync`                                                                                   |
| Bun               | `Bun.zstdCompressSync`, `Bun.gzipSync` | `Bun.zstdDecompressSync`, `Bun.gunzipSync`                                                     |
| Deno              | same as Node, via `node:zlib`          | same                                                                                           |
| Workers, browsers | a userland sync library                | [`fflate`](https://github.com/101arrowz/fflate), [`fzstd`](https://github.com/101arrowz/fzstd) |

Workers and browsers do have `DecompressionStream`, and it is usable here since
the hooks accept promises — but it is asynchronous, and it only knows `gzip`,
`deflate` and `deflate-raw`. Note that none of those is `ZSTD` or `SNAPPY`; for
anything it does not cover, reach for a userland synchronous decoder, or a wasm
one you initialise once up front and then call synchronously.

`decompress` is called as `decompress(page, uncompressedSize)`. That second
argument is deliberate — a decoder that wants to preallocate its output buffer
needs it — and it is why `node:zlib`'s functions are wrapped rather than passed
by reference: their own second parameter is an options object, and a number does
not belong in it.

#### Which container

The page body is the codec's own standard container, not a bare stream:

| `name`    | Body is                                                     |
| --------- | ----------------------------------------------------------- |
| `GZIP`    | an RFC 1952 member — use `gunzip`, **not** a raw `inflate`  |
| `ZSTD`    | an RFC 8878 frame                                           |
| `SNAPPY`  | the **raw block** format, not the framed one                |
| `LZ4_RAW` | a raw LZ4 block — the modern id, and the one to write       |
| `LZ4`     | the older, ambiguous Hadoop framing that `LZ4_RAW` replaced |
| `BROTLI`  | a Brotli stream                                             |
| `LZO`     | LZO1X                                                       |

`tavolato` stamps whichever `name` you give it and never inspects the bytes, so
picking the wrong container produces a file other readers will reject. `LZ4` vs
`LZ4_RAW` is the classic trap: write `LZ4_RAW`.

#### What the guarantee is worth

The rest of this README promises that every corrupted byte comes back as a typed
`TavolatoError`. Through a codec hook that promise is **conditional on the codec**:
a third-party decoder can throw something else, loop, or trap in wasm, and
nothing here can stop it.

What `tavolato` does hold to, on every page:

- the compressed length is bounded against the file **before** your hook sees a
  byte, so it is never handed a length the file cannot back;
- whatever the hook throws or rejects with becomes `ERR_READ_MALFORMED`,
  carrying the original as `cause`;
- what the hook returns must be a `Uint8Array` of **exactly** the length the page
  header declared, or it is `ERR_READ_MALFORMED` too.

On the write side a codec that throws, rejects, or returns something unusable
raises `ERR_WRITER_CODEC_FAILED` with the original as `cause`, and leaves the
writer **unusable**: the row group it was compressing is already detached, so
every later call throws the same error rather than quietly producing a file
missing rows it accepted.

### Errors

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

Every error thrown by the library is a `TavolatoError` carrying a `code` and,
where relevant, the offending `column`. The codes are grouped by what went
wrong: `ERR_SCHEMA_*` for `defineSchema` and `defineColumnType`, `ERR_ROW_*` for
`append`, `ERR_WRITER_*` for the writer's lifecycle and its codec, and
`ERR_READ_MALFORMED` / `ERR_READ_UNSUPPORTED` / `ERR_READ_OPTION_INVALID` for
`readParquet` and `readSchema`. New codes may be added in a minor version, so
match on the ones you handle rather than assuming the list is closed.

### Uploading with `uns3`

The optional `tavolato/uns3` subpath adds a one-call upload helper for
[`uns3`](https://github.com/sandros94/uns3). It is a separate entry point, so
importing `tavolato` never pulls it in, and `uns3` is an optional peer
dependency used only for its types — the helper simply calls `client.put`.

```ts
import { S3Client } from "uns3";
import { createWriter, defineSchema } from "tavolato";
import { putParquet } from "tavolato/uns3";

const client = new S3Client({/* … */});

const writer = createWriter(defineSchema({ n: { type: "i64" } }));
writer.append({ n: 1n });

await putParquet(client, { bucket: "metrics", key: "events/2026-08-22.parquet" }, writer);
```

`putParquet` accepts either a writer (which it finishes for you) or raw bytes,
and defaults `contentType` to `application/vnd.apache.parquet`. Every other
`uns3` put parameter passes straight through.

## Querying the output

`readParquet` is for getting your own rows back — an S3 round trip, a test, a
small aggregation in a worker. For anything else, use a real query engine:
**DuckDB remains the recommended reader** for any Parquet beyond tavolato's own
files, and the only one to reach for when you want predicate pushdown, column
projection, files that do not fit in memory, or files written by something else.

Anything that reads Parquet will read these files. With DuckDB:

```sql
-- One file
SELECT host, count(*) AS n, max(at) AS last_seen
FROM read_parquet('events/2026-08-22.parquet')
WHERE host IS NOT NULL
GROUP BY host
ORDER BY n DESC;

-- A whole prefix, with the declared schema and row groups visible
SELECT name, type, repetition_type, converted_type FROM parquet_schema('events/*.parquet');
SELECT num_rows, num_row_groups, created_by FROM parquet_file_metadata('events/*.parquet');
```

## What ends up in the file

- Data page v1, one page per column chunk per row group.
- `PLAIN` values; `RLE` definition levels (only for nullable columns).
- `UNCOMPRESSED` unless you pass a [`codec`](#compression), in which case each
  page body — levels included — goes through it and the codec's id is stamped on
  every column chunk. No dictionary pages, no page CRC.
- `null_count` statistics per column chunk; no min/max, so no `column_orders`.
- A `created_by` string, overridable via `createWriter(schema, { createdBy })`.

Row groups are flushed once `rowGroupSize` rows have been appended (default
`10_000`), which is also the point at which the column buffers are released. A
row group is also cut early if a column chunk would otherwise outgrow the
signed 32-bit page size the format imposes (roughly 2 GiB per column chunk).

The reader takes the whole file as a `Uint8Array`: it does not stream, and it
does not skip columns. That is the honest cost of being this small — see the note
above about when to reach for DuckDB instead.

`readParquet` additionally returns every row at once, so its memory use is
`O(rows declared in the footer)`, **not** `O(bytes)`. Definition levels are RLE
compressed, so a six byte run can legitimately declare millions of nulls: a tiny
file can expand into a very large result, and nothing distinguishes such a file
from a sparse one somebody meant to write — a byte-count guard would only break
legitimate compression.

[`readRowGroups`](#reading-one-row-group-at-a-time) is the mitigation, and the
one to reach for whenever the file is bigger than a mouthful: it decodes a
single row group per step, which brings that bound down to `O(the rows of one
row group)`. The input bytes stay in memory either way. For **untrusted** input,
cap the byte length you are willing to accept, and use `readSchema` — or
`readRowGroups`, whose `rowCount` and `groupCount` come off the footer — together
with your own row limit before committing to decoding anything.

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
