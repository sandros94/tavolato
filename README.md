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
> - **Flat schemas, forever.** Named columns of `string`, `json`, `f64`, `i64`,
>   `bool` and `timestamp`. No nesting, no lists, no maps, no structs — ever.
>   Repetition levels are always zero. This half of the promise is absolute.
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
// i64, bool and timestamp columns, PLAIN encoded, UNCOMPRESSED, in v1 data pages
```

That is a `TavolatoError` with code `ERR_READ_UNSUPPORTED` and, where the
problem belongs to one column, its `column`. It fires for a nested or `REPEATED`
schema, dictionary encoding, data page v2, any encoding other than `PLAIN` (or
`RLE` for definition levels), a compression codec you have not registered, and
any physical or logical type outside the list — `INT32`, `FLOAT`, `DECIMAL`,
`DATE`, an unannotated `BYTE_ARRAY`, a `TIMESTAMP` in microseconds or
nanoseconds, and so on.

Refusals you can lift say so. A compressed column names the remedy:

```
// … in v1 data pages — register a decompressor for ZSTD in ReadOptions.codecs
// to read it anyway
```

Some cases stay out of reach whatever you register:

- **Dictionary-encoded columns.** `parquet-rs` writers — `celld`'s telemetry
  Parquet among them — dictionary-encode by default, so a codec hook alone does
  not unlock those files. Use DuckDB for them.
- **`INT96`.** Deprecated in the format, and something `tavolato` would never
  write. Permanently refused.

Bytes that are not a well-formed Parquet file at all — wrong magic, a truncated
stream, a length that does not fit, a footer that contradicts itself — raise
`ERR_READ_MALFORMED` instead. Neither ever crashes ungracefully: malformed input
is a typed throw, not a hang or a `RangeError`.

Two leniencies are allowed, and neither changes a single value: an `INT64`
annotated `INT_64` reads as `i64`, because that annotation says nothing a bare
`INT64` does not already say; and a `TIMESTAMP(MILLIS)` reads as a `Date`
whether or not it is marked `isAdjustedToUTC`, because the milliseconds are the
same either way and a `Date` is an instant. Together they are what let DuckDB's
own `COPY … (FORMAT PARQUET)` output be read directly, which the test suite
checks.

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

`writer.appendAll(rows)` appends an iterable in order. A writer that never saw
a row still produces a valid file: schema present, zero row groups,
`num_rows = 0`.

### Column types

| `type`      | Accepts                | Parquet physical | Parquet logical          |
| ----------- | ---------------------- | ---------------- | ------------------------ |
| `string`    | `string`               | `BYTE_ARRAY`     | `STRING` (`UTF8`)        |
| `json`      | `string`               | `BYTE_ARRAY`     | `JSON`                   |
| `f64`       | `number`               | `DOUBLE`         | —                        |
| `i64`       | `bigint`, safe integer | `INT64`          | —                        |
| `bool`      | `boolean`              | `BOOLEAN`        | —                        |
| `timestamp` | `Date`, epoch millis   | `INT64`          | `TIMESTAMP(UTC, MILLIS)` |

`timestamp` is UTC-normalised, which is what `TIMESTAMP_MILLIS` means in the
format. Readers surface it as an instant: DuckDB, for instance, reports
`TIMESTAMP WITH TIME ZONE`.

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
| `i64`       | `bigint`, safe integer | `bigint`     |
| `bool`      | `boolean`              | `boolean`    |
| `timestamp` | `Date`, epoch millis   | `Date`       |

`i64` is **always** a `bigint`, even for values a `number` would hold exactly.
The writer's `bigint | number` is a convenience on the way in; on the way out
consistency wins, because a column whose type depended on its values would be
unusable. `timestamp` is likewise always a `Date` — millis beyond the range
`Date` can represent come back as an invalid one.

A null in an optional column reads back as `null`, and the key is always
present: a row that omitted an optional column entirely still comes back with
that column set to `null`.

`readSchema(bytes)` parses only the footer and returns the same schema without
touching a single page — useful to see what a file holds before deciding to
read it.

Rows are typed loosely, since a file's schema is only known at runtime. When you
do know it, `ReadRowOf` is the read-side twin of the writer's row type:

```ts
import type { ReadRowOf } from "tavolato";

const rows = readParquet(bytes).rows as ReadRowOf<typeof schema.definition>[];
rows[0].n; // bigint
```

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

`readParquet(bytes)` with no options is still typed `ParquetFile`; only the
overload that takes `codecs` widens to `ParquetFile | Promise<ParquetFile>`.
Likewise `createWriter(schema)` without a `codec` hands back a
`SyncParquetWriter`, whose `append` and `finish` do not return promises at all.

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
wrong: `ERR_SCHEMA_*` for `defineSchema`, `ERR_ROW_*` for `append`,
`ERR_WRITER_*` for the writer's lifecycle and its codec, and
`ERR_READ_MALFORMED` / `ERR_READ_UNSUPPORTED` for `readParquet` and
`readSchema`. New codes may be added in a minor version, so match on the ones
you handle rather than assuming the list is closed.

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

The reader takes the whole file as a `Uint8Array` and returns every row: it does
not stream, and it does not skip row groups or columns. That is the honest cost
of being this small — see the note above about when to reach for DuckDB
instead.

Its memory use is therefore `O(rows declared in the footer)`, **not**
`O(bytes)`. Definition levels are RLE compressed, so a six byte run can
legitimately declare millions of nulls: a tiny file can expand into a very large
result, and nothing distinguishes such a file from a sparse one somebody meant
to write — a byte-count guard would only break legitimate compression. For your
own files this is a non-issue. For **untrusted** input, cap the byte length you
are willing to accept, and use `readSchema` together with your own row limit
before committing to a full `readParquet`.

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
