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

## Scope freeze

This is a design promise, not a roadmap gap.

> **`tavolato` writes flat Parquet files, and reads the ones it writes. It will
> never write nested ones, and it will never be a general Parquet reader.**
>
> - **Flat schemas, forever.** Named columns of `string`, `f64`, `i64`, `bool`
>   and `timestamp`. No nesting, no lists, no maps, no structs — ever.
>   Repetition levels are always zero. This half of the promise is absolute.
> - **Reads its own subset.** The reader accepts exactly what the writer emits:
>   a flat schema of those five types, PLAIN values, `UNCOMPRESSED`, v1 data
>   pages, RLE definition levels, one or many row groups, zero rows included.
> - **Every column can be nullable.** Nullability is real Parquet
>   `OPTIONAL`/`REQUIRED` repetition with proper definition levels, not a
>   sentinel value.
> - **PLAIN encoding, UNCOMPRESSED codec.** No dictionary encoding. Compression
>   _may_ be added later behind an option; nesting will not.
> - **No page checksums.** Parquet page CRCs are optional in the format, so
>   `tavolato` omits them and stays free of any hashing dependency.
>
> Keeping the surface this small is what keeps the library maintainable, and
> what lets the whole format-facing core be read in one sitting.

### What the reader refuses

Anything outside that subset is refused by name, never guessed at:

```ts
readParquet(someOtherWritersFile);
// TavolatoError: Cannot read column "host", which is dictionary encoded:
// tavolato only reads the files it writes — flat schemas of string, f64, i64,
// bool and timestamp columns, PLAIN encoded, UNCOMPRESSED, in v1 data pages
```

That is a `TavolatoError` with code `ERR_READ_UNSUPPORTED` and, where the
problem belongs to one column, its `column`. It fires for a nested or `REPEATED`
schema, a compression codec, dictionary encoding, data page v2, any encoding
other than `PLAIN` (or `RLE` for definition levels), and any physical or logical
type outside the five — `INT32`, `FLOAT`, `DECIMAL`, `DATE`, an unannotated
`BYTE_ARRAY`, a `TIMESTAMP` in microseconds or nanoseconds, and so on.

Bytes that are not a well-formed Parquet file at all — wrong magic, a truncated
stream, a length that does not fit, a footer that contradicts itself — raise
`ERR_READ_MALFORMED` instead. Neither ever crashes ungracefully: malformed input
is a typed throw, not a hang or a `RangeError`.

Two leniencies are allowed, and neither changes a single value: an `INT64`
annotated `INT_64` reads as `i64`, because that annotation says nothing a bare
`INT64` does not already say; and a `TIMESTAMP(MILLIS)` reads as a `Date`
whether or not it is marked `isAdjustedToUTC`, because the milliseconds are the
same either way and a `Date` is an instant. Together they are what let DuckDB's
own `COPY … (FORMAT PARQUET, COMPRESSION UNCOMPRESSED)` output be read
directly, which the test suite checks.

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
| `f64`       | `number`               | `DOUBLE`         | —                        |
| `i64`       | `bigint`, safe integer | `INT64`          | —                        |
| `bool`      | `boolean`              | `BOOLEAN`        | —                        |
| `timestamp` | `Date`, epoch millis   | `INT64`          | `TIMESTAMP(UTC, MILLIS)` |

`timestamp` is UTC-normalised, which is what `TIMESTAMP_MILLIS` means in the
format. Readers surface it as an instant: DuckDB, for instance, reports
`TIMESTAMP WITH TIME ZONE`.

Add `optional: true` to a column to make it nullable. `null`, `undefined` and an
absent key all write a null. Omitting a value for a required column throws.

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
`ERR_WRITER_*` for the writer's lifecycle, and `ERR_READ_MALFORMED` /
`ERR_READ_UNSUPPORTED` for `readParquet` and `readSchema`.

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
- `UNCOMPRESSED` codec, no dictionary pages, no page CRC.
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
