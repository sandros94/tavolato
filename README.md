# tavolato

[![npm version](https://npmx.dev/api/registry/badge/version/tavolato?name=true)](https://npmx.dev/package/tavolato)
[![npm downloads](https://npmx.dev/api/registry/badge/downloads/tavolato)](https://npmx.dev/package/tavolato)
[![bundle size](https://npmx.dev/api/registry/badge/size/tavolato)](https://npmx.dev/package/tavolato)

A small [Apache Parquet](https://parquet.apache.org/) writer that works anywhere.

_Tavolato_ is Italian for wooden planking — flat boards, laid side by side. That
is exactly the shape of the data it writes: named columns, no nesting.

## Why

Most Parquet libraries assume Node, a native addon, or a compression codec you
have to ship. `tavolato` assumes none of that. The core imports **nothing** —
no `node:*`, no dependencies — and runs on any JavaScript runtime that has
`Uint8Array`, `DataView`, `TextEncoder` and `BigInt`. That is Node, Deno, Bun,
Cloudflare Workers, Deno Deploy, browsers, and anything else with a modern
JavaScript engine.

The output is verified against DuckDB: every file the test suite produces is
written to disk and read back with the DuckDB CLI, which acts as the
executable specification.

## Scope freeze

This is a design promise, not a roadmap gap.

> **`tavolato` writes flat Parquet files. It will never read them, and it will
> never write nested ones.**
>
> - **Writer only.** No reader, ever. Reading Parquet is a much larger problem
>   and there are good tools for it.
> - **Flat schemas only.** Named columns of `string`, `f64`, `i64`, `bool` and
>   `timestamp`. No nesting, no lists, no maps, no structs — ever. Repetition
>   levels are always zero.
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
where relevant, the offending `column`.

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
