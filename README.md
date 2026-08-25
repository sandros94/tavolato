# tavolato

[![npm version](https://npmx.dev/api/registry/badge/version/tavolato?name=true)](https://npmx.dev/package/tavolato) [![npm downloads](https://npmx.dev/api/registry/badge/downloads/tavolato)](https://npmx.dev/package/tavolato) [![bundle size](https://npmx.dev/api/registry/badge/size/tavolato)](https://npmx.dev/package/tavolato)

Small Parquet for JavaScript and TypeScript. Tavolato writes flat, interoperable [Apache Parquet](https://parquet.apache.org/) files and reads the format subset it writes across Node, Deno, Bun, workers, and browsers.

The core has zero runtime dependencies and no `node:*` imports. Add it when you need portable analytics files without adopting a larger data stack.

- Typed schemas and rows
- Built-in logical types plus custom adapters
- Optional compression hooks
- Column and row-group selection
- Ranged object-store reads through `uns3`
- Cross-read compatibility tests with DuckDB

## Install

```sh
npx nypm install tavolato
```

## Quick start

<!-- automd:file src="./playgrounds/quick-start.ts" code lang="ts" name=false -->

```ts
import { createWriter, defineSchema, readParquet, type ParquetFile } from "tavolato";

const schema = defineSchema({
  at: { type: "timestamp" },
  host: { type: "string", optional: true },
  count: { type: "i64" },
});

export function roundTripEvents(): ParquetFile {
  const writer = createWriter(schema);
  writer.append({ at: Date.UTC(2026, 7, 25), host: "web-1", count: 42n });
  writer.append({ at: new Date("2026-08-25T01:00:00Z"), host: null, count: 7 });

  return readParquet(writer.finish());
}
```

<!-- /automd -->

`defineSchema` validates and freezes a flat schema. `append` validates each row before buffering it, while `appendAll` consumes any iterable. `finish` flushes the final row group and returns a complete Parquet `Uint8Array`; the writer cannot be reused afterwards.

Writer options:

| Option         | Default      | Use                                    |
| -------------- | ------------ | -------------------------------------- |
| `rowGroupSize` | `10_000`     | Maximum buffered rows per row group    |
| `createdBy`    | `"tavolato"` | Footer `created_by` value              |
| `codec`        | none         | Page compressor and Parquet codec name |

## Schemas and column types

A schema is one level of named scalar columns. Set `optional: true` to write a Parquet `OPTIONAL` column; missing, `undefined`, and `null` inputs read back as `null`.

| Type        | Accepted input               | Read value     | Parquet                            |
| ----------- | ---------------------------- | -------------- | ---------------------------------- |
| `string`    | `string`                     | `string`       | `BYTE_ARRAY` / `STRING`            |
| `json`      | `JsonDocument`               | `JsonDocument` | `BYTE_ARRAY` / `JSON`              |
| `f64`       | `number`                     | `number`       | `DOUBLE`                           |
| `f32`       | `number`                     | `number`       | `FLOAT`                            |
| `i64`       | `bigint` or safe integer     | `bigint`       | `INT64`                            |
| `i32`       | 32-bit integer               | `number`       | `INT32`                            |
| `bool`      | `boolean`                    | `boolean`      | `BOOLEAN`                          |
| `timestamp` | `Date` or epoch milliseconds | `Date`         | `INT64` / `TIMESTAMP(MILLIS, UTC)` |

Use `Row<typeof schema.definition>` for write-side row types and `ReadRowOf<typeof schema.definition>` when the file schema is already known by your application.

### Logical type adapters

Adapters preserve Parquet annotations and select their JavaScript representation. Register the same adapters in `ReadOptions.types` when reading.

| Factory | JavaScript representation | Parquet annotation |
| --- | --- | --- |
| `date({ as: "date" })` | `Date` at UTC midnight | `DATE` |
| `date({ as: "number" })` | signed epoch-day `number` | `DATE` |
| `decimal({ precision, scale })` | canonical decimal `string` | `DECIMAL` |
| `uuid()` | canonical UUID `string` | `UUID` |
| `time({ unit, isAdjustedToUTC })` | `number` for millis; otherwise `bigint` | `TIME` |
| `timestamp({ unit, isAdjustedToUTC })` | `bigint` | `TIMESTAMP` |
| `float16({ as: "number" })` | rounded `number` | `FLOAT16` |
| `float16({ as: "bits" })` | exact unsigned 16-bit pattern | `FLOAT16` |
| `integer({ bitWidth, signed })` | `number` or 64-bit `bigint` | `INTEGER` |
| `json({ as: "value" })` | parsed JSON document | `JSON` |
| `json({ as: "text" })` | validated, unchanged JSON text | `JSON` |

<!-- automd:file src="./playgrounds/adapters.ts" code lang="ts" name=false -->

```ts
import {
  createWriter,
  date,
  decimal,
  defineSchema,
  readParquet,
  uuid,
  type ParquetFile,
} from "tavolato";

const uuidType = uuid();
const dateType = date({ as: "date" });
const moneyType = decimal({ precision: 12, scale: 2 });

const schema = defineSchema({
  id: { type: uuidType },
  issued: { type: dateType },
  total: { type: moneyType },
});

export function roundTripInvoice(id: string): ParquetFile {
  const writer = createWriter(schema);
  writer.append({
    id,
    issued: new Date("2026-08-25T00:00:00Z"),
    total: "19.99",
  });

  return readParquet(writer.finish(), {
    types: [uuidType, dateType, moneyType],
  });
}
```

<!-- /automd -->

`date()` defaults to `{ as: "date" }`, `float16()` to `{ as: "number" }`, and `json()` to `{ as: "value" }`. Use the explicit representation when a public API should state its output type.

Value-mode JSON follows native `JSON.stringify` and `JSON.parse` semantics. Dangerous own keys (`__proto__`, `prototype`, and `constructor`) are removed by default, including after a custom reviver. Set `dangerousKeys: "preserve"` only for trusted documents. Use `JSON_NULL` for a top-level JSON literal `null`; a JavaScript `null` in an optional column remains a Parquet null.

### Custom column types

Use `defineColumnType` for an annotated scalar representation not covered by an in-box adapter. Its synchronous `write` and `read` hooks map between your value and a supported Parquet physical value.

<!-- automd:file src="./playgrounds/custom-column.ts" code lang="ts" name=false -->

```ts
import { createWriter, defineColumnType, defineSchema, readParquet } from "tavolato";

const cents = defineColumnType<number, number>({
  name: "cents",
  physical: "i64",
  matches: (annotation) =>
    annotation.kind === "decimal" && annotation.precision === 18 && annotation.scale === 2,
  annotate: () => ({ kind: "decimal", precision: 18, scale: 2 }),
  read: (raw) => Number(raw as bigint) / 100,
  write: (value) => BigInt(Math.round(value * 100)),
});

const schema = defineSchema({ amount: { type: cents } });

export function roundTripAmount(amount: number): number {
  const writer = createWriter(schema);
  writer.append({ amount });

  const { rows } = readParquet(writer.finish(), { types: [cents] });
  return rows[0].amount as number;
}
```

<!-- /automd -->

Register custom adapters in `ReadOptions.types`, most-specific first. An adapter only receives non-null values. A `bytes` or `fixed` writer must return a fresh `Uint8Array` for every value.

## Compression

Tavolato ships no compressor. Pass a codec supplied by the runtime or your own library, then register its decompressor when reading.

<!-- automd:file src="./playgrounds/compression.ts" code lang="ts" name=false -->

```ts
import { gzipSync, gunzipSync } from "node:zlib";
import {
  createWriter,
  defineSchema,
  readParquet,
  type ReaderCodec,
  type WriterCodec,
} from "tavolato";

const gzip = {
  name: "GZIP",
  compress: (page: Uint8Array) => gzipSync(page),
  decompress: (page: Uint8Array) => gunzipSync(page),
} satisfies WriterCodec & ReaderCodec;

const schema = defineSchema({ message: { type: "string" } });

export async function roundTripCompressed(): Promise<string> {
  const writer = createWriter(schema, { codec: gzip });
  await writer.append({ message: "compressed" });

  const bytes = await writer.finish();
  const { rows } = await readParquet(bytes, { codecs: { GZIP: gzip } });
  return rows[0].message as string;
}
```

<!-- /automd -->

Supported Parquet codec names are `GZIP`, `ZSTD`, `SNAPPY`, `BROTLI`, `LZO`, `LZ4_RAW`, and legacy `LZ4`. GZIP uses an RFC 1952 member, ZSTD an RFC 8878 frame, SNAPPY a raw block, and `LZ4_RAW` a raw LZ4 block.

Codec hooks may return a value or promise. Without codecs, writing and local reading are synchronous. With codecs, await `append`, `appendAll`, `finish`, and read results before continuing.

## Reading

| API                              | Result                                      |
| -------------------------------- | ------------------------------------------- |
| `readParquet(bytes, options?)`   | Schema and all selected rows                |
| `readSchema(bytes, options?)`    | Footer schema without decoding pages        |
| `readRowGroups(bytes, options?)` | Schema/counts plus lazy row-group iteration |

`ReadOptions.columns` projects named columns in file order. Unselected chunks are not decoded or decompressed. Unknown, repeated, or empty selections throw `ERR_READ_OPTION_INVALID`.

Use `readRowGroups` when decoded rows should be limited to one row group at a time:

<!-- automd:file src="./playgrounds/row-groups.ts" code lang="ts" name=false -->

```ts
import { createWriter, defineSchema, readRowGroups } from "tavolato";

const schema = defineSchema({ count: { type: "i64" } });

export function sumOneGroupAtATime(): bigint {
  const writer = createWriter(schema, { rowGroupSize: 2 });
  writer.appendAll([{ count: 1n }, { count: 2n }, { count: 3n }]);

  let total = 0n;
  const file = readRowGroups(writer.finish(), { columns: ["count"] });
  for (const rows of file) {
    for (const row of rows) total += row.count as bigint;
  }
  return total;
}
```

<!-- /automd -->

The input `Uint8Array` and footer remain referenced during iteration. Adding `codecs` makes each iteration step a maybe-promise; await the yielded rows.

## Object storage

The optional `tavolato/uns3` entry point wraps an `uns3`-compatible client. The peer dependency is type-only; Tavolato calls the client supplied by your application.

<!-- automd:file src="./playgrounds/object-store.ts" code lang="ts" name=false -->

```ts
import { defineSchema, type ReadRow } from "tavolato";
import { createParquetStore, type ParquetHead, type ParquetStoreClient } from "tavolato/uns3";

const schema = defineSchema({
  at: { type: "timestamp" },
  count: { type: "i64" },
});

export async function storeEvents(
  client: ParquetStoreClient,
): Promise<{ head: ParquetHead; rows: ReadRow[] }> {
  const store = createParquetStore(client, { bucket: "analytics" });
  const key = "events/date=2026-08-25/part-001.parquet";

  await store.put(key, {
    schema,
    rows: [{ at: Date.UTC(2026, 7, 25), count: 1n }],
  });

  const head = await store.head(key);
  const { rows } = await store.get(key, { columns: ["count"], groups: [0] });
  return { head, rows };
}
```

<!-- /automd -->

`createParquetStore` provides:

| Method | Behavior                                                             |
| ------ | -------------------------------------------------------------------- |
| `put`  | Upload bytes, finish a writer, or build from `{ schema, rows }`      |
| `get`  | Read the whole object or selected columns/row groups                 |
| `head` | Read schema, size, ETag, and row-group counts without decoding pages |
| `list` | List objects through the client                                      |
| `del`  | Delete an object through the client                                  |

Supplying `columns`, `groups`, or both lets `get` fetch a footer tail and selected column-chunk ranges. Small objects, or clients that ignore ranges, may still be fetched whole. The client must accept HTTP `206 Partial Content` responses. `putParquet` is available for a single direct upload without creating a store.

## Growing datasets

A completed Parquet file ends with metadata describing every row group. Do not append bytes to an existing object: adding rows requires rebuilding that footer.

For object storage, append new files to a dataset prefix instead. Partition keys used by queries can live in the path, for example `events/date=2026-08-25/hour=14/part-<uuid>.parquet`. DuckDB and other analytics engines can read matching files as one table and prune Hive-style partitions.

Keep batches large enough to avoid excessive tiny files but comfortably below the producing runtime's memory limit. Periodically compact small files in a runtime with more memory, then publish the replacement files or prefix.

Tavolato currently builds the complete output `Uint8Array` before upload. `rowGroupSize` bounds active row-group buffers, not final-file memory. A 128 MB serverless process should therefore create multiple smaller objects rather than one object approaching its heap limit.

See [Parquet's file layout](https://parquet.apache.org/docs/file-format/), [DuckDB multi-file reads](https://duckdb.org/docs/stable/data/multiple_files/overview), and [Hive partitioning](https://duckdb.org/docs/stable/data/partitioning/hive_partitioning).

## Errors

Library errors are `TavolatoError` instances with a stable `code` and, when applicable, `column`. Native output includes both, such as `TavolatoError [ERR_ROW_VALUE_INVALID]: …`. Use `isTavolatoError(error, code)` for programmatic handling.

| Code family               | Source                                          |
| ------------------------- | ----------------------------------------------- |
| `ERR_SCHEMA_*`            | Schema and adapter definitions                  |
| `ERR_ROW_*`               | Row validation                                  |
| `ERR_WRITER_*`            | Writer lifecycle, options, and codecs           |
| `ERR_READ_MALFORMED`      | Invalid Parquet bytes                           |
| `ERR_READ_UNSUPPORTED`    | Valid feature outside the supported read subset |
| `ERR_READ_OPTION_INVALID` | Invalid reader configuration                    |
| `ERR_STORE_*`             | Object-store integration                        |

Unsupported compression errors name the codec to register. Unsupported logical annotations name the adapter required through `ReadOptions.types`.

## Compatibility and limits

- Schemas are flat scalar columns. Lists, maps, structs, and repeated fields are unsupported; use a `json` column for nested documents.
- The writer emits Data Page V1, plain values, RLE definition levels, and one page per column chunk per row group. It does not emit dictionaries, `INT96`, page indexes, bloom filters, or page CRCs.
- The reader targets files Tavolato writes. Dictionary encoding, `INT96`, and nested schemas are rejected. Projection can skip unsupported columns.
- Tavolato omits `repetition_type` from the schema root, as defined upstream. The reader accepts valid root repetition values, including `REQUIRED` emitted by DuckDB and other established writers. Unknown values remain malformed.
- Local APIs take a complete `Uint8Array`. `readParquet` also materializes all selected rows; `readRowGroups` reduces decoded-row memory but does not stream the input bytes. Remote selective reads can avoid most unselected chunk bytes when the store honors ranges; small files may still be fetched whole.
- For untrusted files, cap accepted byte size and inspect `readSchema` or `readRowGroups` counts before decoding rows. Registered codecs and adapters are trusted application code.

Files written by Tavolato are cross-read by DuckDB in the test suite. Use DuckDB directly for arbitrary Parquet files, predicate-heavy analytics, or data that should not be materialized in the JavaScript heap.

## Development

Requires Node `>=24.17.0`, pnpm `11.17.0`, and DuckDB on `PATH` for interoperability tests.

- `pnpm dev:prepare` — build development stubs and install hooks
- `pnpm fmt` — regenerate Markdown and format the repository
- `pnpm lint` — lint and check formatting
- `pnpm typecheck` — typecheck source, tests, and playgrounds
- `pnpm test` — run Vitest and DuckDB interoperability tests
- `pnpm build` — build ESM and declarations

## License

<!-- automd:contributors license=MIT -->

Published under the [MIT](https://github.com/sandros94/tavolato/blob/main/LICENSE) license. Made by [community](https://github.com/sandros94/tavolato/graphs/contributors) 💛 <br><br> <a href="https://github.com/sandros94/tavolato/graphs/contributors"> <img src="https://contrib.rocks/image?repo=sandros94/tavolato" /> </a>

<!-- /automd -->

<!-- automd:with-automd -->

---

_🤖 auto updated with [automd](https://automd.unjs.io)_

<!-- /automd -->
