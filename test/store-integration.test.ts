import { describe, expect, it } from "vitest";
import { S3Client, S3Error } from "uns3";
import { createWriter, defineSchema, readParquet } from "../src/index.ts";
import { createParquetStore } from "../src/uns3.ts";
import { FakeS3 } from "./_store.ts";
import { sync } from "./_sync.ts";

/*
 * ---------------------------------------------------------------------------
 * The store, against the real `uns3` client.
 *
 * `S3ClientConfig` takes a `fetch`, so the actual client — URL building, header
 * serialization, status checking, XML parsing and all — can be run in process
 * against the same in-memory S3 the rest of the suite uses. Nothing here is
 * mocked on tavolato's side: what is being tested is that the parameters the
 * store builds are the parameters `uns3` knows what to do with.
 *
 * Two of the tests below were deliberately red until `uns3` 0.0.8: its `get`
 * checked every response against a hard-coded status list and threw away the
 * `206 Partial Content` it had itself asked for, so the store's ranged reads
 * could not run on the real client at all. Since 0.0.8 the client honors
 * `params.expectedStatus` — the store sends `[200, 206]` — and the ranged path
 * below runs unpatched.
 * ---------------------------------------------------------------------------
 */

/** Serves an in-memory S3 over `fetch`, which is all `uns3` needs to be real. */
function transport(s3: FakeS3): typeof fetch {
  // The client is the one deciding what an acceptable status is here, so the
  // fake stops throwing and answers like a socket would.
  s3.quirks.neverThrow = true;

  return async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const request = new Request(input, init);
    const url = new URL(request.url);
    const [, bucket = "", ...rest] = url.pathname.split("/");
    const key = decodeURIComponent(rest.join("/"));
    const headers = request.headers;

    if (url.searchParams.get("list-type") === "2") {
      const maxKeys = url.searchParams.get("max-keys");
      const result = await s3.list({
        bucket,
        prefix: url.searchParams.get("prefix") ?? undefined,
        delimiter: url.searchParams.get("delimiter") ?? undefined,
        ...(maxKeys === null ? {} : { maxKeys: Number(maxKeys) }),
      });
      return new Response(listXml(result), { status: 200 });
    }

    const range = parseRange(headers.get("range"));
    const ifMatch = headers.get("if-match") ?? undefined;
    switch (request.method) {
      case "GET": {
        return await s3.get({ bucket, key, ...(range === undefined ? {} : { range }), ifMatch });
      }
      case "HEAD": {
        return await s3.head({ bucket, key, ifMatch });
      }
      case "PUT": {
        const body = new Uint8Array(await request.arrayBuffer());
        return await s3.put({ bucket, key, body, ifMatch });
      }
      case "DELETE": {
        return await s3.del({ bucket, key });
      }
      default: {
        return new Response(null, { status: 405 });
      }
    }
  };
}

/** `bytes=10-20` / `bytes=-64`, as `uns3` writes them and S3 reads them. */
function parseRange(header: string | null): { start?: number; end?: number } | undefined {
  if (header === null) return undefined;
  const match = /^bytes=(\d*)-(\d*)$/.exec(header.trim());
  if (match === null) return undefined;
  if (match[1] === "") return { end: Number(match[2]) };
  return match[2] === ""
    ? { start: Number(match[1]) }
    : {
        start: Number(match[1]),
        end: Number(match[2]),
      };
}

/** The ListObjectsV2 XML `uns3` parses back. */
function listXml(result: Awaited<ReturnType<FakeS3["list"]>>): string {
  const contents = result.contents
    .map(
      (object) =>
        `<Contents><Key>${object.key}</Key><Size>${object.size}</Size><ETag>${
          object.etag ?? ""
        }</ETag><LastModified>${object.lastModified}</LastModified><StorageClass>${
          object.storageClass ?? "STANDARD"
        }</StorageClass></Contents>`,
    )
    .join("");
  const prefixes = result.commonPrefixes
    .map((prefix) => `<CommonPrefixes><Prefix>${prefix}</Prefix></CommonPrefixes>`)
    .join("");
  return `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>${contents}${prefixes}<IsTruncated>${String(
    result.isTruncated,
  )}</IsTruncated>${
    result.nextContinuationToken === undefined
      ? ""
      : `<NextContinuationToken>${result.nextContinuationToken}</NextContinuationToken>`
  }</ListBucketResult>`;
}

function realStore(options?: { tailBytes?: number }) {
  const s3 = new FakeS3();
  const client = new S3Client({
    endpoint: "http://s3.test",
    bucketStyle: "path",
    defaultBucket: "b",
    region: "auto",
    credentials: { accessKeyId: "key", secretAccessKey: "secret" },
    fetch: transport(s3),
  });
  return { s3, store: createParquetStore(client, { bucket: "b", ...options }) };
}

const schema = defineSchema({ n: { type: "i64" }, s: { type: "string" } });
const rows = Array.from({ length: 300 }, (_unused, index) => ({
  n: BigInt(index),
  s: `row ${index}`,
}));

function file(): Uint8Array {
  const writer = createWriter(schema, { rowGroupSize: 100 });
  for (const row of rows) sync(writer.append(row));
  return sync(writer.finish());
}

describe("the store on uns3's own client", () => {
  it("puts rows and reads every one of them back", async () => {
    const { s3, store } = realStore();

    await store.put("events/a.parquet", { schema, rows });
    const read = await store.get("events/a.parquet");

    expect(read.rows).toEqual(sync(readParquet(s3.stored("events/a.parquet") as Uint8Array)).rows);
    expect(read.rows).toHaveLength(300);
    expect(read.rows[42]).toEqual({ n: 42n, s: "row 42" });
  });

  it("puts finished bytes as they are", async () => {
    const { s3, store } = realStore();
    const bytes = file();

    await store.put("events/b.parquet", bytes);

    expect(s3.stored("events/b.parquet")).toEqual(bytes);
  });

  it("lists and deletes", async () => {
    const { s3, store } = realStore();
    await store.put("events/a.parquet", file());
    await store.put("events/b.parquet", file());
    await store.put("other.parquet", file());

    const listed = await store.list({ prefix: "events/" });
    expect(listed.contents.map((object) => object.key)).toEqual([
      "events/a.parquet",
      "events/b.parquet",
    ]);
    expect(listed.isTruncated).toBe(false);

    const grouped = await store.list({ delimiter: "/" });
    expect(grouped.commonPrefixes).toEqual(["events/"]);

    await store.del("events/a.parquet");
    expect(s3.stored("events/a.parquet")).toBeUndefined();
  });

  it("passes an S3 failure through as uns3's own error", async () => {
    const { store } = realStore();
    await expect(store.get("missing.parquet")).rejects.toBeInstanceOf(S3Error);
  });

  /*
   * These two were red until `uns3` 0.0.8 — see the note at the top. A small
   * `tailBytes` keeps the footer fetch from swallowing the whole test file, so
   * the ranged path is exercised for real.
   */

  it("range-reads through the real client", async () => {
    const { s3, store } = realStore({ tailBytes: 256 });
    await store.put("events/a.parquet", file());
    s3.reset();

    const read = await store.get("events/a.parquet", { columns: ["n"], groups: [1] });

    expect(read.rows).toEqual(rows.slice(100, 200).map((row) => ({ n: row.n })));
    // Fewer bytes than the object holds, and every request carried a range
    // `uns3` itself serialized.
    expect(s3.bytesServed).toBeLessThan((s3.stored("events/a.parquet") as Uint8Array).length);
    expect(s3.ranges.every((range) => range !== undefined)).toBe(true);
  });

  it("heads without reading a page", async () => {
    const { s3, store } = realStore({ tailBytes: 256 });
    await store.put("events/a.parquet", file());
    s3.reset();

    const head = await store.head("events/a.parquet");

    expect(head.rowCount).toBe(300);
    expect(head.groupCount).toBe(3);
    expect(head.size).toBe((s3.stored("events/a.parquet") as Uint8Array).length);
    expect(s3.bytesServed).toBeLessThan((s3.stored("events/a.parquet") as Uint8Array).length);
  });
});
