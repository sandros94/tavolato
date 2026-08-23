import { describe, expect, it } from "vitest";
import type { PutObjectParams, S3Client } from "uns3";
import { createWriter, defineSchema } from "../src/index.ts";
import { PARQUET_CONTENT_TYPE, putParquet } from "../src/uns3.ts";
import { sync } from "./_sync.ts";
import * as root from "../src/index.ts";

/**
 * `putParquet` only ever calls `client.put`, so a recording stub standing in
 * for `S3Client` is enough to pin the call it makes.
 */
function stubClient(): { client: S3Client; calls: PutObjectParams[] } {
  const calls: PutObjectParams[] = [];
  const client = {
    put(params: PutObjectParams): Promise<Response> {
      calls.push(params);
      return Promise.resolve(new Response(null, { status: 200 }));
    },
  } as unknown as S3Client;
  return { client, calls };
}

const schema = defineSchema({ n: { type: "i64" } });

describe("putParquet", () => {
  it("uploads raw bytes with the Parquet media type", async () => {
    const { client, calls } = stubClient();
    const writer = createWriter(schema);
    writer.append({ n: 1n });
    const bytes = sync(writer.finish());

    const response = await putParquet(client, { bucket: "b", key: "a.parquet" }, bytes);

    expect(response.status).toBe(200);
    expect(calls).toHaveLength(1);
    expect(calls[0].bucket).toBe("b");
    expect(calls[0].key).toBe("a.parquet");
    expect(calls[0].contentType).toBe(PARQUET_CONTENT_TYPE);
    expect(calls[0].body).toBe(bytes);
  });

  it("finishes a writer handed over directly", async () => {
    const { client, calls } = stubClient();
    const writer = createWriter(schema);
    writer.append({ n: 1n });

    await putParquet(client, { bucket: "b", key: "a.parquet" }, writer);

    expect(writer.finished).toBe(true);
    expect(calls[0].body).toBeInstanceOf(Uint8Array);
    expect((calls[0].body as Uint8Array).length).toBeGreaterThan(12);
  });

  it("lets the caller override the content type", async () => {
    const { client, calls } = stubClient();
    await putParquet(
      client,
      { bucket: "b", key: "a.parquet", contentType: "application/octet-stream" },
      new Uint8Array(),
    );
    expect(calls[0].contentType).toBe("application/octet-stream");
  });

  it("lets the caller suppress content type detection", async () => {
    const { client, calls } = stubClient();
    await putParquet(
      client,
      { bucket: "b", key: "a.parquet", contentType: false },
      new Uint8Array(),
    );
    expect(calls[0].contentType).toBe(false);
  });

  it("passes every other put parameter through", async () => {
    const { client, calls } = stubClient();
    const signal = AbortSignal.timeout(1000);
    await putParquet(
      client,
      {
        bucket: "b",
        key: "a.parquet",
        cacheControl: "no-store",
        ifNoneMatch: "*",
        headers: { "x-amz-meta-source": "tavolato" },
        signal,
      },
      new Uint8Array(),
    );
    expect(calls[0].cacheControl).toBe("no-store");
    expect(calls[0].ifNoneMatch).toBe("*");
    expect(calls[0].headers).toEqual({ "x-amz-meta-source": "tavolato" });
    expect(calls[0].signal).toBe(signal);
  });
});

describe("export map", () => {
  it("keeps the uns3 helper out of the root entry point", () => {
    expect(Object.keys(root).sort()).toEqual([
      "ParquetWriter",
      "TavolatoError",
      "createWriter",
      "defineSchema",
      "isTavolatoError",
      "readParquet",
      "readSchema",
    ]);
  });
});
