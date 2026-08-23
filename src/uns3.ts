import type { PutObjectParams, S3Client } from "uns3";

/**
 * IANA media type for Apache Parquet.
 *
 * @see https://www.iana.org/assignments/media-types/application/vnd.apache.parquet
 */
export const PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet";

/**
 * Anything that can hand over a finished Parquet file. `finish` may defer,
 * which is what a `ParquetWriter` with an asynchronous codec does.
 */
export interface ParquetSource {
  finish(): Uint8Array | Promise<Uint8Array>;
}

/**
 * `uns3` put parameters, minus the body (which comes from the writer) and with
 * a Parquet-aware default for `contentType`.
 */
export type PutParquetParams = Omit<PutObjectParams, "body">;

/**
 * Uploads a Parquet file to S3 with `uns3`.
 *
 * Accepts either raw bytes or anything with a `finish()` method — a
 * `ParquetWriter` can be handed over directly, in which case it is finished
 * here and becomes unusable afterwards.
 *
 * `contentType` defaults to {@link PARQUET_CONTENT_TYPE}; pass your own value
 * (or `false` to let `uns3` resolve it from the key) to override.
 *
 * `uns3` is an optional peer dependency and is only used for its types: this
 * helper never imports it at runtime, it just calls `client.put`.
 *
 * @example
 * import { S3Client } from "uns3";
 * import { createWriter, defineSchema } from "tavolato";
 * import { putParquet } from "tavolato/uns3";
 *
 * const writer = createWriter(defineSchema({ n: { type: "i64" } }));
 * writer.append({ n: 1n });
 *
 * const client = new S3Client({ ... });
 * await putParquet(client, { bucket: "metrics", key: "events/2026-08-22.parquet" }, writer);
 */
export async function putParquet(
  client: S3Client,
  params: PutParquetParams,
  source: Uint8Array | ParquetSource,
): Promise<Response> {
  const body = source instanceof Uint8Array ? source : await source.finish();
  return await client.put({
    contentType: PARQUET_CONTENT_TYPE,
    ...params,
    body,
  });
}
