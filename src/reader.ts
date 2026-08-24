import { ByteReader, decodeUtf8 } from "./internal/bytes.ts";
import { chain, chainEach, isThenable } from "./internal/chain.ts";
import { type ColumnValues, decodeRleBitPackedHybrid, readPlain } from "./internal/encoding.ts";
import {
  annotationName,
  annotationOf,
  codecName,
  type ColumnChunkInfo,
  CompressionCodec,
  decodeFileMetadata,
  decodePageHeader,
  Encoding,
  encodingName,
  FieldRepetitionType,
  MAGIC,
  MAX_DEFINITION_LEVEL_BIT_WIDTH,
  type PageHeaderInfo,
  PageType,
  pageTypeName,
  physicalKindOf,
  PhysicalType,
  physicalTypeId,
  physicalTypeName,
  registrableCodec,
  type RowGroupInfo,
  type SchemaElement,
} from "./internal/format.ts";
import { adapterProblem } from "./adapters.ts";
import {
  badOption,
  describe,
  malformed,
  type TavolatoError,
  TYPES_REMEDY,
  unsupported,
} from "./error.ts";
import type {
  Annotation,
  AnyLogicalAdapter,
  ColumnType,
  ParquetFile,
  ParquetRowGroups,
  ParquetSchema,
  PhysicalKind,
  ReadOptions,
  ReadRow,
  ReadValue,
  SchemaColumn,
  SchemaDefinition,
  SyncParquetRowGroups,
} from "./types.ts";

/** Leading magic, the footer length, and the trailing magic: the smallest envelope. */
const MIN_FILE_BYTES = MAGIC.length * 2 + 4;

/**
 * The instant furthest from the epoch a `Date` can hold, in milliseconds.
 * A `TIMESTAMP(MILLIS)` past it has no reading as a `Date` but an invalid one.
 */
const MAX_DATE_MILLIS = 8_640_000_000_000_000n;

/**
 * A column with everything the decode path needs resolved once, where the
 * column is claimed.
 *
 * An adapter is a caller's object: `physical` is a property on it rather than
 * something the file carries, and reading it again for every page would let an
 * object that answers differently between the footer and the last row decode
 * values as a type the pages are not. So the *file's* physical type is what is
 * kept here — the one an adapter had to agree with to claim the column at all —
 * and every page is read from that.
 */
interface ReadColumn {
  readonly name: string;
  readonly optional: boolean;
  /** The column type as the schema hands it back: a built-in name, or the adapter. */
  readonly type: ColumnType | AnyLogicalAdapter;
  /** Byte width of a `FIXED_LEN_BYTE_ARRAY` column, as the file declares it. */
  readonly typeLength: number | undefined;
  /** Where the values are read from. */
  readonly physical: PhysicalKind;
}

/** A file's schema, in both the shape callers see and the shape pages are read with. */
interface FileSchema {
  readonly schema: ParquetSchema;
  readonly columns: readonly ReadColumn[];
}

function hasMagic(bytes: Uint8Array, offset: number): boolean {
  for (let index = 0; index < MAGIC.length; index++) {
    if (bytes[offset + index] !== MAGIC[index]) return false;
  }
  return true;
}

/**
 * Validates the file envelope and returns a view over the footer's
 * `FileMetaData` bytes.
 */
function locateFooter(bytes: Uint8Array): Uint8Array {
  if (!(bytes instanceof Uint8Array)) {
    throw malformed("readParquet expects a Uint8Array");
  }
  if (bytes.length <= MIN_FILE_BYTES) {
    throw malformed(
      `A Parquet file is at least ${MIN_FILE_BYTES + 1} bytes, received ${bytes.length}`,
    );
  }
  if (!hasMagic(bytes, 0) || !hasMagic(bytes, bytes.length - MAGIC.length)) {
    throw malformed("Not a Parquet file: it must both start and end with the PAR1 magic");
  }
  const length = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(
    bytes.length - MAGIC.length - 4,
    true,
  );
  if (length === 0 || length > bytes.length - MIN_FILE_BYTES) {
    throw malformed(
      `The footer declares ${length} bytes of metadata, which does not fit a ${bytes.length} byte file`,
    );
  }
  const end = bytes.length - MAGIC.length - 4;
  return bytes.subarray(end - length, end);
}

/**
 * The built-in column type for a bare physical type, or `undefined` where the
 * annotation means something tavolato has no built-in reading for.
 *
 * Two leniencies live here, and neither moves a value. `INT_32` / `INT_64`
 * (however they are spelled) say exactly what the bare physical type already
 * says, so they read as `i32` and `i64`; and a `TIMESTAMP(MILLIS)` reads as a
 * `Date` whichever way its UTC flag points, because a `Date` is an instant and
 * the milliseconds are the same number either way. Together they are what lets
 * DuckDB's own `COPY … (FORMAT PARQUET)` output be read directly.
 */
function builtinTypeOf(physical: PhysicalKind, annotation: Annotation): ColumnType | undefined {
  const bare = annotation.kind === "none";
  switch (physical) {
    case "bool": {
      return bare ? "bool" : undefined;
    }
    case "f64": {
      return bare ? "f64" : undefined;
    }
    case "f32": {
      return bare ? "f32" : undefined;
    }
    case "i32": {
      return bare || isPlainInteger(annotation, 32) ? "i32" : undefined;
    }
    case "i64": {
      if (bare || isPlainInteger(annotation, 64)) return "i64";
      return annotation.kind === "timestamp" && annotation.unit === "millis"
        ? "timestamp"
        : undefined;
    }
    case "bytes": {
      if (annotation.kind === "string") return "string";
      return annotation.kind === "json" ? "json" : undefined;
    }
    default: {
      // A FIXED_LEN_BYTE_ARRAY never has a built-in meaning: every use of it
      // in the format is an annotation, and an annotation is an adapter's.
      return undefined;
    }
  }
}

/** Whether an annotation says no more than the signed physical type it sits on. */
function isPlainInteger(annotation: Annotation, bitWidth: 32 | 64): boolean {
  return annotation.kind === "integer" && annotation.bitWidth === bitWidth && annotation.isSigned;
}

/**
 * Finds the adapter that claims a column, if any.
 *
 * Registration order is the resolution order: the first adapter whose physical
 * type, byte width and `matches` all agree takes the column. A `matches` that
 * throws is the caller's option misbehaving, not the file's fault, and says so.
 */
function claimedBy(
  element: SchemaElement,
  physical: PhysicalKind,
  typeLength: number | undefined,
  annotation: Annotation,
  types: readonly AnyLogicalAdapter[],
): AnyLogicalAdapter | undefined {
  for (const adapter of types) {
    if (adapter.physical !== physical) continue;
    if (physical === "fixed" && adapter.typeLength !== typeLength) continue;
    let claimed: boolean;
    try {
      claimed = adapter.matches(annotation, physical);
    } catch (cause) {
      throw badOption(
        `The column type ${adapter.name} threw from matches() on column "${element.name}"`,
        element.name,
        cause,
      );
    }
    if (claimed) return adapter;
  }
  return undefined;
}

/** Maps one leaf `SchemaElement` onto a column, or refuses it by name. */
function columnOf(element: SchemaElement, types: readonly AnyLogicalAdapter[]): ReadColumn {
  const { name, physical } = element;
  if (physical === undefined) {
    throw malformed(`Column "${name}" declares no physical type`, name);
  }
  if (physical === PhysicalType.INT96) {
    // Deprecated by the format itself. tavolato may refuse anything Parquet has
    // deprecated outright, and owes no hook for it — this is the named case.
    throw unsupported(
      `column "${name}", an INT96 — a type the format deprecated, and one tavolato refuses permanently`,
      name,
    );
  }
  const kind = physicalKindOf(physical);
  if (kind === undefined) {
    throw malformed(
      `Column "${name}" declares the physical type ${physical}, which Parquet does not define`,
      name,
    );
  }

  let typeLength: number | undefined;
  if (kind === "fixed") {
    typeLength = element.typeLength;
    if (typeLength === undefined || !Number.isSafeInteger(typeLength) || typeLength < 1) {
      throw malformed(
        `Column "${name}" is a FIXED_LEN_BYTE_ARRAY whose type_length is ${describe(element.typeLength)}`,
        name,
      );
    }
  }

  const optional = element.repetition === FieldRepetitionType.OPTIONAL;
  const annotation = annotationOf(element);
  const adapter = claimedBy(element, kind, typeLength, annotation, types);
  if (adapter !== undefined) {
    return { name, type: adapter, optional, typeLength, physical: kind };
  }

  const builtin = builtinTypeOf(kind, annotation);
  if (builtin !== undefined) {
    return { name, type: builtin, optional, typeLength: undefined, physical: kind };
  }

  // Nothing claimed it, and tavolato will not guess which JavaScript type an
  // annotation ought to become — so it says what it found, in full.
  const found = `${physicalTypeName(physical)}${typeLength === undefined ? "" : `(${typeLength})`}`;
  throw unsupported(
    annotation.kind === "none"
      ? `column "${name}", an unannotated ${found}`
      : `column "${name}", a ${found} annotated ${annotationName(annotation)}`,
    name,
    TYPES_REMEDY,
  );
}

/**
 * Turns the footer's depth-first schema list into a `ParquetSchema`, refusing
 * anything that is not one flat level of leaves under the root.
 */
function toSchema(
  elements: readonly SchemaElement[],
  types: readonly AnyLogicalAdapter[],
): FileSchema {
  const root = elements[0];
  if (root === undefined) throw malformed("The footer carries no schema");
  if (root.numChildren === 0) throw unsupported("a file whose schema declares no columns");

  const leaves = elements.slice(1);
  for (const element of leaves) {
    if (element.numChildren > 0) {
      throw unsupported(
        `column "${element.name}", a group of ${element.numChildren} nested fields — tavolato is flat, forever`,
        element.name,
      );
    }
    if (element.repetition === FieldRepetitionType.REPEATED) {
      throw unsupported(`column "${element.name}", a REPEATED field`, element.name);
    }
  }
  if (leaves.length !== root.numChildren) {
    throw malformed(
      `The schema root declares ${root.numChildren} children but ${leaves.length} elements follow it`,
    );
  }

  const columns: ReadColumn[] = [];
  const declared: SchemaColumn[] = [];
  const definition: SchemaDefinition = {};
  for (const element of leaves) {
    if (element.name === "") throw malformed("A column in the schema has an empty name");
    if (Object.hasOwn(definition, element.name)) {
      throw malformed(`The schema declares the column "${element.name}" twice`, element.name);
    }
    const column = columnOf(element, types);
    columns.push(column);
    declared.push(publicColumn(column));
    // An adapter column carries the adapter object itself, so the definition a
    // file yields is still valid input to `createWriter`.
    definition[element.name] = { type: column.type, optional: column.optional };
  }

  // Built directly rather than through `defineSchema` so that column order is
  // the file's order even for integer-like column names, which an object's own
  // key order would reshuffle.
  const schema = Object.freeze({
    columns: Object.freeze(declared) as readonly SchemaColumn[],
    definition: Object.freeze(definition),
  });
  return { schema, columns };
}

/** The half of a column a caller sees: the resolved physical type stays inside. */
function publicColumn(column: ReadColumn): SchemaColumn {
  const { name, type, optional, typeLength } = column;
  return Object.freeze({
    name,
    type,
    optional,
    ...(typeLength === undefined ? {} : { typeLength }),
  });
}

/** Converts one decoded PLAIN value into the type the column promises. */
function toValue(column: ReadColumn, values: ColumnValues, index: number): ReadValue {
  const { type } = column;
  if (typeof type !== "string") return adapt(column, type, values.items[index]);
  switch (values.kind) {
    case "bytes": {
      return decodeUtf8(values.items[index]);
    }
    case "i64": {
      const value = values.items[index];
      if (type !== "timestamp") return value;
      // A `timestamp` column is a `Date`, and a `Date` runs out before an
      // INT64 does. Handing back an Invalid Date would be this library
      // quietly losing a value it can see perfectly well; the count is still
      // there for the asking, through the adapter that reads it as one.
      if (value < -MAX_DATE_MILLIS || value > MAX_DATE_MILLIS) {
        throw unsupported(
          `column "${column.name}", a TIMESTAMP(MILLIS) holding ${value} milliseconds — past the range a JavaScript Date can represent`,
          column.name,
          `register timestamp({ unit: "millis" }) in ReadOptions.types to read the count itself`,
        );
      }
      return new Date(Number(value));
    }
    default: {
      return values.items[index];
    }
  }
}

/**
 * Runs an adapter's `read` on one raw physical value, turning anything it
 * throws into a typed error carrying the original as `cause`.
 *
 * The value is cast on the way out: what an adapter returns is its own
 * business, and `ReadRowOf` is where its real type comes back.
 */
function adapt(column: ReadColumn, adapter: AnyLogicalAdapter, raw: unknown): ReadValue {
  try {
    return adapter.read(raw) as ReadValue;
  } catch (cause) {
    throw malformed(
      `The column type ${adapter.name} failed on a value of column "${column.name}"`,
      column.name,
      cause,
    );
  }
}

/**
 * Turns one page body as it sits in the file into the bytes it decodes from.
 * The identity for an uncompressed chunk, a wrapped hook for every other.
 */
type PageDecompressor = (
  body: Uint8Array,
  uncompressedSize: number,
) => Uint8Array | PromiseLike<Uint8Array>;

const passThrough: PageDecompressor = (body) => body;

/**
 * Resolves the decompressor a column chunk needs, refusing the chunk when it
 * asks for a codec nobody registered.
 *
 * The wrapper is where a third-party decoder is held to tavolato's contract: it
 * is called with bytes already bounded against the file, whatever it throws or
 * rejects with becomes a typed error carrying the original as `cause`, and what
 * it returns has to be exactly as long as the page header promised.
 */
function pageDecompressor(
  column: ReadColumn,
  chunk: ColumnChunkInfo,
  codecs: ReadOptions["codecs"],
): PageDecompressor {
  if (chunk.codec === CompressionCodec.UNCOMPRESSED) return passThrough;

  const name = registrableCodec(chunk.codec);
  const registered = name === undefined ? undefined : codecs?.[name];
  if (registered === undefined || typeof registered.decompress !== "function") {
    throw unsupported(
      `column "${column.name}", compressed with ${codecName(chunk.codec)}`,
      column.name,
      name === undefined
        ? undefined
        : `register a decompressor for ${name} in ReadOptions.codecs to read it anyway`,
    );
  }

  const check = (result: unknown, uncompressedSize: number): Uint8Array => {
    if (!(result instanceof Uint8Array)) {
      throw malformed(
        `The ${name} decompressor returned something other than bytes for column "${column.name}"`,
        column.name,
      );
    }
    if (result.length !== uncompressedSize) {
      throw malformed(
        `A page of column "${column.name}" declares ${uncompressedSize} uncompressed bytes but the ${name} decompressor produced ${result.length}`,
        column.name,
      );
    }
    return result;
  };
  const failed = (cause: unknown): TavolatoError =>
    malformed(
      `The ${name} decompressor failed on a page of column "${column.name}"`,
      column.name,
      cause,
    );

  return (body, uncompressedSize) => {
    let result: Uint8Array | Promise<Uint8Array>;
    try {
      // Called as a method, so a codec that keeps state on `this` still works.
      result = registered.decompress(body, uncompressedSize);
    } catch (cause) {
      throw failed(cause);
    }
    if (!isThenable(result)) return check(result, uncompressedSize);

    // `chain` only forwards fulfilment, so a rejection is caught here instead.
    const settled = result.then(
      (value) => check(value, uncompressedSize),
      (cause: unknown) => {
        throw failed(cause);
      },
    );
    // A thenable is only a thenable by duck typing. One whose `then` returns
    // nothing to chain on — the callback style that predates promises — would
    // otherwise smuggle that `undefined` onward in place of the page bytes.
    if (!isThenable(settled)) {
      throw malformed(
        `The ${name} decompressor returned a thenable whose then() produced nothing to chain on, for column "${column.name}"`,
        column.name,
      );
    }
    return settled;
  };
}

/** Decodes a page body — levels and values — and appends every row to `out`. */
function readPageBody(
  body: ByteReader,
  column: ReadColumn,
  page: PageHeaderInfo,
  out: ReadValue[],
): void {
  let levels: readonly number[] | undefined;
  let present = page.numValues;
  if (column.optional) {
    if (page.definitionLevelEncoding !== Encoding.RLE) {
      throw unsupported(
        `column "${column.name}", whose definition levels are ${encodingName(page.definitionLevelEncoding)} encoded`,
        column.name,
      );
    }
    // A v1 page prefixes the level stream with its length as 4 bytes LE.
    const decoded = decodeRleBitPackedHybrid(
      body.raw(body.u32()),
      MAX_DEFINITION_LEVEL_BIT_WIDTH,
      page.numValues,
    );
    present = 0;
    for (const level of decoded) if (level === 1) present++;
    levels = decoded;
  }

  // Nulls take a definition level but no bytes, so the value cursor only moves
  // on the rows that are present.
  const values = readPlain(body, column.physical, present, column.typeLength);
  let next = 0;
  for (let index = 0; index < page.numValues; index++) {
    out.push(levels !== undefined && levels[index] === 0 ? null : toValue(column, values, next++));
  }
}

/**
 * Reads one v1 data page and appends its values — nulls included — to `out`.
 *
 * `remaining` is how many rows of the row group are still unread; a page may
 * not claim more than that. That is a *consistency* check, not a memory bound:
 * it catches a truncated file and a count corrupted in one place, and it fails
 * fast rather than acting on a lie. It cannot bound allocation, because
 * `remaining` derives from the footer's own row counts, which come from the
 * same file — corrupt them all consistently and the check passes. Nothing here
 * can bound a *validly* encoded page either: an RLE run of six bytes can
 * legitimately declare millions of nulls, and a reader has no way to tell that
 * from a sparse file someone meant to write. See `readParquet` for the memory
 * bound that follows.
 */
function readDataPage(
  input: ByteReader,
  column: ReadColumn,
  out: ReadValue[],
  remaining: number,
  decompress: PageDecompressor,
): void | Promise<void> {
  const page = decodePageHeader(input);
  if (page.pageType !== PageType.DATA_PAGE) {
    throw unsupported(
      `column "${column.name}", stored in a ${pageTypeName(page.pageType)}`,
      column.name,
    );
  }
  if (page.encoding !== Encoding.PLAIN) {
    throw unsupported(
      `column "${column.name}", ${encodingName(page.encoding)} encoded`,
      column.name,
    );
  }
  if (page.numValues <= 0 || page.numValues > remaining) {
    throw malformed(
      `A data page for column "${column.name}" declares ${page.numValues} values with ${remaining} rows left in the row group`,
      column.name,
    );
  }

  // `raw` bounds the compressed length against the file *before* the hook sees
  // a byte, which is the one guarantee tavolato can still make about a page it
  // does not decode itself.
  const raw = input.raw(page.compressedSize);
  return chain(decompress(raw, page.uncompressedSize), (body) => {
    readPageBody(new ByteReader(body), column, page, out);
  });
}

/**
 * Everything about a column chunk that can be settled from the footer alone,
 * ending in the decompressor its pages will need.
 *
 * Split out because it touches no page bytes: `readRowGroups` runs it over
 * every group up front, so a file that cannot be read at all says so when it is
 * opened rather than from whichever step first reaches the bad chunk.
 */
function prepareColumnChunk(
  column: ReadColumn,
  chunk: ColumnChunkInfo,
  numRows: number,
  codecs: ReadOptions["codecs"],
): PageDecompressor {
  if (chunk.path.length !== 1 || chunk.path[0] !== column.name) {
    throw malformed(
      `A row group holds a chunk for "${chunk.path.join(".")}" where the schema declares "${column.name}"`,
      column.name,
    );
  }
  const expected = physicalTypeId(column.physical);
  if (chunk.physical !== undefined && chunk.physical !== expected) {
    throw malformed(
      `Column "${column.name}" is a ${physicalTypeName(expected)} in the schema but a ${physicalTypeName(chunk.physical)} in a row group`,
      column.name,
    );
  }
  const decompress = pageDecompressor(column, chunk, codecs);
  if (chunk.dictionaryPageOffset !== undefined) {
    throw unsupported(`column "${column.name}", which is dictionary encoded`, column.name);
  }
  // A flat schema has exactly one value per row, nulls included.
  if (chunk.numValues !== numRows) {
    throw malformed(
      `Column "${column.name}" declares ${chunk.numValues} values in a row group of ${numRows} rows`,
      column.name,
    );
  }
  return decompress;
}

/** Reads every page of one column chunk into a column of `numRows` values. */
function readColumnChunk(
  input: ByteReader,
  column: ReadColumn,
  chunk: ColumnChunkInfo,
  numRows: number,
  codecs: ReadOptions["codecs"],
): ReadValue[] | Promise<ReadValue[]> {
  const decompress = prepareColumnChunk(column, chunk, numRows, codecs);

  const out: ReadValue[] = [];
  input.seek(chunk.dataPageOffset);
  // The writer emits exactly one page per chunk, but a chunk is a sequence of
  // pages in the format, so read until the row group's rows are covered. The
  // loop only turns into a promise chain if a decompressor defers.
  const readPages = (): void | Promise<void> => {
    while (out.length < numRows) {
      const pending = readDataPage(input, column, out, numRows - out.length, decompress);
      if (isThenable(pending)) return chain(pending, readPages);
    }
  };
  return chain(readPages(), () => out);
}

/**
 * Reads one row group's column chunks and appends its rows to `rows`.
 *
 * The one place a row group is decoded: `readParquet` runs it over every group
 * into a single array, and `readRowGroups` runs it one group at a time into an
 * array of its own. There is no second decoding path, and never should be.
 */
function readRowGroup(
  input: ByteReader,
  columns: readonly ReadColumn[],
  group: RowGroupInfo,
  rows: ReadRow[],
  codecs: ReadOptions["codecs"],
): void | Promise<void> {
  assertChunkCount(columns, group);

  // Chunks share one cursor over the file, so they are read strictly in order.
  const values: ReadValue[][] = [];
  return chain(
    chainEach(columns.length, (index) =>
      chain(
        readColumnChunk(input, columns[index], group.columns[index], group.numRows, codecs),
        (column) => {
          values[index] = column;
        },
      ),
    ),
    () => {
      for (let row = 0; row < group.numRows; row++) {
        const record: ReadRow = {};
        for (const [index, column] of columns.entries()) {
          record[column.name] = values[index][row];
        }
        rows.push(record);
      }
    },
  );
}

/** A row group holds one chunk per column, and the footer has to say so twice. */
function assertChunkCount(columns: readonly ReadColumn[], group: RowGroupInfo): void {
  if (group.columns.length !== columns.length) {
    throw malformed(
      `A row group holds ${group.columns.length} column chunks but the schema declares ${columns.length} columns`,
    );
  }
}

/**
 * Everything the footer says, validated: the schema, the row groups, and the
 * counts they have to agree on. No page byte is touched here.
 */
interface FooterInfo extends FileSchema {
  readonly rowGroups: readonly RowGroupInfo[];
  readonly rowCount: number;
}

/**
 * Reads and validates the footer — the envelope, the metadata, the schema — and
 * nothing else. The entry point every read shares, eager or lazy.
 */
function readFooter(bytes: Uint8Array, options: ReadOptions | undefined): FooterInfo {
  const metadata = decodeFileMetadata(locateFooter(bytes));
  const { schema, columns } = toSchema(metadata.schema, registeredTypes(options));

  // Cross-checked before a single page is touched: the row groups must account
  // for exactly the rows the footer promises, so a count corrupted in one place
  // is caught rather than acted on.
  let declared = 0;
  for (const group of metadata.rowGroups) declared += group.numRows;
  if (declared !== metadata.numRows) {
    throw malformed(
      `The footer declares ${metadata.numRows} rows but its row groups add up to ${declared}`,
    );
  }

  return { schema, columns, rowGroups: metadata.rowGroups, rowCount: metadata.numRows };
}

/**
 * Reads a Parquet file written by `tavolato`.
 *
 * The returned `schema` is derived from the file and has the shape
 * `defineSchema` produces; `rows` holds every row, in file order. Values come
 * back in exactly one JavaScript type per column type — `i64` is always a
 * `bigint` and `timestamp` always a `Date`, even where the writer would also
 * have accepted a `number` — and a null in an optional column is `null`.
 *
 * Anything outside the subset tavolato writes (a nested schema, dictionary
 * encoding, v2 pages, a type or annotation it has no column type for) raises
 * `ERR_READ_UNSUPPORTED` naming what it found. Bytes that are not a well-formed
 * Parquet file raise `ERR_READ_MALFORMED`.
 *
 * Two refusals lift, and both lift the same way — by handing over the piece
 * tavolato will not supply itself. A compressed column chunk is refused until a
 * decompressor for its codec is registered in `options.codecs`; an annotated
 * column is refused until a matching column type is passed in `options.types`,
 * because which JavaScript type a `DECIMAL` or a `UUID` should become is a
 * decision about your program, not about the file.
 *
 * `types` are tried in order and claim a column before the built-in types get
 * to. Only a codec can make this defer: called without options, with `types`
 * alone, or with a synchronous decompressor, it returns a `ParquetFile`
 * outright.
 *
 * Memory is `O(rows declared in the footer)`, not `O(bytes)`: definition levels
 * are RLE compressed, so a six byte run can legitimately declare millions of
 * nulls, and a file that does so is indistinguishable from a sparse one someone
 * meant to write. Reading a small file can therefore allocate a large result.
 * That is fine for your own files; for **untrusted** input, cap the byte length
 * you accept and use {@link readSchema} plus your own row limit before
 * committing to a full read. {@link readRowGroups} is the same read one row
 * group at a time, which brings that bound down to the largest single group.
 *
 * @example
 * const { schema, rows } = readParquet(bytes);
 * schema.columns; // [{ name: "n", type: "i64", optional: false }]
 * rows[0].n; // 42n
 *
 * @example
 * import { gunzipSync } from "node:zlib";
 * const codecs = { GZIP: { decompress: (page: Uint8Array) => gunzipSync(page) } };
 * const { rows } = readParquet(bytes, { codecs });
 *
 * @example
 * const { rows } = readParquet(bytes, { types: [uuid(), decimal({ precision: 12, scale: 2 })] });
 *
 * @throws {TavolatoError} `ERR_READ_MALFORMED`, `ERR_READ_UNSUPPORTED` or
 * `ERR_READ_OPTION_INVALID`.
 */
export function readParquet(
  bytes: Uint8Array,
  options?: ReadOptions & { codecs?: undefined },
): ParquetFile;
export function readParquet(
  bytes: Uint8Array,
  options: ReadOptions,
): ParquetFile | Promise<ParquetFile>;
export function readParquet(
  bytes: Uint8Array,
  options?: ReadOptions,
): ParquetFile | Promise<ParquetFile> {
  const { schema, columns, rowGroups } = readFooter(bytes, options);
  const input = new ByteReader(bytes);
  const rows: ReadRow[] = [];
  const codecs = options?.codecs;

  // The eager composition of the lazy read: the same footer, the same group
  // decode, every group's rows into one array.
  return chain(
    chainEach(rowGroups.length, (index) =>
      readRowGroup(input, columns, rowGroups[index], rows, codecs),
    ),
    () => ({ schema, rows }),
  );
}

/**
 * Reads a Parquet file one **row group** at a time.
 *
 * A Parquet file is sliced horizontally into row groups, and each one is an
 * independently decodable segment carrying all of the columns for its slice of
 * the rows. {@link readParquet} materializes every row of every group at once,
 * which is `O(all declared rows)` of memory; this decodes one group per
 * iteration step, which is `O(the rows of one group)` — DuckDB writes about
 * 122k rows per group by default, and some writers put a single group in a
 * file.
 *
 * The footer is still read up front, eagerly and in full: that is where the
 * schema and the groups' locations live, so there is nothing to be lazy about
 * there. Anything wrong at that level — a bad envelope, a schema outside the
 * subset, an annotation nothing claims, a chunk that contradicts the schema, a
 * codec nobody registered — throws from this call. Only page-level problems
 * wait for the step that reaches them. This is lazy *decoding* over bytes you
 * already hold, not streaming input: `bytes` is referenced for as long as the
 * result is used.
 *
 * Each step yields that group's rows, under the same rule the codec hooks
 * follow everywhere else: with no codec or a synchronous one the value is the
 * rows array itself and the walk allocates no promises at all; an asynchronous
 * decompressor makes *that step's* value a promise to await. One group is one
 * maybe-promise, never a mix of the two.
 *
 * The state of a walk lives in its iterator, not in the object, so every
 * `[Symbol.iterator]()` starts again at group 0 and two walks may run at once
 * without disturbing each other. Steps are independent of one another too —
 * each owns its cursor over the bytes — so they may be pulled without being
 * awaited: `await Promise.all([...file])` decodes every group concurrently and
 * is the eager read again, group by group. A step that throws or rejects has
 * consumed its group: the next step moves on to the following one, and the walk
 * still ends after `groupCount` steps.
 *
 * @example
 * const file = readRowGroups(bytes);
 * file.rowCount; // 250_000, from the footer
 * for (const group of file) {
 *   for (const row of group) count += 1; // one group in memory at a time
 * }
 *
 * @example
 * // With an asynchronous decompressor, each step is a promise:
 * for (const group of readRowGroups(bytes, { codecs })) {
 *   for (const row of await group) count += 1;
 * }
 *
 * @throws {TavolatoError} `ERR_READ_MALFORMED`, `ERR_READ_UNSUPPORTED` or
 * `ERR_READ_OPTION_INVALID`.
 */
export function readRowGroups(
  bytes: Uint8Array,
  options?: ReadOptions & { codecs?: undefined },
): SyncParquetRowGroups;
export function readRowGroups(bytes: Uint8Array, options: ReadOptions): ParquetRowGroups;
export function readRowGroups(bytes: Uint8Array, options?: ReadOptions): ParquetRowGroups {
  const { schema, columns, rowGroups, rowCount } = readFooter(bytes, options);
  const codecs = options?.codecs;

  // Every chunk-level check the footer can answer runs here, over every group:
  // a file that cannot be read at all should say so when it is opened, not
  // three steps into a walk that has already handed back rows.
  for (const group of rowGroups) {
    assertChunkCount(columns, group);
    for (const [index, column] of columns.entries()) {
      prepareColumnChunk(column, group.columns[index], group.numRows, codecs);
    }
  }

  return Object.freeze({
    schema,
    rowCount,
    groupCount: rowGroups.length,
    [Symbol.iterator](): IterableIterator<ReadRow[] | Promise<ReadRow[]>> {
      let next = 0;
      const iterator: IterableIterator<ReadRow[] | Promise<ReadRow[]>> = {
        next(): IteratorResult<ReadRow[] | Promise<ReadRow[]>> {
          if (next >= rowGroups.length) return { done: true, value: undefined };
          const group = rowGroups[next++];
          // A cursor of its own, per **step** rather than per walk, and an
          // array to go with it.
          //
          // A column chunk is read page by page from one position, and a codec
          // puts an `await` between two of those reads — so a step holding a
          // cursor anything else can move comes back to find it moved, and
          // reads another group's pages as its own. Owning it is what makes
          // steps that overlap safe, which is to say what makes collecting
          // them and awaiting them together — `Promise.all([...file])` — a
          // concurrent read rather than a corrupt one. A `ByteReader` is a
          // view over `bytes`, so the whole guarantee costs one small object.
          //
          // The array is the step's for the same reason it is fresh: once it
          // has been yielded nothing here refers to it, which is what makes
          // the memory bound real.
          const input = new ByteReader(bytes);
          const rows: ReadRow[] = [];
          return {
            done: false,
            value: chain(readRowGroup(input, columns, group, rows, codecs), () => rows),
          };
        },
        [Symbol.iterator](): IterableIterator<ReadRow[] | Promise<ReadRow[]>> {
          return iterator;
        },
      };
      return iterator;
    },
  });
}

/**
 * Reads only the schema of a Parquet file: the footer is parsed, the pages are
 * not touched.
 *
 * Useful to check what a file holds before deciding to read it — and it
 * rejects an unsupported *schema* just as `readParquet` would, though an
 * unsupported *encoding* only surfaces once pages are read. `types` claim
 * columns here exactly as they do there, since claiming happens in the footer.
 *
 * @throws {TavolatoError} `ERR_READ_MALFORMED`, `ERR_READ_UNSUPPORTED` or
 * `ERR_READ_OPTION_INVALID`.
 */
export function readSchema(bytes: Uint8Array, options?: ReadOptions): ParquetSchema {
  return toSchema(decodeFileMetadata(locateFooter(bytes)).schema, registeredTypes(options)).schema;
}

/**
 * Validates `options.types` once, before a footer is looked at, and freezes
 * each of them.
 *
 * An entry that is not a column type would otherwise surface as a `TypeError`
 * from somewhere inside a column, breaking the one promise that holds
 * everywhere else: everything this library throws is a `TavolatoError`. The
 * freeze is the cheap half of the same idea as `ReadColumn`: a column type is a
 * value, and the read it takes part in should not depend on when it was asked.
 */
function registeredTypes(options: ReadOptions | undefined): readonly AnyLogicalAdapter[] {
  const types = options?.types;
  if (types === undefined) return [];
  if (!Array.isArray(types)) {
    throw badOption(`ReadOptions.types must be an array, received ${describe(types)}`);
  }
  for (const [index, adapter] of types.entries()) {
    const problem = adapterProblem(adapter);
    if (problem !== undefined) throw badOption(`ReadOptions.types[${index}] ${problem}`);
    Object.freeze(adapter);
  }
  return types;
}
