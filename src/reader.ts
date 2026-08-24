import { ByteReader, decodeUtf8 } from "./internal/bytes.ts";
import { chain, chainEach, isThenable } from "./internal/chain.ts";
import { type ColumnValues, decodeRleBitPackedHybrid, readPlain } from "./internal/encoding.ts";
import {
  annotationName,
  annotationOf,
  codecName,
  type ColumnChunkInfo,
  columnPhysical,
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
  ParquetSchema,
  PhysicalKind,
  ReadOptions,
  ReadRow,
  ReadValue,
  SchemaColumn,
  SchemaDefinition,
} from "./types.ts";

/** Leading magic, the footer length, and the trailing magic: the smallest envelope. */
const MIN_FILE_BYTES = MAGIC.length * 2 + 4;

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
function columnOf(element: SchemaElement, types: readonly AnyLogicalAdapter[]): SchemaColumn {
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
    return Object.freeze({
      name,
      type: adapter,
      optional,
      ...(typeLength === undefined ? {} : { typeLength }),
    });
  }

  const builtin = builtinTypeOf(kind, annotation);
  if (builtin !== undefined) return Object.freeze({ name, type: builtin, optional });

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
): ParquetSchema {
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

  const columns: SchemaColumn[] = [];
  const definition: SchemaDefinition = {};
  for (const element of leaves) {
    if (element.name === "") throw malformed("A column in the schema has an empty name");
    if (Object.hasOwn(definition, element.name)) {
      throw malformed(`The schema declares the column "${element.name}" twice`, element.name);
    }
    const column = columnOf(element, types);
    columns.push(column);
    // An adapter column carries the adapter object itself, so the definition a
    // file yields is still valid input to `createWriter`.
    definition[element.name] = { type: column.type, optional: column.optional };
  }

  // Built directly rather than through `defineSchema` so that column order is
  // the file's order even for integer-like column names, which an object's own
  // key order would reshuffle.
  return Object.freeze({
    columns: Object.freeze(columns) as readonly SchemaColumn[],
    definition: Object.freeze(definition),
  });
}

/** Converts one decoded PLAIN value into the type the column promises. */
function toValue(column: SchemaColumn, values: ColumnValues, index: number): ReadValue {
  const { type } = column;
  if (typeof type !== "string") return adapt(column, type, values.items[index]);
  switch (values.kind) {
    case "bytes": {
      return decodeUtf8(values.items[index]);
    }
    case "i64": {
      const value = values.items[index];
      return type === "timestamp" ? new Date(Number(value)) : value;
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
function adapt(column: SchemaColumn, adapter: AnyLogicalAdapter, raw: unknown): ReadValue {
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
  column: SchemaColumn,
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
  column: SchemaColumn,
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
  const values = readPlain(body, columnPhysical(column.type), present, column.typeLength);
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
  column: SchemaColumn,
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

/** Reads every page of one column chunk into a column of `numRows` values. */
function readColumnChunk(
  input: ByteReader,
  column: SchemaColumn,
  chunk: ColumnChunkInfo,
  numRows: number,
  codecs: ReadOptions["codecs"],
): ReadValue[] | Promise<ReadValue[]> {
  if (chunk.path.length !== 1 || chunk.path[0] !== column.name) {
    throw malformed(
      `A row group holds a chunk for "${chunk.path.join(".")}" where the schema declares "${column.name}"`,
      column.name,
    );
  }
  const expected = physicalTypeId(columnPhysical(column.type));
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

/** Reads one row group's column chunks and appends its rows to `rows`. */
function readRowGroup(
  input: ByteReader,
  schema: ParquetSchema,
  group: RowGroupInfo,
  rows: ReadRow[],
  codecs: ReadOptions["codecs"],
): void | Promise<void> {
  if (group.columns.length !== schema.columns.length) {
    throw malformed(
      `A row group holds ${group.columns.length} column chunks but the schema declares ${schema.columns.length} columns`,
    );
  }

  // Chunks share one cursor over the file, so they are read strictly in order.
  const values: ReadValue[][] = [];
  return chain(
    chainEach(schema.columns.length, (index) =>
      chain(
        readColumnChunk(input, schema.columns[index], group.columns[index], group.numRows, codecs),
        (column) => {
          values[index] = column;
        },
      ),
    ),
    () => {
      for (let row = 0; row < group.numRows; row++) {
        const record: ReadRow = {};
        for (const [index, column] of schema.columns.entries()) {
          record[column.name] = values[index][row];
        }
        rows.push(record);
      }
    },
  );
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
 * committing to a full read.
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
  const metadata = decodeFileMetadata(locateFooter(bytes));
  const schema = toSchema(metadata.schema, registeredTypes(options));
  const input = new ByteReader(bytes);
  const rows: ReadRow[] = [];

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

  const codecs = options?.codecs;
  return chain(
    chainEach(metadata.rowGroups.length, (index) =>
      readRowGroup(input, schema, metadata.rowGroups[index], rows, codecs),
    ),
    () => ({ schema, rows }),
  );
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
  return toSchema(decodeFileMetadata(locateFooter(bytes)).schema, registeredTypes(options));
}

/**
 * Validates `options.types` once, before a footer is looked at.
 *
 * An entry that is not a column type would otherwise surface as a `TypeError`
 * from somewhere inside a column, breaking the one promise that holds
 * everywhere else: everything this library throws is a `TavolatoError`.
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
  }
  return types;
}
