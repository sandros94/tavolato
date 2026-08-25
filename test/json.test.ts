import { describe, expect, it } from "vitest";
import {
  createWriter,
  defineColumnType,
  defineSchema,
  json,
  JSON_NULL,
  jsonReviver,
  readParquet,
  readSchema,
} from "../src/index.ts";
import type { JsonDocument, ParquetSchema, ReadRow, SchemaDefinition } from "../src/index.ts";
import { decodeUtf8, utf8 } from "../src/internal/bytes.ts";
import { expectError } from "./_errors.ts";
import { sync } from "./_sync.ts";

/**
 * The `json` column type, which is the one built-in whose value has an inside.
 *
 * The stored form is a JSON **string** in a `BYTE_ARRAY` annotated `JSON` —
 * unchanged, and the same thing every other engine writes — while the
 * JavaScript value is the structure. Everything below follows from that: the
 * round trip is `JSON.stringify`'s and `JSON.parse`'s, so what it costs is
 * JSON's own semantics rather than any invented here, and the two cases JSON
 * cannot express at all become typed errors.
 */

const schema = defineSchema({ k: { type: "i64" }, doc: { type: "json" } });

/** Writes one document and reads it back. */
function roundtrip(document: unknown): unknown {
  const writer = createWriter(schema);
  writer.append({ k: 0n, doc: document as JsonDocument });
  return readParquet(sync(writer.finish())).rows[0].doc;
}

/** Writes `rows` under `definition` and hands back the bytes. */
function write<TDefinition extends SchemaDefinition>(
  declared: ParquetSchema<TDefinition>,
  rows: readonly unknown[],
): Uint8Array {
  const writer = createWriter(declared);
  for (const row of rows) writer.append(row as never);
  return sync(writer.finish());
}

/**
 * A `json`-annotated column that stores whatever text it is handed, which is
 * the only way to make a file the built-in `json` type cannot parse. Any writer
 * may claim a column is JSON; nothing checks it, which is exactly why the
 * reader has to.
 */
const rawJson = defineColumnType<string, string>({
  name: "raw-json",
  physical: "bytes",
  matches: (annotation) => annotation.kind === "json",
  annotate: () => ({ kind: "json" }),
  read: (raw) => decodeUtf8(raw as Uint8Array),
  write: (value) => utf8.encode(value),
});

describe("the JSON round trip is JSON's", () => {
  it("keeps every shape a document can be", () => {
    for (const document of [
      {},
      [],
      42,
      -0.5,
      true,
      false,
      "text",
      "",
      { a: [1, { b: null }], c: "d" },
      [[[]]],
    ]) {
      expect(roundtrip(document)).toEqual(document);
    }
  });

  it("turns NaN and the infinities into null, as JSON.stringify does", () => {
    // Pinned rather than fixed. JSON has no spelling for these, and inventing
    // one — a string, a sentinel object — would be tavolato deciding what a
    // document means. `f64` is the column that keeps them exactly.
    expect(roundtrip({ nan: Number.NaN, up: Infinity, down: -Infinity })).toEqual({
      nan: null,
      up: null,
      down: null,
    });
    expect(roundtrip([Number.NaN])).toEqual([null]);
  });

  it("drops undefined, function and symbol properties", () => {
    expect(
      roundtrip({ kept: 1, gone: undefined, fn: () => 1, [Symbol("s")]: 2, sym: Symbol("t") }),
    ).toEqual({ kept: 1 });
    // In an array the same values become null: an array has no way to be short
    // a slot, which is JSON.stringify's rule and therefore this one.
    expect(roundtrip([1, undefined, () => 1, 2])).toEqual([1, null, null, 2]);
  });

  it("writes a Date as its ISO string, and reads a string back", () => {
    const at = new Date(1_700_000_000_000);
    expect(roundtrip({ at })).toEqual({ at: at.toISOString() });
    // A `timestamp` column is where a Date stays a Date; inside a document it
    // is whatever `toJSON` made of it, which for a Date is text.
    expect(typeof (roundtrip({ at }) as { at: unknown }).at).toBe("string");
  });

  it("honours toJSON, and flattens what has none", () => {
    expect(roundtrip({ boxed: { toJSON: () => ({ replaced: true }) } })).toEqual({
      boxed: { replaced: true },
    });
    expect(roundtrip({ map: new Map([["a", 1]]), set: new Set([1]) })).toEqual({
      map: {},
      set: {},
    });
  });

  it("loses -0, which JSON writes as 0", () => {
    expect(Object.is((roundtrip({ z: -0 }) as { z: number }).z, 0)).toBe(true);
  });

  it("does not preserve whitespace or key order beyond what stringify produces", () => {
    // The stored text is `JSON.stringify`'s, so insertion order survives and
    // formatting does not exist to survive. Reading gives back the structure,
    // and comparing structures is the only thing that was ever promised.
    const document = { z: 1, a: 2 };
    expect(Object.keys(roundtrip(document) as object)).toEqual(["z", "a"]);
  });
});

describe("values a json column refuses", () => {
  it("refuses a bigint anywhere in the document", () => {
    for (const document of [1n, { n: 1n }, [1n], { deep: { deeper: [1n] } }]) {
      const error = expectError("ERR_ROW_VALUE_INVALID", () =>
        createWriter(schema).append({ k: 0n, doc: document as never }),
      );
      expect(error.column).toBe("doc");
      expect(error.cause).toBeInstanceOf(TypeError);
    }
  });

  it("refuses a value that serializes to nothing at all", () => {
    for (const document of [() => 1, Symbol("nope")]) {
      const error = expectError("ERR_ROW_VALUE_INVALID", () =>
        createWriter(schema).append({ k: 0n, doc: document as never }),
      );
      expect(error.column).toBe("doc");
      expect(error.message).toContain("nothing at all");
    }
    // An object whose `toJSON` opts out reaches the same place.
    expectError("ERR_ROW_VALUE_INVALID", () =>
      createWriter(schema).append({ k: 0n, doc: { toJSON: () => undefined } as never }),
    );
  });

  it("refuses a document that cannot be serialized without touching the writer", () => {
    const writer = createWriter(schema);
    writer.append({ k: 0n, doc: { ok: true } });
    expectError("ERR_ROW_VALUE_INVALID", () => writer.append({ k: 1n, doc: 1n as never }));
    expect(writer.rowCount).toBe(1);
  });

  it("refuses a circular document, with the TypeError as the cause", () => {
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    const error = expectError("ERR_ROW_VALUE_INVALID", () =>
      createWriter(schema).append({ k: 0n, doc: circular as never }),
    );
    expect(error.cause).toBeInstanceOf(TypeError);
  });

  it("treats a top-level null as the column being null, not as the document null", () => {
    // Ordinary JavaScript `null` chooses Parquet absence, which `optional`
    // permits. JSON_NULL is the distinct present JSON document literal.
    const optional = defineSchema({ k: { type: "i64" }, doc: { type: "json", optional: true } });
    const { rows } = readParquet(write(optional, [{ k: 0n, doc: null }, { k: 1n }]));
    expect(rows.map((row) => row.doc)).toEqual([null, null]);

    const error = expectError("ERR_ROW_VALUE_MISSING", () =>
      createWriter(schema).append({ k: 0n, doc: null as never }),
    );
    expect(error.column).toBe("doc");
  });
});

describe("top-level JSON null", () => {
  it("distinguishes the document literal from a Parquet null in the built-in", () => {
    const declared = defineSchema({
      k: { type: "i64" },
      doc: { type: "json", optional: true },
    });
    const bytes = write(declared, [
      { k: 0n, doc: null },
      { k: 1n, doc: JSON_NULL },
      { k: 2n, doc: { nested: null } },
    ]);
    const documents = readParquet(bytes).rows.map((row) => row.doc);
    expect(documents).toEqual([null, JSON_NULL, { nested: null }]);
    expect(documents[1]).toBe(JSON_NULL);
  });

  it("round-trips by identity through the default value adapter", () => {
    const value = json();
    const declared = defineSchema({ doc: { type: value } });
    const bytes = write(declared, [{ doc: JSON_NULL }]);
    expect(readParquet(bytes, { types: [value] }).rows[0].doc).toBe(JSON_NULL);
  });

  it("always spells null without invoking a replacer", () => {
    let calls = 0;
    const value = json({
      replacer: (_key, found) => {
        calls++;
        return found;
      },
    });
    const bytes = write(defineSchema({ doc: { type: value } }), [{ doc: JSON_NULL }]);
    expect(calls).toBe(0);
    expect(readParquet(bytes, { types: [value] }).rows[0].doc).toBe(JSON_NULL);
  });

  it("still lets a reviver transform the root null", () => {
    const value = json<string>({
      reviver: (key, found) => (key === "" && found === null ? "revived" : found),
    });
    const bytes = write(defineSchema({ doc: { type: rawJson } }), [{ doc: "null" }]);
    expect(readParquet(bytes, { types: [value] }).rows[0].doc).toBe("revived");
  });

  it("maps a final root null introduced by a reviver to the singleton", () => {
    const value = json({ reviver: (key, found) => (key === "" ? null : found) });
    const bytes = write(defineSchema({ doc: { type: rawJson } }), [{ doc: '{"present":true}' }]);
    expect(readParquet(bytes, { types: [value] }).rows[0].doc).toBe(JSON_NULL);
  });

  it("refuses the sentinel when it reaches serialization inside an object or array", () => {
    const builtIn = defineSchema({ doc: { type: "json" } });
    const adapted = defineSchema({
      doc: {
        type: json({ replacer: (key, value) => (key === "sentinel" ? undefined : value) }),
      },
    });
    for (const document of [{ sentinel: JSON_NULL }, [JSON_NULL]]) {
      expectError("ERR_ROW_VALUE_INVALID", () =>
        createWriter(builtIn).append({ doc: document as never }),
      );
      const error = expectError("ERR_ROW_VALUE_INVALID", () =>
        createWriter(adapted).append({ doc: document as never }),
      );
      expect(error.message).toContain("JSON_NULL");
    }
  });

  it("allows an ancestor replacer to remove a subtree containing the sentinel", () => {
    const value = json({ replacer: (key, found) => (key === "hidden" ? undefined : found) });
    const document = { kept: true, hidden: { sentinel: JSON_NULL } };
    const bytes = write(defineSchema({ doc: { type: value } }), [{ doc: document as never }]);
    expect(readParquet(bytes).rows[0].doc).toEqual({ kept: true });
  });

  it("allows toJSON to replace a subtree containing the sentinel", () => {
    const document = {
      kept: true,
      hidden: {
        sentinel: JSON_NULL,
        toJSON: () => "replaced",
      },
    };
    const bytes = write(defineSchema({ doc: { type: "json" } }), [{ doc: document as never }]);
    expect(readParquet(bytes).rows[0].doc).toEqual({ kept: true, hidden: "replaced" });
  });

  it("preserves JSON.stringify's replacer holder binding", () => {
    const document = { factor: 2, n: 3 };
    const value = json({
      replacer: function (key, found) {
        if (key === "n") expect(this).toBe(document);
        return found;
      },
    });
    const bytes = write(defineSchema({ doc: { type: value } }), [{ doc: document }]);
    expect(readParquet(bytes).rows[0].doc).toEqual(document);
  });
});

describe("a JSON-annotated column that does not hold JSON", () => {
  /** A file whose `doc` column is annotated JSON but holds `text`. */
  function holding(text: string): Uint8Array {
    return write(defineSchema({ doc: { type: rawJson } }), [{ doc: text }]);
  }

  it("is malformed, named, and carries the SyntaxError", () => {
    const error = expectError("ERR_READ_MALFORMED", () => readParquet(holding("not json at all")));
    expect(error.column).toBe("doc");
    expect(error.message).toContain("not valid JSON");
    expect(error.cause).toBeInstanceOf(SyntaxError);
  });

  it("names the truncated text rather than the whole column value", () => {
    const error = expectError("ERR_READ_MALFORMED", () => readParquet(holding("x".repeat(500))));
    expect(error.message).toContain("…");
    expect(error.message.length).toBeLessThan(200);
  });

  it("still reads its schema, which never touches a page", () => {
    // The annotation is all the footer carries, and it is perfectly well-formed.
    expect(readSchema(holding("{"))).toEqual(
      expect.objectContaining({ columns: [{ name: "doc", type: "json", optional: false }] }),
    );
  });

  it("reads as text again once a column type claims it", () => {
    expect(readParquet(holding("{"), { types: [rawJson] }).rows).toEqual([{ doc: "{" }]);
  });
});

describe("dangerous keys inside a document", () => {
  /**
   * Two different layers, and the tests below are deliberately next to each
   * other. A *column* named `__proto__` is a name Parquet allows and JavaScript
   * finds special, and it round-trips faithfully. A dangerous *key inside a
   * json value* is somebody's document landing in an object your program will
   * use, and the default value-mode policy drops it.
   */
  const polluting = '{"__proto__":{"polluted":true},"constructor":1,"prototype":2,"kept":3}';

  function parsed(options?: { types: readonly ReturnType<typeof json>[] }): ReadRow {
    const bytes = write(defineSchema({ doc: { type: rawJson } }), [{ doc: polluting }]);
    return readParquet(bytes, options).rows[0];
  }

  it("drops __proto__, constructor and prototype by default", () => {
    const document = parsed().doc as Record<string, unknown>;
    expect(document).toEqual({ kept: 3 });
    expect(Object.hasOwn(document, "__proto__")).toBe(false);
    expect(Object.hasOwn(document, "constructor")).toBe(false);
    expect(Object.hasOwn(document, "prototype")).toBe(false);
    // Nothing reached a prototype slot either, which is the point of dropping.
    expect(Object.getPrototypeOf(document)).toBe(Object.prototype);
    expect((document as { polluted?: unknown }).polluted).toBeUndefined();
    expect(({} as { polluted?: unknown }).polluted).toBeUndefined();
  });

  it("still drops them after a custom reviver runs", () => {
    let sawDangerousAtRoot = false;
    const document = parsed({
      types: [
        json({
          reviver: (key, value) => {
            if (key === "") {
              sawDangerousAtRoot = Object.hasOwn(value as object, "constructor");
            }
            return value;
          },
        }),
      ],
    }).doc as Record<string, unknown>;
    expect(sawDangerousAtRoot).toBe(true);
    expect(document).toEqual({ kept: 3 });
    expect(Object.hasOwn(document, "__proto__")).toBe(false);
    expect(Object.hasOwn(document, "constructor")).toBe(false);
    expect(Object.hasOwn(document, "prototype")).toBe(false);
  });

  it("preserves original dangerous own keys only when explicitly requested", () => {
    const document = parsed({ types: [json({ dangerousKeys: "preserve" })] }).doc as Record<
      string,
      unknown
    >;
    expect(Object.hasOwn(document, "__proto__")).toBe(true);
    expect(document.constructor).toBe(1);
    expect(document.prototype).toBe(2);
    // Even kept, `JSON.parse` defines the property rather than assigning it, so
    // the object's own prototype is still untouched.
    expect(Object.getPrototypeOf(document)).toBe(Object.prototype);
  });

  it("drops dangerous own keys introduced by a custom reviver", () => {
    const guarded = json({
      reviver: (key, value) => {
        if (key !== "") return value;
        Object.defineProperty(value, "__proto__", {
          value: { polluted: true },
          configurable: true,
          enumerable: true,
        });
        Object.defineProperty(value, "constructor", {
          value: "introduced",
          configurable: true,
          enumerable: true,
        });
        return value;
      },
    });
    const document = parsed({ types: [guarded] }).doc as Record<string, unknown>;
    expect(document).toEqual({ kept: 3 });
    expect(Object.hasOwn(document, "__proto__")).toBe(false);
    expect(Object.hasOwn(document, "constructor")).toBe(false);
  });

  it("preserves dangerous own keys introduced by a reviver when opted out", () => {
    const preserving = json({
      dangerousKeys: "preserve",
      reviver: (key, value) => {
        if (key !== "") return value;
        Object.defineProperty(value, "__proto__", {
          value: "introduced",
          configurable: true,
          enumerable: true,
        });
        return value;
      },
    });
    const document = parsed({ types: [preserving] }).doc as Record<string, unknown>;
    expect(Object.hasOwn(document, "__proto__")).toBe(true);
    expect(document["__proto__"]).toBe("introduced");
    expect(document.constructor).toBe(1);
    expect(document.prototype).toBe(2);
  });

  it("sanitizes cyclic graphs introduced by a reviver without recursing forever", () => {
    const cyclic = json({
      reviver: (key, value) => {
        if (key !== "") return value;
        Object.defineProperty(value, "self", {
          value,
          configurable: true,
          enumerable: true,
        });
        return value;
      },
    });
    const document = parsed({ types: [cyclic] }).doc as Record<string, unknown>;
    expect(document.self).toBe(document);
  });

  it("preserves identity, prototypes and exotic values introduced by a reviver", () => {
    class Envelope {
      constructor(readonly at: Date) {}
    }
    const at = new Date("2026-08-25T00:00:00.000Z");
    const envelope = new Envelope(at);
    const adapted = json({ reviver: (key, value) => (key === "" ? envelope : value) });
    const document = parsed({ types: [adapted] }).doc as unknown as Envelope;
    expect(document).toBe(envelope);
    expect(document).toBeInstanceOf(Envelope);
    expect(document.at).toBe(at);
  });

  it("refuses a function's locked prototype unless preservation is explicit", () => {
    function callable(): string {
      return "trusted";
    }
    const guarded = json({ reviver: (key, value) => (key === "" ? callable : value) });
    const error = expectError("ERR_READ_MALFORMED", () => parsed({ types: [guarded] }));
    expect(error.column).toBe("doc");

    const preserving = json({
      dangerousKeys: "preserve",
      reviver: (key, value) => (key === "" ? callable : value),
    });
    expect(parsed({ types: [preserving] }).doc).toBe(callable);
  });

  it("removes dangerous accessors without invoking any getter", () => {
    const guarded = json({
      reviver: (key, value) => {
        if (key !== "") return value;
        const fail = (): never => {
          throw new Error("getter ran");
        };
        Object.defineProperty(value, "safe", { get: fail, configurable: true, enumerable: true });
        Object.defineProperty(value, "prototype", {
          get: fail,
          configurable: true,
          enumerable: true,
        });
        return value;
      },
    });
    const document = parsed({ types: [guarded] }).doc as Record<string, unknown>;
    expect(Object.hasOwn(document, "prototype")).toBe(false);
    expect(Object.getOwnPropertyDescriptor(document, "safe")).toMatchObject({
      get: expect.any(Function),
    });
  });

  it("fails with a typed read error rather than leaving a non-configurable key", () => {
    const guarded = json({
      reviver: (key, value) => {
        if (key === "") {
          Object.defineProperty(value, "constructor", {
            value: "locked",
            configurable: false,
            enumerable: true,
          });
        }
        return value;
      },
    });
    const error = expectError("ERR_READ_MALFORMED", () => parsed({ types: [guarded] }));
    expect(error.column).toBe("doc");
  });

  it("fails with a typed read error when graph inspection is trapped", () => {
    const guarded = json({
      reviver: (key, value) =>
        key === ""
          ? new Proxy(value as object, {
              ownKeys: () => {
                throw new Error("trapped");
              },
            })
          : value,
    });
    const error = expectError("ERR_READ_MALFORMED", () => parsed({ types: [guarded] }));
    expect(error.column).toBe("doc");
  });

  it("removes known dangerous keys even when a proxy omits them from ownKeys", () => {
    const guarded = json({
      reviver: (key, value) =>
        key === ""
          ? new Proxy(value as object, {
              ownKeys: (target) =>
                Reflect.ownKeys(target).filter(
                  (found) =>
                    found !== "__proto__" && found !== "constructor" && found !== "prototype",
                ),
            })
          : value,
    });
    const document = parsed({ types: [guarded] }).doc as Record<string, unknown>;
    expect(Object.hasOwn(document, "__proto__")).toBe(false);
    expect(Object.hasOwn(document, "constructor")).toBe(false);
    expect(Object.hasOwn(document, "prototype")).toBe(false);
    expect(document).toEqual({ kept: 3 });
  });

  it("fails when a proxy claims deletion without removing a dangerous key", () => {
    const guarded = json({
      reviver: (key, value) =>
        key === ""
          ? new Proxy(value as object, {
              deleteProperty: (target, found) =>
                found === "constructor" ? true : Reflect.deleteProperty(target, found),
            })
          : value,
    });
    const error = expectError("ERR_READ_MALFORMED", () => parsed({ types: [guarded] }));
    expect(error.column).toBe("doc");
  });

  it("does not inspect an ownKeys-hiding proxy when preservation is explicit", () => {
    let ownKeysCalls = 0;
    const preserving = json({
      dangerousKeys: "preserve",
      reviver: (key, value) =>
        key === ""
          ? new Proxy(value as object, {
              ownKeys: (target) => {
                ownKeysCalls++;
                return Reflect.ownKeys(target).filter((found) => found !== "__proto__");
              },
            })
          : value,
    });
    const document = parsed({ types: [preserving] }).doc as Record<string, unknown>;
    expect(ownKeysCalls).toBe(0);
    expect(Object.hasOwn(document, "__proto__")).toBe(true);
  });

  it("keeps jsonReviver idempotent when a custom reviver composes with it", () => {
    const document = parsed({
      types: [
        json({
          reviver: (key, value) => (key === "kept" ? "seen" : jsonReviver(key, value)),
        }),
      ],
    }).doc as Record<string, unknown>;
    expect(document).toEqual({ kept: "seen" });
    expect(Object.hasOwn(document, "__proto__")).toBe(false);
  });

  it("leaves a column named __proto__ round-tripping, json values and all", () => {
    // The other layer. The column name is the file's, and it comes back whole.
    const named = defineSchema({ ["__proto__"]: { type: "json" }, n: { type: "i64" } });
    const bytes = write(named, [{ ["__proto__"]: { deep: [1, 2] }, n: 1n }]);
    const row = readParquet(bytes).rows[0];

    expect(Object.hasOwn(row, "__proto__")).toBe(true);
    expect(Object.getPrototypeOf(row)).toBe(Object.prototype);
    expect(row["__proto__"]).toEqual({ deep: [1, 2] });
    expect(Object.keys(row)).toEqual(["__proto__", "n"]);
  });
});

describe("the json column type", () => {
  it("writes exactly the bytes the built-in writes", () => {
    const document = { a: [1, 2], b: "café", c: null };
    const builtin = write(defineSchema({ doc: { type: "json" } }), [{ doc: document }]);
    const adapted = write(defineSchema({ doc: { type: json() } }), [{ doc: document }]);
    expect(adapted).toEqual(builtin);
  });

  it("claims the annotation on the way in and back out", () => {
    const bytes = write(defineSchema({ doc: { type: json() } }), [{ doc: { a: 1 } }]);
    // Without it registered the built-in reads the column, and reads it the
    // same way: the adapter is a different set of hooks, not a different type.
    expect(readParquet(bytes).rows).toEqual([{ doc: { a: 1 } }]);
    const claimed = readParquet(bytes, { types: [json()] });
    expect(claimed.rows).toEqual([{ doc: { a: 1 } }]);
    expect((claimed.schema.columns[0].type as { name: string }).name).toBe("json");
  });

  it("runs a replacer on the way in", () => {
    const redacting = json({
      replacer: (key, value) => (key === "secret" ? "REDACTED" : value),
    });
    const bytes = write(defineSchema({ doc: { type: redacting } }), [
      { doc: { secret: "hunter2", kept: 1 } },
    ]);
    expect(readParquet(bytes).rows).toEqual([{ doc: { secret: "REDACTED", kept: 1 } }]);
  });

  it("runs a reviver on the way out, ahead of the built-in", () => {
    const bytes = write(defineSchema({ doc: { type: "json" } }), [{ doc: { n: "1" } }]);
    const counting = json({ reviver: (key, value) => (key === "n" ? Number(value) : value) });
    expect(readParquet(bytes, { types: [counting] }).rows).toEqual([{ doc: { n: 1 } }]);
    // The built-in is what reads it without one.
    expect(readParquet(bytes).rows).toEqual([{ doc: { n: "1" } }]);
  });

  it("preserves JSON.parse's reviver holder binding", () => {
    const bytes = write(defineSchema({ doc: { type: "json" } }), [{ doc: { n: 1 } }]);
    const value = json({
      reviver: function (key, found) {
        if (key === "n") expect(this).toEqual({ n: 1 });
        return found;
      },
    });
    expect(readParquet(bytes, { types: [value] }).rows).toEqual([{ doc: { n: 1 } }]);
  });

  it("turns a parse failure into the same malformed error the built-in raises", () => {
    const bytes = write(defineSchema({ doc: { type: rawJson } }), [{ doc: "{oops" }]);
    const error = expectError("ERR_READ_MALFORMED", () => readParquet(bytes, { types: [json()] }));
    expect(error.column).toBe("doc");
  });

  it("refuses a hook that is not a function", () => {
    // @ts-expect-error deliberately wrong input
    expectError("ERR_SCHEMA_COLUMN_INVALID", () => json({ reviver: "nope" }));
    // @ts-expect-error deliberately wrong input
    expectError("ERR_SCHEMA_COLUMN_INVALID", () => json({ replacer: 1 }));
  });

  it("refuses an unknown dangerous-key policy", () => {
    expectError("ERR_SCHEMA_COLUMN_INVALID", () => json({ dangerousKeys: "remove" as never }));
  });

  it("refuses the same values the built-in refuses", () => {
    const declared = defineSchema({ doc: { type: json() } });
    const error = expectError("ERR_ROW_VALUE_INVALID", () =>
      createWriter(declared).append({ doc: 1n as never }),
    );
    expect(error.column).toBe("doc");
    expectError("ERR_ROW_VALUE_INVALID", () =>
      createWriter(declared).append({ doc: (() => 1) as never }),
    );
  });

  it("preserves lone surrogates through JSON's well-formed escaped representation", () => {
    const value = { text: "\ud800" };
    const declared = defineSchema({
      builtin: { type: "json" },
      adapter: { type: json() },
    });
    const bytes = write(declared, [{ builtin: value, adapter: value }]);
    expect(readParquet(bytes).rows).toEqual([{ builtin: value, adapter: value }]);
  });

  it("round-trips an optional column, nulls included", () => {
    const declared = defineSchema({ k: { type: "i64" }, doc: { type: json(), optional: true } });
    const bytes = write(declared, [{ k: 0n, doc: { a: 1 } }, { k: 1n, doc: null }, { k: 2n }]);
    expect(readParquet(bytes, { types: [json()] }).rows.map((row) => row.doc)).toEqual([
      { a: 1 },
      null,
      null,
    ]);
  });
});

describe("the json text representation", () => {
  const text = json({ as: "text" });

  it("refuses a dangerous-key policy because text is never materialized", () => {
    expectError("ERR_SCHEMA_COLUMN_INVALID", () =>
      json({ as: "text", dangerousKeys: "preserve" } as never),
    );
  });

  it("preserves each complete document exactly, without normalization", () => {
    const declared = defineSchema({ doc: { type: text } });
    const documents = ['{ "b": 2, "a": 1 }\n', "  [1, 2, 3]  ", '"café"', '"\\ud800"', "null"];
    const bytes = write(
      declared,
      documents.map((doc) => ({ doc })),
    );
    expect(readParquet(bytes, { types: [text] }).rows.map((row) => row.doc)).toEqual(documents);
  });

  it("reads valid text written by another JSON adapter unchanged", () => {
    const document = '{\n  "ordered": true,\n  "n": 1\n}\n';
    const bytes = write(defineSchema({ doc: { type: rawJson } }), [{ doc: document }]);
    expect(readParquet(bytes, { types: [text] }).rows).toEqual([{ doc: document }]);
  });

  it("writes the exact validated text bytes", () => {
    const document = '{ "b": 2, "a": 1 }\n';
    const expected = write(defineSchema({ doc: { type: rawJson } }), [{ doc: document }]);
    const actual = write(defineSchema({ doc: { type: text } }), [{ doc: document }]);
    expect(actual).toEqual(expected);
  });

  it.each(["", "{", "undefined", "1 2"])("refuses invalid write text %j", (document) => {
    const error = expectError("ERR_ROW_VALUE_INVALID", () =>
      createWriter(defineSchema({ doc: { type: text } })).append({ doc: document }),
    );
    expect(error.column).toBe("doc");
    expect(error.cause).toBeInstanceOf(Error);
  });

  it("refuses a non-string instead of serializing it", () => {
    expectError("ERR_ROW_VALUE_INVALID", () =>
      createWriter(defineSchema({ doc: { type: text } })).append({
        doc: { parsed: true } as never,
      }),
    );
  });

  it("refuses source text that UTF-8 cannot preserve exactly", () => {
    expectError("ERR_ROW_VALUE_INVALID", () =>
      createWriter(defineSchema({ doc: { type: text } })).append({ doc: '"\ud800"' }),
    );
  });

  it("refuses invalid JSON on read as a malformed, column-scoped file", () => {
    const bytes = write(defineSchema({ doc: { type: rawJson } }), [{ doc: "{" }]);
    const error = expectError("ERR_READ_MALFORMED", () => readParquet(bytes, { types: [text] }));
    expect(error.column).toBe("doc");
  });

  it("refuses a leading UTF-8 BOM instead of stripping it from JSON source", () => {
    const bytes = write(defineSchema({ doc: { type: rawJson } }), [{ doc: "\uFEFF{}" }]);
    const error = expectError("ERR_READ_MALFORMED", () => readParquet(bytes, { types: [text] }));
    expect(error.column).toBe("doc");
    expect((error.cause as Error).message).toContain("not valid JSON");
  });

  it("structurally refuses value hooks and unknown representations", () => {
    expectError("ERR_SCHEMA_COLUMN_INVALID", () =>
      json({ as: "text", reviver: (_key: string, value: unknown) => value } as never),
    );
    expectError("ERR_SCHEMA_COLUMN_INVALID", () =>
      json({ as: "text", replacer: (_key: string, value: unknown) => value } as never),
    );
    expectError("ERR_SCHEMA_COLUMN_INVALID", () => json({ as: "bytes" } as never));
  });
});
