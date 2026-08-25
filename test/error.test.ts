import { describe, expect, it } from "vitest";
import { describe as describeValue, TavolatoError, unsupported } from "../src/error.ts";

function hostileTag<T extends object>(target: T): T {
  return new Proxy(target, {
    get(source, key, receiver) {
      if (key === Symbol.toStringTag) throw new Error("no tag");
      return Reflect.get(source, key, receiver);
    },
  });
}

describe("value diagnostics", () => {
  it("uses stable fallbacks when object and function tags trap", () => {
    expect(describeValue(hostileTag({}))).toBe("an object");
    expect(describeValue(hostileTag(() => undefined))).toBe("a function");
  });
});

describe("error rendering", () => {
  it("includes the machine-readable code in native error output", () => {
    const error = new TavolatoError(
      'Column "total" expects an i32 integer; received "12".',
      "ERR_ROW_VALUE_INVALID",
      "total",
    );

    expect(String(error)).toBe(
      'TavolatoError [ERR_ROW_VALUE_INVALID]: Column "total" expects an i32 integer; received "12".',
    );
    expect(error.stack?.split("\n", 1)[0]).toBe(String(error));
    expect(error).toMatchObject({
      code: "ERR_ROW_VALUE_INVALID",
      column: "total",
      message: 'Column "total" expects an i32 integer; received "12".',
    });
  });

  it("keeps custom codes on one unambiguous line", () => {
    const code = "ERR_CUSTOM]\n\u001b[31mforged";
    const error = new TavolatoError("message", code);

    expect(String(error)).toBe(
      "TavolatoError [ERR_CUSTOM\\u{5d}\\u{a}\\u{1b}\\u{5b}31mforged]: message",
    );
    expect(String(error)).not.toContain("\n");
    expect(error.code).toBe(code);
    expect(error.stack?.split("\n", 1)[0]).toBe(String(error));
  });

  it("renders unsupported features as a concise problem and remedy", () => {
    const error = unsupported(
      'column "total", compressed with GZIP',
      "total",
      "register a GZIP decompressor in ReadOptions.codecs to read it anyway",
    );

    expect(String(error)).toBe(
      'TavolatoError [ERR_READ_UNSUPPORTED]: Cannot read column "total", compressed with GZIP. Register a GZIP decompressor in ReadOptions.codecs to read it anyway.',
    );
    expect(error.column).toBe("total");
  });

  it("renders unsupported features without remedies concisely", () => {
    expect(unsupported("a dictionary-encoded column").message).toBe(
      "Cannot read a dictionary-encoded column. This feature is outside tavolato's supported Parquet subset.",
    );
  });
});
