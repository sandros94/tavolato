import { describe, expect, it } from "vitest";
import { describe as describeValue } from "../src/error.ts";

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
