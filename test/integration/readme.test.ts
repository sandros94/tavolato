import { describe, expect, inject, it } from "vitest";
import { storeEvents } from "../../playgrounds/object-store.ts";
import { connect } from "./_wire.ts";

describe.each(inject("stores"))("README playgrounds on $name", (backend) => {
  it("writes and selectively reads an object", async () => {
    const result = await storeEvents(connect(backend).client);

    expect(result.head).toMatchObject({ rowCount: 1, groupCount: 1 });
    expect(result.rows).toEqual([{ count: 1n }]);
  });
});
