import { expect, test } from "bun:test";
import { parseMsdTable } from "../src";

const testTable = `
{
  "columns": [
    {
      "data": {
        "DateTime": [
          1735660800000000,
          1735747200000000,
          1735833600000000
        ]
      },
      "kind": "DateTime",
      "metadata": null,
      "name": "ts"
    },
    {
      "data": {
        "Decimal64": [
          31520,
          5136,
          7696
        ]
      },
      "kind": "Decimal64",
      "metadata": null,
      "name": "price"
    },
    {
      "data": "Null",
      "kind": "Null",
      "metadata": null,
      "name": "null"
    }
  ],
  "metadata": {
    "obj": {
      "String": "SH600000"
      }
  },
  "version": 1299972097
}
`;

test("parse table and access cells", () => {
  performance.mark("parse-start");
  for (let i = 0; i < 1000; i++) {
    parseMsdTable(testTable);
  }
  performance.mark("parse-end");

  performance.measure("parse", "parse-start", "parse-end");
  const measurements = performance.getEntriesByName("parse");
  console.log(JSON.stringify(measurements, null, 2));

  const msdTable = parseMsdTable(testTable);
  expect(msdTable.getRowsCount()).toBe(3);
  expect(msdTable.getColumnsCount()).toBe(3);

  type rowType = {
    ts: Date | null;
    price: string | null;
    null: null;
  };

  for (const row of msdTable as Iterable<rowType>) {
    expect(row["ts"] instanceof Date).toBe(true);
    expect(typeof row["price"]).toBe("string");
    expect(row["null"]).toBeNull();
    console.log(JSON.stringify(row));
  }

  const tsColumn = msdTable.column("ts");
  expect(tsColumn).toBeDefined();
  expect(tsColumn?.length).toBe(3);
  expect(typeof tsColumn?.[0]).toBe("number");
  expect(typeof tsColumn?.[1]).toBe("number");
  expect(typeof tsColumn?.[2]).toBe("number");

  const priceColumn = msdTable.column("price");
  expect(priceColumn).toBeDefined();
  expect(priceColumn?.length).toBe(3);
  expect(typeof priceColumn?.[0]).toBe("number");
  expect(typeof priceColumn?.[1]).toBe("number");
  expect(typeof priceColumn?.[2]).toBe("number");

  const nullColumn = msdTable.column("null");
  expect(nullColumn).toBeNull();
});
