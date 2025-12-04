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
  "metadata": null,
  "version": 1299972097
}
`;

test("parse table and access cells", () => {
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
});
