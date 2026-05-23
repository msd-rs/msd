import { expect, test } from "bun:test";
import { parseMsdTable, parseTableBin } from "../src";

const testTable = `
{
  "columns": [
    {
      "data": [
        1735660800000000,
        1735747200000000,
        1735833600000000
      ],
      "kind": "DateTime",
      "metadata": null,
      "name": "ts"
    },
    {
      "data": [
        31520,
        5136,
        7696
      ],
      "kind": "Decimal64",
      "metadata": null,
      "name": "price"
    },
    {
      "data": null,
      "kind": "Null",
      "metadata": null,
      "name": "null"
    }
  ],
  "metadata": {
    "obj": "SH600000"
  },
  "version": 1299972097
}
`;

test("parse table and access cells", () => {
  performance.mark("parse-start");
  for (let i = 0; i < 10000; i++) {
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
    price: number | null;
    null: null;
  };

  for (const row of msdTable as Iterable<rowType>) {
    expect(row["ts"] instanceof Date).toBe(true);
    expect(typeof row["price"]).toBe("number");
    expect(row["null"]).toBeNull();
    //console.log(JSON.stringify(row));
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

const tableBuf = await Bun.file("tests/fixtures/test_table.bin").arrayBuffer();
test("parse binary table and access cells", async () => {
  performance.mark("parse-start");
  for (let i = 0; i < 10000; i++) {
    parseTableBin(tableBuf);
  }
  performance.mark("parse-end");

  performance.measure("parse", "parse-start", "parse-end");
  const measurements = performance.getEntriesByName("parse");
  console.log(JSON.stringify(measurements, null, 2));

  const msdTable = parseTableBin(tableBuf);

  //console.log(JSON.stringify(msdTable, null, 2));

  expect(msdTable.getRowsCount()).toBe(3);
  expect(msdTable.getColumnsCount()).toBe(3);
  expect(msdTable.getMetadata("obj")).toBe("SH600000");

  expect(msdTable.cell<Date>(0, 0).toISOString()).toBe(
    "2024-12-31T16:00:00.000Z",
  );
  expect(msdTable.cell<number>(0, 1)).toBe(1.23);
  expect(msdTable.cell(0, 2)).toBeNull();

  expect(msdTable.cell<Date>(1, 0).toISOString()).toBe(
    "2025-01-01T16:00:00.000Z",
  );
  expect(msdTable.cell<number>(1, 1)).toBe(2.0);

  expect(msdTable.cell<Date>(2, 0).toISOString()).toBe(
    "2025-01-02T16:00:00.000Z",
  );
  expect(msdTable.cell<number>(2, 1)).toBe(3.0);
});

const frameBuf = await Bun.file("tests/fixtures/test_frame.bin").arrayBuffer();
test("parse packed frame and access cells", async () => {
  const msdTable = parseTableBin(frameBuf);

  //console.log(JSON.stringify(msdTable, null, 2));

  expect(msdTable.getRowsCount()).toBe(3);
  expect(msdTable.getColumnsCount()).toBe(3);
  expect(msdTable.getMetadata("obj")).toBe("SH600000");

  expect(msdTable.cell<Date>(0, 0).toISOString()).toBe(
    "2024-12-31T16:00:00.000Z",
  );
  expect(msdTable.cell<number>(0, 1)).toBe(1.23);
});
