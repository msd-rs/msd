import { expect, test } from "bun:test";
import { parseMsdTable, parseTableBin, parseTableBinV2 } from "../src";
import { Field, SchemaFromFields } from "../src/table";

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
  const objs = msdTable.column("obj")

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

  // Test 1: Explicit generic argument at call site
  const tsColumn = msdTable.column("ts");
  expect(tsColumn).toBeDefined();
  expect(tsColumn instanceof Float64Array).toBe(true);
  expect(tsColumn?.length).toBe(3);
  expect(typeof tsColumn?.[0]).toBe("number");

  const priceColumn = msdTable.column("price");
  expect(priceColumn).toBeDefined();
  expect(priceColumn instanceof Float64Array).toBe(true);

  // Test 2: Un-annotated call site (returns SeriesTypes[keyof SeriesTypes] | null)
  const tsColUnannotated = msdTable.column("ts");
  expect(tsColUnannotated).toBeDefined();

  // Test 3: Automatic inference via table schema generic parameter
  type TestSchema = [
    { name: "ts"; metadata: null; kind: "DateTime"; data: Float64Array },
    { name: "price"; metadata: null; kind: "Decimal64"; data: Float64Array },
    { name: "null"; metadata: null; kind: "Null"; data: null },
  ];
  const typedTable = parseMsdTable<TestSchema>(testTable);
  const autoInferredTs = typedTable.column("ts"); // Automatically Float64Array | null!
  expect(autoInferredTs instanceof Float64Array).toBe(true);

  const autoInferredPrice = typedTable.column(1); // Automatically Float64Array | null!
  expect(autoInferredPrice instanceof Float64Array).toBe(true);

  const nullColumn = msdTable.column("null");
  expect(nullColumn).toBeNull();
});

const tableBuf = await Bun.file(
  "/home/jia/repo/msd-rs2/bindings/typescript/tests/fixtures/test_table.bin",
).arrayBuffer();
test("parse binary table and access cells", async () => {
  performance.mark("parse-start");
  for (let i = 0; i < 10000; i++) {
    parseTableBin(new DataView(tableBuf));
  }
  performance.mark("parse-end");

  performance.measure("parse", "parse-start", "parse-end");
  const measurements = performance.getEntriesByName("parse");
  console.log(JSON.stringify(measurements, null, 2));

  const msdTable = parseTableBin(new DataView(tableBuf));

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

const frameBuf = await Bun.file(
  "/home/jia/repo/msd-rs2/bindings/typescript/tests/fixtures/test_frame.bin",
).arrayBuffer();
test("parse packed frame and access cells", async () => {
  const msdTable = parseTableBin(new DataView(frameBuf));

  //console.log(JSON.stringify(msdTable, null, 2));

  expect(msdTable.getRowsCount()).toBe(3);
  expect(msdTable.getColumnsCount()).toBe(3);
  expect(msdTable.getMetadata("obj")).toBe("SH600000");

  expect(msdTable.cell<Date>(0, 0).toISOString()).toBe(
    "2024-12-31T16:00:00.000Z",
  );
  expect(msdTable.cell<number>(0, 1)).toBe(1.23);
});

const frameV2Buf = await Bun.file(
  "/home/jia/repo/msd-rs2/bindings/typescript/tests/fixtures/v2.bin",
).arrayBuffer();
test("parse packed frame v2 and access cells", async () => {
  const msdTable = parseTableBinV2(new DataView(frameV2Buf));

  expect(msdTable.getRowsCount()).toBe(100);
  expect(msdTable.getColumnsCount()).toBe(7);
  expect(msdTable.getMetadata("obj")).toBe("SH600000");

  expect(msdTable.cell<Date>(0, 0).toISOString()).toBe(
    "1999-11-09T16:00:00.000Z",
  );

  // Also test parseTableBin auto-dispatching to v2
  const autoTable = parseTableBin(new DataView(frameV2Buf));
  expect(autoTable.getRowsCount()).toBe(100);
  expect(autoTable.getMetadata("obj")).toBe("SH600000");
});
