import { expect, test } from "bun:test";
import { msdQuery } from "../src/query";

const baseURL = "http://127.0.0.1:50510";

test("msdQuery", async () => {
  const query = "SELECT * FROM stock_kline_1d WHERE obj IN ('SH600000', 'SZ000001')";
  const result = await msdQuery(query, { baseURL });
  console.log(JSON.stringify(result, null, 2));
  expect(result).toBeDefined();
  expect(typeof result).toBe("object");
  expect(Object.keys(result).length).toBe(2);
  expect(result.SH600000.getRowsCount()).toBeGreaterThan(0);
  expect(result.SZ000001.getRowsCount()).toBeGreaterThan(0);
  expect(result.SH600000.getMetadata("obj")).toBe("SH600000");
  expect(result.SZ000001.getMetadata("obj")).toBe("SZ000001");
});


test("benchQuery", async () => {
  const query = "SELECT * FROM kline WHERE obj = 'SH6000*'";
  const result = await msdQuery(query, { baseURL });
  performance.mark("benchQuery-start");
  await msdQuery(query, { baseURL });

  let rows = 0;
  const objects = Object.keys(result).length;
  for (const obj of Object.values(result)) {
    rows += obj.getRowsCount();
  }

  performance.mark("benchQuery-end");

  performance.measure("benchQuery", "benchQuery-start", "benchQuery-end");
  const measurements = performance.getEntriesByName("benchQuery");
  const d = measurements[0].duration;
  console.log(`fetch ${objects} objects, ${rows} rows in ${d.toFixed(2)} ms, ${(1000 * rows / d).toFixed(2)} rows/s`);
})