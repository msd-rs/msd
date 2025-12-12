import { expect, test } from "bun:test";
import { msdQuery } from "../src/query";

const baseURL = "http://127.0.0.1:50510";

test("msdQuery", async () => {
  const query = "SELECT * FROM kline WHERE obj IN ('SH600000', 'SH600001')";
  const result = await msdQuery(query, { baseURL });
  expect(result).toBeDefined();
  expect(typeof result).toBe("object");
  expect(Object.keys(result).length).toBe(2);
  expect(result.SH600000.getRowsCount()).toBeGreaterThan(0);
  expect(result.SH600001.getRowsCount()).toBeGreaterThan(0);
});
