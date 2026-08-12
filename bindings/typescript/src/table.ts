/**
 * Copyright 2026 MSD-RS Project LiJia
 * SPDX-License-Identifier: agpl-3.0-only
 */

export type SeriesTypes = {
  String: string[];
  Bytes: Uint8Array[];
  Int32: Int32Array;
  UInt32: Uint32Array;
  Int64: BigInt64Array;
  UInt64: BigUint64Array;
  Float32: Float32Array;
  Float64: Float64Array;
  Decimal64: Float64Array;
  Bool: boolean[];
  DateTime: Float64Array;
  Null: null;
};

export type SeriesType<T extends keyof SeriesTypes> = SeriesTypes[T];
export type SeriesKind = keyof SeriesTypes;
export type DataForKind<K extends SeriesKind> = SeriesTypes[K];



export type Field = {
  [K in SeriesKind]: {
    name: string;
    metadata: Record<string, any> | null;
    kind: K;
    data: SeriesType<K>;
  }
}[SeriesKind]

export type SchemaFromFields<Cols extends readonly Field[]> = {
  [C in Cols[number] as C['name']]: C['kind'];
};

export type TableSchema = Record<string, SeriesKind>

export type MsdTable = {
  columns: Field[];
  metadata: Record<string, any> | null;
};


export type MsdTableApi<Schema extends TableSchema = TableSchema> = {
  /**
   * Get the number of rows in the table
   */
  getRowsCount(): number;
  /**
   * Get the number of columns in the table
   */
  getColumnsCount(): number;
  /**
   * Get the value of a cell at a specific row and column
   * @param row The row index
   * @param column The column index
   * @returns The value of the cell
   */
  cell(row: number, column: number): any;
  /**
   * Get the values of all cells in a specific row
   * @param row The row index
   * @returns An object containing the values of all cells in the row
   */
  row(row: number): any;

  /**
   * Get the values of all cells in a specific column
   * @param column The column index or name
   * @returns An array or TypedArray containing the values of all cells in the column
   */
  column<Name extends string & keyof Schema>(
    column: Name | number
  ): Schema[Name] extends SeriesKind
    ? SeriesTypes[Schema[Name]]
    : Field['data'];

  /**
   * Get the metadata of a specific column
   * @param key The key of the metadata
   * @returns The value of the metadata if it exists, otherwise null
   */
  getMetadata(key: string): any;

  /**
   * Iterate over all rows in the table
   * @returns An iterator over all rows in the table
   */
  [Symbol.iterator]<T = { [key: string]: any }>(): Iterator<T>;
};

const MSD_TABLE_V1_MAGIC = 0x4d7c0001;
const MSD_TABLE_V2_MAGIC = 0x4d7c0200;

function checkMsdTable(obj: any): obj is MsdTable {
  if (typeof obj !== "object" || obj === null) {
    return false;
  }
  if (obj.version !== MSD_TABLE_V1_MAGIC && obj.version !== MSD_TABLE_V2_MAGIC) {
    return false;
  }
  return true;
}

/**
 * Parse a JSON string representing an MSd table into an MsdTable object
 * with helper methods for accessing rows and columns.
 *
 * Raw MSd tables is columnar, the helpers allow easy row-wise access.
 * It also handles type conversions, e.g. Decimal64 to string. DateTime to Date.
 *
 * @example
 * const sample = '{"columns":[{"schema":{"name":"ts","kind":"DateTime","metadata":null},"data":{"DateTime":[1735689600000000,1735747200000000,1735833600000000]}},{"schema":{"name":"price","kind":"Decimal64","metadata":null},"data":{"Decimal64":[31520,5136,7696]}}],"metadata":null}';
 * const parsed = parseMsdTable(sample);
 * console.log(parsed.getRowsCount()); // 3
 * console.log(parsed.getColumnsCount()); // 2
 * // Access as iterable of rows
 * for (const row of parsed as Iterable<{ts: Date; price: string;}>) {
 *  console.log(row.ts, row.price);
 * }
 * // Access via cell method
 * for (let i = 0; i < parsed.getRowsCount(); i++) {
 *  console.log(parsed.cell<Date>(i, 0), parsed.cell<string>(i, 1));
 * }
 *
 * @param data JSON string representing the MSd table
 * @return MsdTable object with helper methods
 */
export function wrapMsdTable<const Cols extends readonly Field[]>(
  obj: MsdTable,
): MsdTable & MsdTableApi<SchemaFromFields<Cols>> {
  const apiObj = obj as any;

  apiObj.getRowsCount = function (): number {
    for (const col of this.columns) {
      if (col.kind !== "Null" && col.data && typeof col.data.length === "number") {
        return col.data.length;
      }
    }
    return 0;
  };

  apiObj.getColumnsCount = function (): number {
    return this.columns.length;
  };

  apiObj.getMetadata = function (key: string): any {
    const v = this.metadata?.[key];
    if (v === undefined) {
      return null;
    }
    if (typeof v === "object" && v !== null) {
      return Object.values(v)[0] as any;
    } else {
      return v;
    }
  };

  apiObj.cell = function <
    T = string | number | boolean | Uint8Array | Date | null,
  >(row: number, column: number): T {
    const col = this.columns[column];
    if (!col || col.kind === "Null" || !col.data) {
      return null as T;
    }
    switch (col.kind) {
      case "String":
        return (col.data[row] ?? null) as T;
      case "Bytes":
        return (col.data[row] ?? null) as T;
      case "Int32":
        return (col.data[row] ?? null) as T;
      case "UInt32":
        return (col.data[row] ?? null) as T;
      case "Int64":
        return (col.data[row] ?? null) as T;
      case "UInt64":
        return (col.data[row] ?? null) as T;
      case "Float32":
        return (col.data[row] ?? null) as T;
      case "Float64":
        return (col.data[row] ?? null) as T;
      case "Decimal64":
        return (col.data[row] ?? null) as T;
      case "Bool":
        return (col.data[row] ?? null) as T;
      case "DateTime":
        const ts = col.data[row];
        return (ts ? new Date(ts) : null) as T;
    }
    return null as T;
  };

  apiObj.column = function (column: number | string): any {
    if (typeof column === "string") {
      const col = this.columns.find((col: any) => col.name === column);
      if (!col) {
        return null;
      }
      column = this.columns.indexOf(col);
    }
    const col = this.columns[column];
    if (!col || col.kind === "Null" || !col.data) {
      return null;
    }
    return col.data;
  };

  apiObj.row = function <T = { [key: string]: any }>(row: number): T {
    const result: { [key: string]: any } = {};
    for (let colIndex = 0; colIndex < this.columns.length; colIndex++) {
      const col = this.columns[colIndex];
      // @ts-ignore
      result[col!.name] = this.cell(row, colIndex);
    }
    return result as T;
  };

  return apiObj;
}

/**
 * Parse a JSON string representing an MSd table into an MsdTable object
 * with helper methods for accessing rows and columns.
 *
 * Raw MSd tables is columnar, the helpers allow easy row-wise access.
 * It also handles type conversions, e.g. Decimal64 to string. DateTime to Date.
 *
 * @example
 * const sample = '{"columns":[{"schema":{"name":"ts","kind":"DateTime","metadata":null},"data":{"DateTime":[1735689600000000,1735747200000000,1735833600000000]}},{"schema":{"name":"price","kind":"Decimal64","metadata":null},"data":{"Decimal64":[31520,5136,7696]}}],"metadata":null}';
 * const parsed = parseMsdTable(sample);
 * console.log(parsed.getRowsCount()); // 3
 * console.log(parsed.getColumnsCount()); // 2
 * // Access as iterable of rows
 * for (const row of parsed as Iterable<{ts: Date; price: string;}>) {
 *  console.log(row.ts, row.price);
 * }
 * // Access via cell method
 * for (let i = 0; i < parsed.getRowsCount(); i++) {
 *  console.log(parsed.cell<Date>(i, 0), parsed.cell<string>(i, 1));
 * }
 *
 * @param data JSON string representing the MSd table
 * @return MsdTable object with helper methods
 */
export function parseMsdTable<const Cols extends readonly Field[]>(
  data: string,
): MsdTable & MsdTableApi<SchemaFromFields<Cols>> {
  const obj = JSON.parse(data);
  if (!checkMsdTable(obj)) {
    throw new Error("Invalid MsdTable");
  }
  for (const col of obj.columns) {
    if (col.data === null) {
      continue;
    }
    switch (col.kind) {
      case "Int32":
        col.data = new Int32Array(col.data);
        break;
      case "UInt32":
        col.data = new Uint32Array(col.data);
        break;
      case "Int64":
        col.data = new BigInt64Array(col.data.map((v: any) => BigInt(v)));
        break;
      case "UInt64":
        col.data = new BigUint64Array(col.data.map((v: any) => BigInt(v)));
        break;
      case "Float32":
        col.data = new Float32Array(col.data);
        break;
      case "Float64":
        col.data = new Float64Array(col.data);
        break;
      case "Decimal64":
        col.data = new Float64Array(col.data);
        break;
      case "DateTime":
        col.data = new Float64Array(col.data);
        break;
      case "Bytes":
        col.data = col.data.map((v: any) => new Uint8Array(v));
        break;
    }
  }
  return wrapMsdTable(obj) 
}
