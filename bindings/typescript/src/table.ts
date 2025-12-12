import { d64ToFloat } from "./d64";

type SeriesTypes = {
  String: { String: string[] };
  Bytes: { Bytes: number[][] };
  Int32: { Int32: number[] };
  UInt32: { UInt32: number[] };
  Int64: { Int64: number[] };
  UInt64: { UInt64: number[] };
  Float32: { Float32: number[] };
  Float64: { Float64: number[] };
  Decimal64: { Decimal64: number[] };
  Decimal128: { Decimal128: number[] };
  Bool: { Bool: boolean[] };
  DateTime: { DateTime: number[] };
  Null: "Null";
};

// Discriminated union: for each kind in `SeriesTypes` we map its corresponding `data` type.
// Each column stored in the serialized MSd table has a `schema` describing the
// column, and `data` whose type depends on the `kind` in the schema.  We model
// that relationship as a discriminated union keyed by `schema.kind`.

// Each `Field` is a discriminated union which pairs a specific `kind` with
// the corresponding `data` object. Use a mapped type to build that union so
// TypeScript understands the relation between `kind` and `data`.
type Field = {
  name: string;
  metadata: Record<string, number | string | boolean> | null;
} & {
  [K in keyof SeriesTypes]: { kind: K; data: SeriesTypes[K] };
}[keyof SeriesTypes];

export type MsdTable = {
  columns: Field[];
  metadata: Record<string, number | string | boolean> | null;
};

type CellType = string | number | boolean | Uint8Array | Date | null;

export type MsdTableApi = {
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
  cell<T = CellType>(row: number, column: number): T;
  /**
   * Get the values of all cells in a specific row
   * @param row The row index
   * @returns An object containing the values of all cells in the row
   */
  row<T = { [key: string]: CellType }>(row: number): T;

  /**
   * Iterate over all rows in the table
   * @returns An iterator over all rows in the table
   */
  [Symbol.iterator]<T = { [key: string]: CellType }>(): Iterator<T>;
};

const MSD_TABLE_V1_MAGIC = 0x4d7c0001;

function checkMsdTable(obj: any): obj is MsdTable {
  if (typeof obj !== "object" || obj === null) {
    return false;
  }
  if (obj.version !== MSD_TABLE_V1_MAGIC) {
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
export function parseMsdTable(data: string): MsdTable & MsdTableApi {
  const obj = JSON.parse(data) as MsdTable & MsdTableApi;
  if (!checkMsdTable(obj)) {
    throw new Error("Invalid MsdTable");
  }

  obj.getRowsCount = function (): number {
    for (const col of this.columns) {
      if (col.kind !== "Null") {
        const values = Object.values(col.data) as any[];
        if (values.length > 0) {
          return (values[0] as any[]).length;
        }
      }
    }
    return 0;
  };

  obj.getColumnsCount = function (): number {
    return this.columns.length;
  };

  obj.cell = function <
    T = string | number | boolean | Uint8Array | Date | null
  >(row: number, column: number): T {
    const col = this.columns[column];
    if (!col || col.kind === "Null") {
      return null as T;
    }
    switch (col.kind) {
      case "String":
        return ((col.data.String[row] as string) ?? null) as T;
      case "Bytes":
        return new Uint8Array(col.data.Bytes[row] ?? []) as T;
      case "Int32":
        return ((col.data.Int32[row] as number) ?? null) as T;
      case "UInt32":
        return ((col.data.UInt32[row] as number) ?? null) as T;
      case "Int64":
        return ((col.data.Int64[row] as number) ?? null) as T;
      case "UInt64":
        return ((col.data.UInt64[row] as number) ?? null) as T;
      case "Float32":
        return ((col.data.Float32[row] as number) ?? null) as T;
      case "Float64":
        return ((col.data.Float64[row] as number) ?? null) as T;
      case "Decimal64":
        return (
          col.data.Decimal64[row] ? d64ToFloat(col.data.Decimal64[row]) : null
        ) as T;
      case "Decimal128":
        return ((col.data.Decimal128[row] as number) ?? null) as T;
      case "Bool":
        return ((col.data.Bool[row] as boolean) ?? null) as T;
      case "DateTime":
        return (
          col.data.DateTime[row]
            ? new Date(col.data.DateTime[row] / 1000)
            : null
        ) as T;
    }
  };

  obj.row = function <T = { [key: string]: CellType }>(row: number): T {
    const result: { [key: string]: CellType } = {};
    for (let colIndex = 0; colIndex < this.columns.length; colIndex++) {
      const col = this.columns[colIndex];
      // @ts-ignore
      result[col!.name] = this.cell(row, colIndex);
    }
    return result as T;
  };

  obj[Symbol.iterator] = function* <
    T = { [key: string]: CellType }
  >(): Iterator<T> {
    const rowsCount = this.getRowsCount();
    for (let rowIndex = 0; rowIndex < rowsCount; rowIndex++) {
      yield this.row(rowIndex);
    }
  };

  return obj as MsdTable & MsdTableApi;
}
