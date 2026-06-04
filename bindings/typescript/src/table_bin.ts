/**
 * Copyright 2026 MSD-RS Project LiJia
 * SPDX-License-Identifier: agpl-3.0-only
 */

import type { MsdQueryOptions } from "./query";
import { type MsdTable, type MsdTableApi, wrapMsdTable, type SeriesTypes, type SeriesType } from "./table";

class BincodeReader {
  private view: DataView;
  private offset: number;

  constructor(view: DataView, offset: number = 0) {
    this.view = view;
    this.offset = offset;
  }

  // Read raw bytes
  readBytes(length: number): Uint8Array {
    if (this.offset + length > this.view.byteLength) {
      throw new Error("Out of bounds read");
    }
    const bytes = new Uint8Array(
      this.view.buffer,
      this.view.byteOffset + this.offset,
      length,
    );
    this.offset += length;
    return bytes;
  }

  // Read a boolean (1 byte)
  readBool(): boolean {
    const b = this.view.getUint8(this.offset);
    this.offset += 1;
    return b !== 0;
  }

  // Read u8 directly
  readU8(): number {
    const val = this.view.getUint8(this.offset);
    this.offset += 1;
    return val;
  }

  // Read i8 directly
  readI8(): number {
    const val = this.view.getInt8(this.offset);
    this.offset += 1;
    return val;
  }

  // Read VarInt unsigned integer (represented as bigint)
  readVarInt(): bigint | number {
    const tag = this.readU8();
    if (tag < 251) {
      return tag;
    } else if (tag === 251) {
      const val = this.view.getUint16(this.offset, true);
      this.offset += 2;
      return val;
    } else if (tag === 252) {
      const val = this.view.getUint32(this.offset, true);
      this.offset += 4;
      return val;
    } else if (tag === 253) {
      // const low = this.view.getUint32(this.offset, true);
      // const high = this.view.getUint32(this.offset + 4, true);
      const n = this.view.getBigUint64(this.offset, true); // for bounds check
      this.offset += 8;
      if (n > Number.MAX_SAFE_INTEGER) {
        return n;
      } else {
        return Number(n);
      }
    } else if (tag === 254) {
      const lowLow = BigInt(this.view.getUint32(this.offset, true));
      const lowHigh = BigInt(this.view.getUint32(this.offset + 4, true));
      const highLow = BigInt(this.view.getUint32(this.offset + 8, true));
      const highHigh = BigInt(this.view.getUint32(this.offset + 12, true));
      this.offset += 16;
      return (highHigh << 96n) | (highLow << 64n) | (lowHigh << 32n) | lowLow;
    } else {
      throw new Error(`Invalid varInt tag: ${tag}`);
    }
  }

  // Read VarInt signed integer
  readVarIntSigned(): bigint | number {
    const u = this.readVarInt();
    if (typeof u === "bigint") {
      return (u >> 1n) ^ -(u & 1n);
    } else {
      if (u % 2 === 0) {
        return u / 2;
      } else {
        return -(u + 1) / 2;
      }
    }
  }

  // Read standard types
  readU16(): number {
    return Number(this.readVarInt());
  }
  readI16(): number {
    return Number(this.readVarIntSigned());
  }
  readU32(): number {
    return Number(this.readVarInt());
  }
  readI32(): number {
    return Number(this.readVarIntSigned());
  }
  readU64(): bigint | number {
    return this.readVarInt();
  }
  readI64(): bigint | number {
    return this.readVarIntSigned();
  }

  // Read f32 (IEEE 754 float, little-endian)
  readF32(): number {
    const val = this.view.getFloat32(this.offset, true);
    this.offset += 4;
    return val;
  }

  // Read f64 (IEEE 754 double, little-endian)
  readF64(): number {
    const val = this.view.getFloat64(this.offset, true);
    this.offset += 8;
    return val;
  }

  // Read string: length-prefixed UTF-8
  readString(): string {
    const len = Number(this.readVarInt());
    const bytes = this.readBytes(len);
    const decoder = new TextDecoder();
    return decoder.decode(bytes);
  }

  // Read Option
  readOption<T>(readerFn: () => T): T | null {
    const hasValue = this.readBool();
    return hasValue ? readerFn() : null;
  }

  // Read list
  readList<T>(readerFn: () => T): T[] {
    const len = Number(this.readVarInt());
    const list: T[] = [];
    for (let i = 0; i < len; i++) {
      list.push(readerFn());
    }
    return list;
  }

  // Read byte sequence (Vec<u8>)
  readByteSeq(): number[] {
    const len = Number(this.readVarInt());
    const bytes = this.readBytes(len);
    return Array.from(bytes);
  }
}

const dec_factor = [
  1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000,
];
function d64FromI64(v: bigint | number, dec: number): number {
  // const vBig = BigInt(v);
  // const isNeg = vBig < 0n;
  // const n = isNeg ? -vBig : vBig;
  // const flag = (BigInt(dec) << 4n) | (isNeg ? 1n : 0n);
  // const u64Val = (n << 8n) | flag;
  // return Number(u64Val);

  if (dec < 0) {
    return Number(v);
  }
  if (dec < dec_factor.length) {
    return Number(v) / dec_factor[dec]!;
  } else {
    return Number(v) / Math.pow(10, dec);
  }
}

function parseDataType(reader: BincodeReader): string {
  const index = Number(reader.readVarInt());
  switch (index) {
    case 0:
      return "Null";
    case 1:
      return "DateTime";
    case 2:
      return "Int64";
    case 3:
      return "Float64";
    case 4:
      return "Decimal64";
    case 5:
      return "String";
    case 6:
      return "Bool";
    case 7:
      return "Int32";
    case 8:
      return "UInt32";
    case 9:
      return "UInt64";
    case 10:
      return "Float32";
    case 11:
      return "Bytes";
    case 12:
      throw new Error("Decimal128 is not supported");
    default:
      throw new Error(`Invalid DataType variant index: ${index}`);
  }
}

function parseMetadata(reader: BincodeReader): Record<string, any> {
  const len = Number(reader.readVarInt());
  const map: Record<string, any> = {};
  for (let i = 0; i < len; i++) {
    const k = reader.readString();
    const v = parseVariant(reader);
    map[k] = v;
  }
  return map;
}

function parseVariant(reader: BincodeReader): any {
  const index = Number(reader.readVarInt());
  switch (index) {
    case 0:
      return { Null: "Null" };
    case 1:
      return { DateTime: Number(reader.readI64()) };
    case 2:
      return { Int64: Number(reader.readI64()) };
    case 3:
      return { Float64: reader.readF64() };
    case 4:
      return { Decimal64: Number(reader.readU64()) };
    case 5:
      return { String: reader.readString() };
    case 6:
      return { Bool: reader.readBool() };
    case 7:
      return { Int32: reader.readI32() };
    case 8:
      return { UInt32: reader.readU32() };
    case 9:
      return { UInt64: Number(reader.readU64()) };
    case 10:
      return { Float32: reader.readF32() };
    case 11:
      return { Bytes: reader.readByteSeq() };
    case 12:
      throw new Error("Decimal128 is not supported");
    default:
      throw new Error(`Invalid Variant variant index: ${index}`);
  }
}

function newTypedArray<K extends keyof SeriesTypes>(type: K, len: number, options?: MsdQueryOptions): SeriesType<K> {
  const shared = options?.shared ?? false;
  const arrayOptions = options?.resizable ? {maxByteLength: 524288} : {};
  

  const newBuffer = (size: number) => {
    if (shared) {
      return new SharedArrayBuffer(size, arrayOptions);
    } else {
      return new ArrayBuffer(size, arrayOptions);
    }
  }

  let result: any;
  switch (type) {
    case "String":
      result = [];
      break;
    case "Bytes":
      result = [];
      break;
    case "Int32":
      result = new Int32Array(newBuffer(len * 4));
      break;
    case "UInt32":
      result = new Uint32Array(newBuffer(len * 4));
      break;
    case "Int64":
      result = new BigInt64Array(newBuffer(len * 8));
      break;
    case "UInt64":
      result = new BigUint64Array(newBuffer(len * 8));
      break;
    case "Float32":
      result = new Float32Array(newBuffer(len * 4));
      break;
    case "Float64":
      result = new Float64Array(newBuffer(len * 8));
      break;
    case "Decimal64":
      result = new Float64Array(newBuffer(len * 8));
      break;
    case "Bool":
      result = [];
      break;
    case "DateTime":
      result = new Float64Array(newBuffer(len * 8));
      break;
    case "Null":
      result = null;
      break;
  }
  return result;
}

function parseSeries(reader: BincodeReader, options?: MsdQueryOptions): any {
  const index = Number(reader.readVarInt());
  switch (index) {
    case 0: // Null
      return null;
    case 1: {
      // DateTime
      const len = Number(reader.readVarInt());
      if (len === 0) {
        return { kind: "DateTime", data: new Float64Array(0) };
      }
      const gcd = Number(reader.readI64()) / 1000;
      const diffs: number[] = [];
      for (let i = 0; i < len - 1; i++) {
        diffs.push(reader.readI64() as number);
      }
      const values = newTypedArray("DateTime", len - 1, options);
      if (diffs.length > 0) {
        values[0] = diffs[0]! * gcd;
        for (let i = 1; i < diffs.length; i++) {
          values[i] = values[i - 1]! + diffs[i]! * gcd;
        }
      }
      return { kind: "DateTime", data: values };
    }
    case 2: {
      // Int64
      const len = Number(reader.readVarInt());
      const values = newTypedArray("Int64", len, options);
      for (let i = 0; i < len; i++) {
        values[i] = BigInt(reader.readI64());
      }
      return { kind: "Int64", data: values };
    }
    case 3: {
      // Float64
      const len = Number(reader.readVarInt());
      const values = newTypedArray("Float64", len, options);
      for (let i = 0; i < len; i++) {
        values[i] = reader.readF64();
      }
      return { kind: "Float64", data: values };
    }
    case 4: {
      // Decimal64
      const len = Number(reader.readVarInt());
      if (len === 0) {
        return { kind: "Decimal64", data: new Float64Array(0) };
      }
      const dec_num = Number(reader.readI64());
      const diffs: number[] = [];
      for (let i = 0; i < len - 1; i++) {
        diffs.push(Number(reader.readI64()));
      }
      const values = newTypedArray("Decimal64", len - 1, options);
      if (diffs.length > 0) {
        let current = diffs[0]!;
        values[0] = d64FromI64(current, dec_num);
        for (let i = 1; i < diffs.length; i++) {
          current += diffs[i]!;
          values[i] = d64FromI64(current, dec_num);
        }
      }
      return { kind: "Decimal64", data: values };
    }
    case 5: {
      // String
      const values = reader.readList(() => reader.readString());
      return { kind: "String", data: values };
    }
    case 6: {
      // Bool
      const values = reader.readList(() => reader.readBool());
      return { kind: "Bool", data: values };
    }
    case 7: {
      // Int32
      const len = Number(reader.readVarInt());
      const values = newTypedArray("Int32", len, options);
      for (let i = 0; i < len; i++) {
        values[i] = reader.readI32();
      }
      return { kind: "Int32", data: values };
    }
    case 8: {
      // UInt32
      const len = Number(reader.readVarInt());
      const values = newTypedArray("UInt32", len, options);
      for (let i = 0; i < len; i++) {
        values[i] = reader.readU32();
      }
      return { kind: "UInt32", data: values };
    }
    case 9: {
      // UInt64
      const len = Number(reader.readVarInt());
      const values = newTypedArray("UInt64", len, options);
      for (let i = 0; i < len; i++) {
        values[i] = BigInt(reader.readU64());
      }
      return { kind: "UInt64", data: values };
    }
    case 10: {
      // Float32
      const len = Number(reader.readVarInt());
      const values = newTypedArray("Float32", len, options);
      for (let i = 0; i < len; i++) {
        values[i] = reader.readF32();
      }
      return { kind: "Float32", data: values };
    }
    case 11: {
      // Bytes
      const values = reader.readList(() => new Uint8Array(reader.readByteSeq()));
      return { kind: "Bytes", data: values };
    }
    case 12:
      throw new Error("Decimal128 is not supported");
    default:
      throw new Error(`Invalid Series variant index: ${index}`);
  }
}


function parseField(reader: BincodeReader, options?: MsdQueryOptions): any {
  const name = reader.readString();
  const kind = parseDataType(reader);
  const metadata = reader.readOption(() => parseMetadata(reader));
  const seriesInfo = parseSeries(reader, options);

  return {
    name,
    kind,
    metadata,
    data: seriesInfo === null ? null : seriesInfo.data,
  };
}

export function parseTableBin(
  view: DataView,
  offset: number = 0,
  options?: MsdQueryOptions
): MsdTable & MsdTableApi {
  if (view.byteLength - offset >= 8) {
    const m0 = view.getUint8(offset);
    const m1 = view.getUint8(offset + 1);
    const v0 = view.getUint8(offset + 2);
    const v1 = view.getUint8(offset + 3);
    if (m0 === 0x7c && m1 === 0x4d && v0 === 0x01 && v1 === 0x00) {
      offset += 8;
    }
  }
  const reader = new BincodeReader(view, offset);
  const version = reader.readU32();
  if (version !== 1299972097) {
    throw new Error(`Unsupported table binary version: ${version}`);
  }
  const columns = reader.readList(() => parseField(reader, options));
  const metadata = reader.readOption(() => parseMetadata(reader));
  const is_kv = reader.readBool();

  const tableObj: MsdTable = {
    // @ts-ignore
    version,
    columns,
    metadata,
    is_kv,
  };

  return wrapMsdTable(tableObj);
}
