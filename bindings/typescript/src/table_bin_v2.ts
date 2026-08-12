/**
 * Copyright 2026 MSD-RS Project LiJia
 * SPDX-License-Identifier: agpl-3.0-only
 */

import type { MsdQueryOptions } from "./query";
import { type MsdTable, type MsdTableApi, wrapMsdTable, type SeriesTypes, type Field, type SchemaFromFields } from "./table";

export function parseTableBinV2<C extends readonly Field[] = Field[]>(
  view: DataView,
  offset: number = 0,
  options?: MsdQueryOptions
): MsdTable & MsdTableApi<SchemaFromFields<C>> {
  let currentOffset = offset;

  if (view.byteLength - currentOffset >= 8) {
    const magic = view.getUint16(currentOffset, true);
    const version = view.getUint16(currentOffset + 2, true);
    if (magic === 0x4d7c && version === 0x0200) {
      currentOffset += 8;
    }
  }

  const textDecoder = new TextDecoder();

  function readU8(): number {
    const val = view.getUint8(currentOffset);
    currentOffset += 1;
    return val;
  }

  function readU32(): number {
    const val = view.getUint32(currentOffset, true);
    currentOffset += 4;
    return val;
  }

  function readString(): string {
    const len = readU32();
    if (len === 0) {
      return "";
    }
    const bytes = new Uint8Array(view.buffer, view.byteOffset + currentOffset, len);
    currentOffset += len;
    return textDecoder.decode(bytes);
  }

  function readBytes(): Uint8Array {
    const len = readU32();
    if (len === 0) {
      return new Uint8Array(0);
    }
    const bytes = new Uint8Array(view.buffer, view.byteOffset + currentOffset, len);
    currentOffset += len;
    return bytes.slice();
  }

  function newArrayBuffer(begin: number, end: number) {
    const { shared = false, resizable = false } = options ?? {};
    const len = end - begin;
    const maxByteLength = resizable ? 524288 : len;
    let ab: ArrayBuffer | SharedArrayBuffer;
    if (shared) {
      ab = new SharedArrayBuffer(len, { maxByteLength });
    } else {
      ab = new ArrayBuffer(len, { maxByteLength });
    }
    const dst = new Uint8Array(ab);
    dst.set(new Uint8Array(view.buffer, begin, len));
    return ab;
  }


  function readTypedArray(kind: keyof SeriesTypes, rows: number) {
    const offset = currentOffset;
    switch (kind) {
      case "DateTime": {
        currentOffset += rows * Float64Array.BYTES_PER_ELEMENT;
        return new Float64Array(newArrayBuffer(offset, currentOffset));
      }
      case "Int64": {
        currentOffset += rows * BigInt64Array.BYTES_PER_ELEMENT;
        return new BigInt64Array(newArrayBuffer(offset, currentOffset));
      }
      case "Float64": {
        currentOffset += rows * Float64Array.BYTES_PER_ELEMENT;
        return new Float64Array(newArrayBuffer(offset, currentOffset));
      }
      case "Decimal64": {
        currentOffset += rows * Float64Array.BYTES_PER_ELEMENT;
        return new Float64Array(newArrayBuffer(offset, currentOffset));
      }
      case "Bool": {
        currentOffset += rows * Uint8Array.BYTES_PER_ELEMENT;
        const arr = new Uint8Array(newArrayBuffer(offset, currentOffset));
        return Array.from(arr).map((b) => b !== 0);
      }
      case "Int32": {
        currentOffset += rows * Int32Array.BYTES_PER_ELEMENT;
        return new Int32Array(newArrayBuffer(offset, currentOffset));
      }
      case "UInt32": {
        currentOffset += rows * Uint32Array.BYTES_PER_ELEMENT;
        return new Uint32Array(newArrayBuffer(offset, currentOffset));
      }
      case "UInt64": {
        currentOffset += rows * BigUint64Array.BYTES_PER_ELEMENT;
        return new BigUint64Array(newArrayBuffer(offset, currentOffset));
      }
      case "Float32": {
        currentOffset += rows * 4;
        return new Float32Array(newArrayBuffer(offset, currentOffset));
      }
      default:
        throw new Error(`Invalid DataType variant byte: ${kind}`);
    }
  }

  const obj = readString();
  const table = readString();
  const cols = readU32();
  const rows = readU32();

  const metadata: Record<string, any> = {};
  if (obj) {
    metadata["obj"] = obj;
  }
  if (table) {
    metadata["table"] = table;
  }

  const columns: Field[] = [];

  for (let i = 0; i < cols; i++) {
    const name = readString();
    const kindByte = readU8();
    let kind: keyof SeriesTypes;
    let data: any;

    switch (kindByte) {
      case 0: {
        kind = "Null";
        data = null;
        break;
      }
      case 1: {
        kind = "DateTime";
        data = readTypedArray("DateTime", rows);
        break;
      }
      case 2: {
        kind = "Int64";
        data = readTypedArray("Int64", rows);
        break;
      }
      case 3: {
        kind = "Float64";
        data = readTypedArray("Float64", rows);
        break;
      }
      case 4: {
        kind = "Decimal64";
        data = readTypedArray("Decimal64", rows);
        break;
      }
      case 5: {
        kind = "String";
        const arr: string[] = new Array(rows);
        for (let r = 0; r < rows; r++) {
          arr[r] = readString();
        }
        data = arr;
        break;
      }
      case 6: {
        kind = "Bool";
        data = readTypedArray("Bool", rows);
        break;
      }
      case 7: {
        kind = "Int32";
        data = readTypedArray("Int32", rows);
        break;
      }
      case 8: {
        kind = "UInt32";
        data = readTypedArray("UInt32", rows);
        break;
      }
      case 9: {
        kind = "UInt64";
        data = readTypedArray("UInt64", rows);
        break;
      }
      case 10: {
        kind = "Float32";
        data = readTypedArray("Float32", rows);
        break;
      }
      case 11: {
        kind = "Bytes";
        const arr: Uint8Array[] = new Array(rows);
        for (let r = 0; r < rows; r++) {
          arr[r] = readBytes();
        }
        data = arr;
        break;
      }
      default:
        throw new Error(`Invalid DataType variant byte: ${kindByte}`);
    }

    columns.push({
      name,
      kind: kind as any,
      metadata: null,
      data,
    });
  }

  const tableObj: MsdTable = {
    columns,
    metadata: Object.keys(metadata).length > 0 ? metadata : null,
  };

  return wrapMsdTable(tableObj) as unknown as MsdTable & MsdTableApi<SchemaFromFields<C>>;
}