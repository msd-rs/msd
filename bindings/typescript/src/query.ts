/**
 * Copyright 2026 MSD-RS Project LiJia
 * SPDX-License-Identifier: agpl-3.0-only
 */

import type { MsdTable, MsdTableApi } from "./table";
import { parseMsdTable } from "./table";
import { parseTableBin } from "./table_bin";
import { parseTableBinV2 } from "./table_bin_v2";

/**
 * Query options for msdQuery function.
 */
export type MsdQueryOptions = {
  /**
   * The base URL of the MSD-RS server, e.g., "http://localhost:50510"
   */
  baseURL: string;
  /**
   * Custom fetch function, defaults to global fetch
   */
  fetch?: typeof fetch;
  /**
   * Whether to use binary format for the response
   */
  binary?: number;

  /**
   * use shared array buffer for table column data.
   */
  shared?: boolean;

  /**
   * does column data can be resize
   */
  resizable?: boolean;

  /**
   * when set, the `baseURL` is a path to the static file. each number is the format of file
   * - 0: json
   * - 1: bin v1
   * - 2: bin v2
   */
  staticFile?: number;
};

export type MsdQueryResponse = {
  [obj: string]: { [table: string]: MsdTableApi & MsdTable };
};


async function mockMsdQuery(options: MsdQueryOptions): Promise<MsdQueryResponse> {
  const { baseURL, fetch = globalThis.fetch} = options;

  const response = await fetch(baseURL)


  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }
  if (!response.body) {
    throw new Error("Response body is null");
  }

  const reader = response.body!.getReader() as unknown as ReadableStreamDefaultReader<Uint8Array>;

  if (options.staticFile! > 0) {
    const result = await parseBinaryResponse(reader, options);
    return result;
  } else {
    const result = await parseTextResponse(reader);
    return result;
  }

}

export async function msdQuery(
  query: string,
  options: MsdQueryOptions,
): Promise<MsdQueryResponse> {

  // do mock query
  if (typeof options.staticFile === 'number' && options.staticFile >= 0 && options.staticFile < 3) {
    return mockMsdQuery(options);
  }

  const { baseURL, fetch = globalThis.fetch, binary = false } = options;
  const url = `${baseURL}/query`;
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };
  if (binary) {
    headers["x-msd-client"] = binary.toString();
  }

  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify({ query }),
  });


  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }
  if (!response.body) {
    throw new Error("Response body is null");
  }

  const reader = response.body!.getReader() as unknown as ReadableStreamDefaultReader<Uint8Array>;


  const contentType = response.headers.get("Content-Type") || "";


  if (contentType == "application/x-msd-table-frame" || contentType == "application/x-msd-table-frame-v2") {
    const result = await parseBinaryResponse(reader, options);
    return result;
  } else {
    const result = await parseTextResponse(reader);
    return result;
  }
}

async function parseBinaryResponse(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  options: MsdQueryOptions,
): Promise<MsdQueryResponse> {
  const result: MsdQueryResponse = {};
  const buffer = new ArrayBuffer(0, { maxByteLength: 1024 * 1024 * 4 });

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    appendBuffer(buffer, value);

    let totalReadBytes = 0;
    while (true) {
      let b = new DataView(buffer, totalReadBytes);
      const { table, readBytes } = tryReadTable(b, options);
      if (table) {
        const obj = table.getMetadata("obj");
        const kind = table.getMetadata("table")
        if (typeof obj === "string" && typeof kind === "string") {
          if (result[obj]) {
            result[obj][kind] = table;
          } else {
            result[obj] = { [kind]: table };
          }
        }
        totalReadBytes += readBytes;
      } else {
        const v = new Uint8Array(buffer);
        const remaining = v.subarray(totalReadBytes);
        const remainingLen = remaining.length;
        v.set(v.subarray(totalReadBytes));
        buffer.resize(remainingLen);
        break;
      }
    }
  }
  return result;
}

function appendBuffer(buffer: ArrayBuffer, data: Uint8Array): ArrayBuffer {
  if (buffer.resizable) {
    const currentLength = buffer.byteLength;
    buffer.resize(currentLength + data.length);
    const view = new Uint8Array(buffer);
    view.set(data, currentLength);
    return buffer;
  } else {
    throw new Error("Buffer is not resizable");
  }
}

function tryReadTable(view: DataView, options: MsdQueryOptions): {
  table: (MsdTableApi & MsdTable) | null;
  readBytes: number;
} {
  if (view.byteLength < 8) {
    // no enough data to read header
    return { table: null, readBytes: 0 };
  }
  const magic = view.getUint32(0, true);

  if (magic == 0x00014d7c) {
    const tableLength = view.getUint32(4, true);
    if (view.byteLength < 8 + tableLength) {
      // no enough data to read header
      return { table: null, readBytes: 0 };
    }
    return { table: parseTableBin(view, 8, options), readBytes: 8 + tableLength };
  }
  else if (magic == 0x02004d7c) {
    const tableLength = view.getUint32(4, true);
    if (view.byteLength < 8 + tableLength) {
      // no enough data to read header
      return { table: null, readBytes: 0 };
    }
    return { table: parseTableBinV2(view, 8, options), readBytes: 8 + tableLength };
  }else{
    throw new Error("Invalid magic number in binary response");
  }
}

async function parseTextResponse(
  reader: ReadableStreamDefaultReader<Uint8Array>,
): Promise<MsdQueryResponse> {
  const decoder = new TextDecoder();
  let buffer = "";
  const result: MsdQueryResponse = {};

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      // The last element is possibly an incomplete line, so keep it in the buffer
      buffer = lines.pop() || "";

      for (const line of lines) {
        if (!line.trim()) continue;
        const table = parseMsdTable(line);
        const obj = table.getMetadata("obj");
        const kind = table.getMetadata("table");
        if (typeof obj === "string" && typeof kind === 'string') {
          if (result[obj]) {
            result[obj][kind] = table;
          } else {
            result[obj] = { [kind]: table };
          }
        }
      }
    }
  } finally {
    reader.releaseLock();
  }

  // Process any remaining buffer content
  buffer += decoder.decode();
  if (buffer.trim()) {
    const table = parseMsdTable(buffer);
    const obj = table.getMetadata("obj");
    const kind = table.getMetadata("table");
    if (typeof obj === "string" && typeof kind === 'string') {
      if (result[obj]) {
        result[obj][kind] = table;
      } else {
        result[obj] = { [kind]: table };
      }
    }
  }

  return result;
}
