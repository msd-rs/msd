/**
 * Copyright 2026 MSD-RS Project LiJia
 * SPDX-License-Identifier: agpl-3.0-only
 */

import type { MsdTable, MsdTableApi } from "./table";
import { parseMsdTable } from "./table";
import { parseTableBin } from "./table_bin";

/**
 * Query options for msdQuery function.
 */
export type MsdQueryOptions = {
  /// The base URL of the MSD-RS server, e.g., "http://localhost:50510"
  baseURL: string;
  /// Custom fetch function, defaults to global fetch
  fetch?: typeof fetch;
  /// Whether to use binary format for the response
  binary?: boolean;
};

export type MsdQueryResponse = {
  [key: string]: MsdTableApi & MsdTable;
};

export async function msdQuery(
  query: string,
  options: MsdQueryOptions,
): Promise<MsdQueryResponse> {
  const { baseURL, fetch = globalThis.fetch, binary = false } = options;
  const url = `${baseURL}/query`;
  const userAgent = binary ? "msd-client" : navigator.userAgent;
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "User-Agent": userAgent,
    },
    body: JSON.stringify({ query }),
  });

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }
  if (!response.body) {
    throw new Error("Response body is null");
  }

  const reader = response.body!.getReader();

  const isBinaryResponse =
    response.headers.get("Content-Type") === "application/x-msd-table-frame";

  if (isBinaryResponse) {
    return await parseBinaryResponse(reader);
  } else {
    return await parseTextResponse(reader);
  }
}

async function parseBinaryResponse(
  reader: ReadableStreamDefaultReader<Uint8Array>,
): Promise<MsdQueryResponse> {
  const result: MsdQueryResponse = {};
  const buffer = new ArrayBuffer(0, { maxByteLength: 1024 * 1024 * 10 });

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    appendBuffer(buffer, value);

    while (true) {
      let totalReadBytes = 0;
      let b = buffer.slice(0);
      const { table, readBytes } = tryReadTable(b);
      if (table) {
        const obj = table.getMetadata("obj");
        if (typeof obj === "string") {
          result[obj] = table;
        }
        b = buffer.slice(readBytes);
        totalReadBytes += readBytes;
      } else {
        const v = new Uint8Array(buffer);
        const remaining = v.slice(totalReadBytes);
        const remainingLen = remaining.length;
        // Move the remaining data to the beginning of the buffer
        const view = new Uint8Array(buffer);
        view.set(remaining, 0);
        // Update the buffer length to reflect the remaining data
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

function tryReadTable(buffer: ArrayBuffer): {
  table: (MsdTableApi & MsdTable) | null;
  readBytes: number;
} {
  if (buffer.byteLength < 8) {
    // no enough data to read header
    return { table: null, readBytes: 0 };
  }
  const dataView = new DataView(buffer);
  const magic = dataView.getUint32(0, true);
  if (magic !== 0x4d7c00001) {
    throw new Error("Invalid magic number in binary response");
  }
  const tableLength = dataView.getUint32(4, true);
  if (buffer.byteLength < 8 + tableLength) {
    // no enough data to read header
    return { table: null, readBytes: 0 };
  }
  const tableBuffer = buffer.slice(8, 8 + tableLength - 4); // exclude the 4 bytes of CRC
  return { table: parseTableBin(tableBuffer), readBytes: 8 + tableLength };
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
        if (typeof obj === "string") {
          result[obj] = table;
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
    if (typeof obj === "string") {
      result[obj] = table;
    }
  }

  return result;
}
