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
  const buffer = new ArrayBuffer(0, { maxByteLength: 1024 * 1024 });

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    appendBuffer(buffer, value);

    let totalReadBytes = 0;
    while (true) {
      let b = new DataView(buffer, totalReadBytes);
      const { table, readBytes } = tryReadTable(b);
      if (table) {
        const obj = table.getMetadata("obj");
        if (typeof obj === "string") {
          result[obj] = table;
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

function tryReadTable(view: DataView): {
  table: (MsdTableApi & MsdTable) | null;
  readBytes: number;
} {
  if (view.byteLength < 8) {
    // no enough data to read header
    return { table: null, readBytes: 0 };
  }
  const magic = view.getUint32(0, true);
  if (magic !== 0x14d7c) {
    throw new Error("Invalid magic number in binary response");
  }
  const tableLength = view.getUint32(4, true);
  if (view.byteLength < 8 + tableLength) {
    // no enough data to read header
    return { table: null, readBytes: 0 };
  }
  return { table: parseTableBin(view, 8), readBytes: 8 + tableLength };
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
