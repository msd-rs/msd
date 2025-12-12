import type { MsdTable, MsdTableApi} from "./table";
import { parseMsdTable } from "./table";



export type MsdQueryOptions = {
  baseURL: string;
  fetch?: typeof fetch;
}

export type MsdQueryResponse = {
  [key: string]: MsdTableApi & MsdTable;  
}

export async function msdQuery(query: string, options: MsdQueryOptions): Promise<MsdQueryResponse> {
  const { baseURL, fetch = globalThis.fetch } = options;
  const url = `${baseURL}/data`;
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
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
        // Use cell(0, 0) as the key. We expect it to be a string.
        const key = table.cell(0, 0);
        if (typeof key === "string") {
          result[key] = table;
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
    const key = table.cell(0, 0);
    if (typeof key === "string") {
      result[key] = table;
    }
  }

  return result;
}