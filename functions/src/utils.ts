import * as functions from "firebase-functions";
import { ParamsSpec } from "./build_bundle";

/**
 * Return query parameters that are specified in given `ParamsSpec`.
 */
export function filterQuery(
  qs: { [key: string]: any },
  params: ParamsSpec
): { [key: string]: any } {
  const out: { [key: string]: any } = {};
  for (const k in qs) {
    if (params[k]) out[k] = qs[k];
  }
  return out;
}

/**
 * Joins all query parameters and values, and sort them into one string.
 */
export function sortQuery(qs: { [key: string]: any }): string {
  const arr: string[] = [];
  for (const k in qs) {
    arr.push([k, qs[k].toString()].join("="));
  }
  return arr.sort().join("&");
}

/**
 * Creates a valid storage path for bundle files, with proper character encoding
 * to avoid issues with special characters like question marks.
 *
 * Note: The query parameters should already be filtered by filterQuery to include
 * only relevant parameters defined in the bundle spec.
 */
export function createStoragePath(
  bundleId: string,
  query: { [k: string]: any },
  storagePrefix: string
): string {
  // Sanitize bundle ID to avoid path issues
  const sanitizedBundleId = bundleId.replace(/[^a-zA-Z0-9_-]/g, "_");

  // Create a unique hash for query parameters instead of including them in the path
  const queryString = sortQuery(query);
  const queryHash = createHash(queryString);

  return `${storagePrefix}/${sanitizedBundleId}_${queryHash}`;
}

/**
 * Creates a simple hash from a string - used to create a deterministic
 * but safe filename from query parameters
 */
export function createHash(str: string): string {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash = hash & hash; // Convert to 32bit integer
  }
  // Convert to a positive hex string and take the first 8 characters
  return Math.abs(hash).toString(16).substring(0, 8);
}

/**
 * Sets appropriate cache control headers based on bundle specification
 */
export function setCacheControlHeaders(
  res: functions.Response,
  serverCache?: number,
  clientCache?: number
): void {
  if (serverCache || clientCache) {
    const maxAgeString = clientCache ? `, max-age=${clientCache}` : "";
    const sMaxAgeString = serverCache ? `, s-maxage=${serverCache}` : "";
    res.set("cache-control", `public${maxAgeString}${sMaxAgeString}`);
  } else {
    // Default to no-cache if not specified
    res.set("cache-control", "no-cache");
  }
}

/**
 * Clones a readable stream by consuming it and creating a new one
 * with the same content. This is needed when we want to pipe a stream
 * to multiple destinations.
 */
export async function cloneStream(
  stream: NodeJS.ReadableStream
): Promise<Buffer> {
  return new Promise<Buffer>((resolve, reject) => {
    const chunks: Buffer[] = [];

    stream.on("data", (chunk) => {
      chunks.push(Buffer.from(chunk));
    });

    stream.on("end", () => {
      resolve(Buffer.concat(chunks));
    });

    stream.on("error", (err) => {
      reject(err);
    });
  });
}
