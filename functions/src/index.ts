/**
 * This extension sets up a http-serving Cloud Function.
 *
 * The function reads all bundle specifications under a configured Firestore
 * collection, and serves requests for bundle files specified by the
 * specifications.
 */

import * as admin from "firebase-admin";
import * as functions from "firebase-functions";
import { Timestamp } from "@google-cloud/firestore";
import { BundleSpec, build, ParamsSpec } from "./build_bundle";
import { Storage } from "@google-cloud/storage";
import { createGzip } from "zlib";
const { Readable } = require("stream");

// Configuration constants
const BUNDLESPEC_COLLECTION = process.env.BUNDLESPEC_COLLECTION || "bundles";
const BUNDLE_STORAGE_BUCKET =
  process.env.BUNDLE_STORAGE_BUCKET || "bundle-builder-files";
const STORAGE_PREFIX = process.env.STORAGE_PREFIX || "bundles";

// Initialize the Firebase Admin SDK
admin.initializeApp();
const db = admin.firestore();

// Initialize Storage with proper error handling
const storage = new Storage();
const bucket = storage.bucket(BUNDLE_STORAGE_BUCKET);

// Add a startup check to ensure the bucket exists and is accessible
(async () => {
  try {
    const [exists] = await bucket.exists();
    if (!exists) {
      functions.logger.error(`Bucket ${BUNDLE_STORAGE_BUCKET} does not exist!`);
    } else {
      functions.logger.info(
        `Successfully connected to bucket: ${BUNDLE_STORAGE_BUCKET}`
      );
    }
  } catch (error) {
    functions.logger.error(
      `Error accessing bucket ${BUNDLE_STORAGE_BUCKET}:`,
      error
    );
  }
})();

const _waits: (() => void)[] = [];
// Cached Bundle specifications, read from Firestore.
const _specs: { [name: string]: BundleSpec } = {};
let _specsReady = false;

function specsReady() {
  _specsReady = true;
  while (_waits && _waits.length > 0) {
    // tslint:disable-next-line:no-empty
    (_waits.pop() || (() => {}))();
  }
}

// Returns a BundleSpec for the given bundle ID/name.
function spec(name: string): Promise<BundleSpec | null> {
  if (_specsReady) {
    return Promise.resolve(_specs[name] || null);
  }

  return new Promise((resolve) => {
    _waits.push(() => {
      resolve(_specs[name] || null);
    });
  });
}

// Return query parameters that are specified in given `ParamsSpec`.
function filterQuery(
  qs: { [key: string]: any },
  params: ParamsSpec
): { [key: string]: any } {
  const out: { [key: string]: any } = {};
  for (const k in qs) {
    if (params[k]) out[k] = qs[k];
  }
  return out;
}

// Joins all query parameters and values, and sort them into one string.
function sortQuery(qs: { [key: string]: any }): string {
  const arr: string[] = [];
  for (const k in qs) {
    arr.push([k, qs[k].toString()].join("="));
  }
  return arr.sort().join("&");
}

/**
 * Creates a valid storage path for bundle files, with proper character encoding
 * to avoid issues with special characters like question marks.
 */
function createStoragePath(
  bundleId: string,
  query: { [k: string]: any }
): string {
  // Sanitize bundle ID to avoid path issues
  const sanitizedBundleId = bundleId.replace(/[^a-zA-Z0-9_-]/g, "_");

  // Create a unique hash for query parameters instead of including them in the path
  const queryString = sortQuery(query);
  const queryHash = createHash(queryString);

  return `${STORAGE_PREFIX}/${sanitizedBundleId}_${queryHash}`;
}

/**
 * Creates a simple hash from a string - used to create a deterministic
 * but safe filename from query parameters
 */
function createHash(str: string): string {
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
 * Checks if a valid bundle file exists in Cloud Storage and returns
 * a readable stream if found. Returns null if not found or if the file
 * is expired according to the TTL.
 */
async function getFileCacheStream(
  bundleId: string,
  query: { [k: string]: any },
  options: {
    ttlSec: number;
    gzip?: boolean;
  }
): Promise<NodeJS.ReadableStream | null> {
  const path = createStoragePath(bundleId, query);
  const file = bucket.file(path);

  try {
    // Check if file exists
    const [exists] = await file.exists();
    if (!exists) {
      functions.logger.debug(`File does not exist: ${path}`);
      return null;
    }

    // Check file metadata for age
    const [metadata] = await file.getMetadata();
    const createTime = new Date(metadata.timeCreated).getTime();
    const now = Date.now();
    const ageInSeconds = (now - createTime) / 1000;

    // Check if file has expired based on TTL
    if (ageInSeconds > options.ttlSec) {
      functions.logger.debug(
        `File exists but has expired (age: ${ageInSeconds}s): ${path}`
      );
      return null;
    }

    functions.logger.debug(`Found valid cached file: ${path}`);
    return file.createReadStream({ decompress: !options.gzip });
  } catch (e) {
    functions.logger.error("Error checking/reading cached file:", e);
    return null;
  }
}

/**
 * Saves a bundle to Cloud Storage with proper error handling
 */
async function saveBundleToStorage(
  bundleId: string,
  query: { [k: string]: any },
  stream: NodeJS.ReadableStream
): Promise<void> {
  return new Promise((resolve, reject) => {
    const path = createStoragePath(bundleId, query);
    functions.logger.debug(`Saving bundle to: ${path}`);

    const gzip = createGzip({ level: 9 });
    const compressedStream = stream.pipe(gzip);

    const writeStream = bucket.file(path).createWriteStream({
      metadata: {
        contentEncoding: "gzip",
        contentType: "application/octet-stream",
      },
      resumable: false, // For smaller files, non-resumable uploads may be faster
    });

    // Handle events for proper error reporting and cleanup
    writeStream.on("error", (err) => {
      functions.logger.error(`Error saving bundle to storage: ${err}`);
      reject(err);
    });

    writeStream.on("finish", () => {
      functions.logger.debug(`Successfully saved bundle to storage: ${path}`);
      resolve();
    });

    compressedStream.pipe(writeStream);
  });
}

/**
 * Sets appropriate cache control headers based on bundle specification
 */
function setCacheControlHeaders(
  res: functions.Response,
  bundleSpec: BundleSpec
): void {
  if (bundleSpec.serverCache || bundleSpec.clientCache) {
    const maxAgeString = bundleSpec.clientCache
      ? `, max-age=${bundleSpec.clientCache}`
      : "";
    const sMaxAgeString = bundleSpec.serverCache
      ? `, s-maxage=${bundleSpec.serverCache}`
      : "";
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
async function cloneStream(stream: NodeJS.ReadableStream): Promise<Buffer> {
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

// Starts listening to the bundle specification collection, and update in memory cache when there
// are new snapshots.
db.collection(BUNDLESPEC_COLLECTION).onSnapshot((snap) => {
  snap.docs.forEach((doc) => {
    _specs[doc.id] = doc.data();
  });
  specsReady();
});

/**
 * Cloud Function to serve bundle building http requests.
 *
 * The last path segment of the http request will be the bundle ID to look for, and the
 * request query parameters will be the values to use to parameterize the bundle
 * specification.
 *
 * If the bundle specification has `fileCache` enabled, it would first check if
 * there is a valid bundle file saved in GCS, and return that if yes. It would
 * save the built bundle file GCS, if a valid bundle file could not be found.
 */
export const serve = functions.handler.https.onRequest(
  async (req, res): Promise<any> => {
    try {
      // Set CORS headers for browser compatibility
      res.set("Access-Control-Allow-Origin", "*");
      if (req.method === "OPTIONS") {
        res.set("Access-Control-Allow-Methods", "GET");
        res.set("Access-Control-Allow-Headers", "Content-Type");
        res.set("Access-Control-Max-Age", "3600");
        res.status(204).send("");
        return;
      }

      functions.logger.debug("Request headers:", req.headers);
      const canGzip = req.get("accept-encoding")?.includes("gzip") || false;
      if (canGzip) {
        res.set("content-encoding", "gzip");
      }

      // Parse bundle ID from path
      const path = req.path;
      const parts = path.split("/").filter((p) => p.length > 0);
      if (parts.length === 0) {
        res
          .status(400)
          .send("Invalid request path. Expected format: /[bundleId]");
        return;
      }

      const bundleId = parts[parts.length - 1];
      functions.logger.debug(`Requested bundle: ${bundleId}`);

      // Wait for bundle specifications to be loaded
      let bundleSpec = await spec(bundleId);

      // Check if bundle exists
      if (!bundleSpec) {
        functions.logger.warn(`Bundle not found: ${bundleId}`);
        res.status(404).send(`Could not find bundle with ID ${bundleId}`);
        return;
      }

      // Extract parameter values from query string
      const paramValues = filterQuery(req.query, bundleSpec.params || {});
      functions.logger.debug("Bundle spec:", bundleSpec);
      functions.logger.debug("Parameter values:", paramValues);

      // Set cache control headers
      setCacheControlHeaders(res, bundleSpec);

      // Try to serve from file cache if enabled
      if (bundleSpec.fileCache && typeof bundleSpec.fileCache === "number") {
        functions.logger.debug(
          `Checking file cache (TTL: ${bundleSpec.fileCache}s)`
        );

        try {
          const cachedStream = await getFileCacheStream(bundleId, paramValues, {
            ttlSec: bundleSpec.fileCache,
            gzip: canGzip,
          });

          if (cachedStream) {
            functions.logger.debug("Serving from file cache");
            cachedStream.pipe(res);
            return;
          }

          functions.logger.debug("File cache miss, building bundle");
        } catch (error) {
          // Log cache error but continue to build bundle
          functions.logger.warn("Error accessing file cache:", error);
        }
      }

      // Build the bundle
      let bundleStream: NodeJS.ReadableStream;
      try {
        const bundleBuilder = await build(
          db,
          bundleId,
          bundleSpec,
          paramValues
        );
        bundleStream = Readable.from(bundleBuilder.build());
      } catch (error) {
        functions.logger.error("Error building bundle:", error);
        res.status(500).send(`Error building bundle: ${error.message}`);
        return;
      }

      // Apply compression if needed
      let responseStream = bundleStream;
      if (canGzip) {
        const gzip = createGzip({ level: 9 });
        responseStream = responseStream.pipe(gzip);
      }

      // Save to file cache if enabled
      if (bundleSpec.fileCache && typeof bundleSpec.fileCache === "number") {
        try {
          // Create a duplicate of the stream for saving to storage
          // We need to clone the stream since we can't pipe to two destinations
          const bundleData = await cloneStream(bundleStream);
          const streamForStorage = Readable.from(bundleData);

          // Save to storage asynchronously - don't wait for it to complete
          saveBundleToStorage(bundleId, paramValues, streamForStorage)
            .then(() => {
              functions.logger.debug("Successfully saved bundle to storage");
            })
            .catch((error) => {
              functions.logger.error(
                "Failed to save bundle to storage:",
                error
              );
            });
        } catch (error) {
          // Log but continue - serving the bundle is more important than caching
          functions.logger.error("Error preparing bundle for storage:", error);
        }
      }

      // Send response to client
      responseStream.pipe(res);
    } catch (error) {
      functions.logger.error("Unhandled error:", error);
      res.status(500).send(`Internal server error: ${error.message}`);
    }
  }
);
