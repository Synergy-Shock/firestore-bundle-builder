import * as functions from "firebase-functions";
import { Storage } from "@google-cloud/storage";
import { createGzip } from "zlib";
import { createStoragePath } from "./utils";
import { Readable } from "stream";

// Initialize Storage
const storage = new Storage();

export interface StorageOptions {
  bucketName: string;
  storagePrefix: string;
}

export class StorageService {
  private bucket;
  private storagePrefix: string;

  constructor(options: StorageOptions) {
    this.bucket = storage.bucket(options.bucketName);
    this.storagePrefix = options.storagePrefix;
  }

  /**
   * Validates that we can access the bucket
   */
  async validateBucket(): Promise<boolean> {
    try {
      const [exists] = await this.bucket.exists();
      if (!exists) {
        functions.logger.error(`Bucket ${this.bucket.name} does not exist!`);
        return false;
      } else {
        functions.logger.info(
          `Successfully connected to bucket: ${this.bucket.name}`
        );
        return true;
      }
    } catch (error) {
      functions.logger.error(
        `Error accessing bucket ${this.bucket.name}:`,
        error
      );
      return false;
    }
  }

  /**
   * Downloads and returns a bundle from the file cache if available and not expired
   * For large files (>5MB), this uses streaming to avoid memory issues
   *
   * Note: Make sure the query parameters are already filtered using filterQuery
   */
  async getBundle(
    bundleId: string,
    query: { [k: string]: any },
    ttlSec: number,
    requestId: string,
    clientAcceptsGzip: boolean = false
  ): Promise<{
    stream: NodeJS.ReadableStream;
    size: number;
    isCompressed: boolean;
    contentType: string;
  } | null> {
    // The query parameters should already be filtered by the caller
    const path = createStoragePath(bundleId, query, this.storagePrefix);
    const file = this.bucket.file(path);

    try {
      // Check if file exists
      functions.logger.debug(`[${requestId}] Checking if file exists: ${path}`);
      const [exists] = await file.exists();
      if (!exists) {
        functions.logger.debug(`[${requestId}] File does not exist: ${path}`);
        return null;
      }

      // Check file metadata for age
      functions.logger.debug(
        `[${requestId}] File exists, checking metadata: ${path}`
      );
      const [metadata] = await file.getMetadata();

      // Log the metadata for debugging
      functions.logger.debug(`[${requestId}] File metadata:`, {
        contentType: metadata.contentType,
        contentEncoding: metadata.contentEncoding,
        size: metadata.size,
        timeCreated: metadata.timeCreated,
      });

      const createTime = new Date(metadata.timeCreated).getTime();
      const now = Date.now();
      const ageInSeconds = (now - createTime) / 1000;

      // Check if file has expired based on TTL
      if (ageInSeconds > ttlSec) {
        functions.logger.debug(
          `[${requestId}] File exists but has expired (age: ${ageInSeconds}s): ${path}`
        );
        return null;
      }

      functions.logger.debug(
        `[${requestId}] Found valid cached file: ${path}, preparing stream`
      );

      const isGzipped = metadata.contentEncoding === "gzip";
      const fileSize = parseInt(metadata.size, 10) || 0;
      const contentType = metadata.contentType || "application/octet-stream";

      // Decision tree for handling compression based on client's capabilities and file state
      if (isGzipped && clientAcceptsGzip) {
        // 1. File is gzipped and client accepts gzip - stream directly
        functions.logger.debug(
          `[${requestId}] File is gzipped and client accepts gzip, streaming compressed file directly`
        );

        return {
          stream: file.createReadStream(),
          size: fileSize,
          isCompressed: true,
          contentType,
        };
      } else if (isGzipped && !clientAcceptsGzip) {
        // 2. File is gzipped but client doesn't accept gzip - decompress on-the-fly
        functions.logger.debug(
          `[${requestId}] File is gzipped but client doesn't accept gzip, decompressing on-the-fly`
        );

        const zlib = require("zlib");
        const gunzip = zlib.createGunzip();
        const fileStream = file.createReadStream();

        // Set up error handling for the stream
        gunzip.on("error", (err) => {
          functions.logger.error(
            `[${requestId}] Error decompressing stream: ${err}`
          );
        });

        fileStream.on("error", (err) => {
          functions.logger.error(
            `[${requestId}] Error reading file stream: ${err}`
          );
        });

        // Pipe through decompression
        return {
          stream: fileStream.pipe(gunzip),
          size: fileSize, // Note: This is the compressed size, actual size will be larger
          isCompressed: false,
          contentType,
        };
      } else {
        // 3. File is not compressed - stream directly
        functions.logger.debug(
          `[${requestId}] File is not compressed, streaming directly`
        );

        return {
          stream: file.createReadStream(),
          size: fileSize,
          isCompressed: false,
          contentType,
        };
      }
    } catch (e) {
      functions.logger.error(
        `[${requestId}] Error checking/reading cached file:`,
        e
      );
      return null;
    }
  }

  /**
   * Saves a bundle to Cloud Storage with proper error handling
   * Uses streaming for efficiency with large files
   *
   * Note: Make sure the query parameters are already filtered using filterQuery
   */
  async saveBundle(
    bundleId: string,
    query: { [k: string]: any },
    buffer: Buffer,
    requestId: string
  ): Promise<boolean> {
    // The query parameters should already be filtered by the caller
    const path = createStoragePath(bundleId, query, this.storagePrefix);
    functions.logger.debug(`[${requestId}] Saving bundle to: ${path}`);

    try {
      // For large files, streaming compression is more memory-efficient
      const isLargeFile = buffer.length > 5 * 1024 * 1024; // 5MB threshold

      if (isLargeFile) {
        functions.logger.debug(
          `[${requestId}] Large file detected (${buffer.length} bytes), using streaming compression`
        );

        return new Promise<boolean>((resolve, reject) => {
          const zlib = require("zlib");
          const gzip = zlib.createGzip({ level: 6 }); // Lower level for better performance
          const bufferStream = Readable.from(buffer);

          // Track compressed size for logging
          let compressedSize = 0;
          gzip.on("data", (chunk) => {
            compressedSize += chunk.length;
          });

          const writeStream = this.bucket.file(path).createWriteStream({
            metadata: {
              contentEncoding: "gzip",
              contentType: "application/octet-stream",
            },
            resumable: true, // Use resumable uploads for large files
          });

          writeStream.on("error", (err) => {
            functions.logger.error(
              `[${requestId}] Error saving bundle to storage: ${err}`
            );
            reject(err);
          });

          writeStream.on("finish", () => {
            functions.logger.debug(
              `[${requestId}] Successfully saved bundle to storage: ${path}, compressed size: ~${compressedSize} bytes`
            );
            resolve(true);
          });

          // Setup error handling on the streams
          gzip.on("error", (err) => {
            functions.logger.error(
              `[${requestId}] Error compressing data: ${err}`
            );
            reject(err);
          });

          bufferStream.on("error", (err) => {
            functions.logger.error(
              `[${requestId}] Error reading buffer: ${err}`
            );
            reject(err);
          });

          // Pipe the data
          bufferStream.pipe(gzip).pipe(writeStream);
        });
      } else {
        // For smaller files, the original method works well
        functions.logger.debug(
          `[${requestId}] Standard file size (${buffer.length} bytes), using regular compression`
        );

        // Compress the buffer
        const zlib = require("zlib");
        const compressedBuffer = await new Promise<Buffer>(
          (resolve, reject) => {
            zlib.gzip(buffer, { level: 9 }, (err, result) => {
              if (err) {
                functions.logger.error(
                  `[${requestId}] Compression error:`,
                  err
                );
                reject(err);
              } else {
                functions.logger.debug(
                  `[${requestId}] Compressed bundle: ${result.length} bytes (from ${buffer.length} bytes)`
                );
                resolve(result);
              }
            });
          }
        );

        // Upload the compressed buffer
        await this.bucket.file(path).save(compressedBuffer, {
          metadata: {
            contentType: "application/octet-stream",
            contentEncoding: "gzip",
          },
        });

        functions.logger.debug(
          `[${requestId}] Successfully saved bundle to storage: ${path}`
        );
        return true;
      }
    } catch (error) {
      functions.logger.error(
        `[${requestId}] Error saving bundle to storage:`,
        error
      );
      return false;
    }
  }
}
