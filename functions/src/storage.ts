import * as functions from "firebase-functions";
import { Storage } from "@google-cloud/storage";
import { createGunzip, createGzip, gzip } from "zlib";
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
  } | null> {
    const startTime = Date.now();
    functions.logger.debug(`[${requestId}] Starting getBundle operation`);

    // The query parameters should already be filtered by the caller
    const path = createStoragePath(bundleId, query, this.storagePrefix);
    const file = this.bucket.file(path);

    try {
      // Check if file exists
      const existsStartTime = Date.now();
      functions.logger.debug(`[${requestId}] Checking if file exists: ${path}`);
      const [exists] = await file.exists();
      const existsDuration = Date.now() - existsStartTime;
      functions.logger.debug(
        `[${requestId}] File exists check took ${existsDuration}ms, result: ${exists}`
      );

      if (!exists) {
        functions.logger.debug(`[${requestId}] File does not exist: ${path}`);
        const totalDuration = Date.now() - startTime;
        functions.logger.info(
          `[${requestId}] getBundle operation completed in ${totalDuration}ms (file not found)`
        );
        return null;
      }

      // Check file metadata for age
      const metadataStartTime = Date.now();
      functions.logger.debug(
        `[${requestId}] File exists, checking metadata: ${path}`
      );
      const [metadata] = await file.getMetadata();
      const metadataDuration = Date.now() - metadataStartTime;
      functions.logger.debug(
        `[${requestId}] Metadata retrieval took ${metadataDuration}ms`
      );

      // Log the metadata for debugging
      functions.logger.debug(`[${requestId}] File metadata:`, {
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
        const totalDuration = Date.now() - startTime;
        functions.logger.info(
          `[${requestId}] getBundle operation completed in ${totalDuration}ms (file expired)`
        );
        return null;
      }

      functions.logger.debug(
        `[${requestId}] Found valid cached file: ${path}, preparing stream`
      );

      const isGzipped = metadata.contentEncoding === "gzip";
      const fileSize = parseInt(metadata.size, 10) || 0;

      // Decision tree for handling compression based on client's capabilities and file state
      if (isGzipped && clientAcceptsGzip) {
        // 1. File is gzipped and client accepts gzip - stream directly
        functions.logger.debug(
          `[${requestId}] File is gzipped and client accepts gzip, streaming compressed file directly`
        );

        const createStreamTime = Date.now();
        const stream = file.createReadStream({
          decompress: false,
        });
        const createStreamDuration = Date.now() - createStreamTime;
        functions.logger.debug(
          `[${requestId}] Stream creation took ${createStreamDuration}ms`
        );

        const totalDuration = Date.now() - startTime;
        functions.logger.info(
          `[${requestId}] getBundle operation completed in ${totalDuration}ms (returning compressed stream)`
        );

        return {
          stream,
          size: fileSize,
          isCompressed: true,
        };
      } else if (isGzipped && !clientAcceptsGzip) {
        // 2. File is gzipped but client doesn't accept gzip - decompress on-the-fly
        functions.logger.debug(
          `[${requestId}] File is gzipped but client doesn't accept gzip, decompressing on-the-fly`
        );

        const createStreamTime = Date.now();
        const gunzip = createGunzip();
        const fileStream = file.createReadStream({
          decompress: false,
        });

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
        const stream = fileStream.pipe(gunzip);
        const createStreamDuration = Date.now() - createStreamTime;
        functions.logger.debug(
          `[${requestId}] Decompressed stream creation took ${createStreamDuration}ms`
        );

        const totalDuration = Date.now() - startTime;
        functions.logger.info(
          `[${requestId}] getBundle operation completed in ${totalDuration}ms (returning decompressed stream)`
        );

        return {
          stream,
          size: fileSize, // Note: This is the compressed size, actual size will be larger
          isCompressed: false,
        };
      } else {
        // 3. File is not compressed - stream directly
        functions.logger.debug(
          `[${requestId}] File is not compressed, streaming directly`
        );

        const createStreamTime = Date.now();
        const stream = file.createReadStream({
          decompress: false,
        });
        const createStreamDuration = Date.now() - createStreamTime;
        functions.logger.debug(
          `[${requestId}] Stream creation took ${createStreamDuration}ms`
        );

        const totalDuration = Date.now() - startTime;
        functions.logger.info(
          `[${requestId}] getBundle operation completed in ${totalDuration}ms (returning uncompressed stream)`
        );

        return {
          stream,
          size: fileSize,
          isCompressed: false,
        };
      }
    } catch (e) {
      const totalDuration = Date.now() - startTime;
      functions.logger.error(
        `[${requestId}] Error checking/reading cached file after ${totalDuration}ms:`,
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
    const startTime = Date.now();
    functions.logger.debug(
      `[${requestId}] Starting saveBundle operation, buffer size: ${buffer.length} bytes`
    );

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
          const compressionStartTime = Date.now();
          const gzip = createGzip({ level: 6 }); // Lower level for better performance
          const bufferStream = Readable.from(buffer);

          // Track compressed size for logging
          let compressedSize = 0;
          gzip.on("data", (chunk) => {
            compressedSize += chunk.length;
          });

          const writeStream = this.bucket.file(path).createWriteStream({
            metadata: {
              contentEncoding: "gzip",
            },
            resumable: true, // Use resumable uploads for large files
          });

          writeStream.on("error", (err) => {
            const uploadDuration = Date.now() - compressionStartTime;
            functions.logger.error(
              `[${requestId}] Error saving bundle to storage after ${uploadDuration}ms: ${err}`
            );
            reject(err);
          });

          writeStream.on("finish", () => {
            const uploadDuration = Date.now() - compressionStartTime;
            const totalDuration = Date.now() - startTime;
            functions.logger.debug(
              `[${requestId}] Successfully saved bundle to storage: ${path}, compressed size: ~${compressedSize} bytes, upload took ${uploadDuration}ms, total operation: ${totalDuration}ms`
            );
            resolve(true);
          });

          // Setup error handling on the streams
          gzip.on("error", (err) => {
            const compressionDuration = Date.now() - compressionStartTime;
            functions.logger.error(
              `[${requestId}] Error compressing data after ${compressionDuration}ms: ${err}`
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
        const compressionStartTime = Date.now();
        const compressedBuffer = await new Promise<Buffer>(
          (resolve, reject) => {
            gzip(buffer, { level: 9 }, (err, result) => {
              if (err) {
                const compressionDuration = Date.now() - compressionStartTime;
                functions.logger.error(
                  `[${requestId}] Compression error after ${compressionDuration}ms:`,
                  err
                );
                reject(err);
              } else {
                const compressionDuration = Date.now() - compressionStartTime;
                functions.logger.debug(
                  `[${requestId}] Compressed bundle in ${compressionDuration}ms: ${
                    result.length
                  } bytes (from ${buffer.length} bytes), ratio: ${(
                    (result.length / buffer.length) *
                    100
                  ).toFixed(1)}%`
                );
                resolve(result);
              }
            });
          }
        );

        // Upload the compressed buffer
        const uploadStartTime = Date.now();
        await this.bucket.file(path).save(compressedBuffer, {
          metadata: {
            contentEncoding: "gzip",
          },
        });
        const uploadDuration = Date.now() - uploadStartTime;
        const totalDuration = Date.now() - startTime;

        functions.logger.debug(
          `[${requestId}] Upload completed in ${uploadDuration}ms, total operation: ${totalDuration}ms`
        );

        functions.logger.info(
          `[${requestId}] Successfully saved bundle to storage: ${path}, total time: ${totalDuration}ms`
        );
        return true;
      }
    } catch (error) {
      const totalDuration = Date.now() - startTime;
      functions.logger.error(
        `[${requestId}] Error saving bundle to storage after ${totalDuration}ms:`,
        error
      );
      return false;
    }
  }

  /**
   * Utility method to check if a file exists in storage
   * @param relativePath Path relative to the storage prefix
   * @returns Whether the file exists
   */
  async fileExists(relativePath: string): Promise<boolean> {
    try {
      const fullPath = `${this.storagePrefix}/${relativePath}`;
      const file = this.bucket.file(fullPath);
      const [exists] = await file.exists();
      return exists;
    } catch (error) {
      functions.logger.error(
        `Error checking if file exists: ${relativePath}`,
        error
      );
      return false;
    }
  }

  /**
   * Utility method to get metadata for a file
   * @param relativePath Path relative to the storage prefix
   * @returns File metadata or null if not found or error
   */
  async getFileMetadata(relativePath: string): Promise<any | null> {
    try {
      const fullPath = `${this.storagePrefix}/${relativePath}`;
      const file = this.bucket.file(fullPath);
      const [metadata] = await file.getMetadata();
      return metadata;
    } catch (error) {
      functions.logger.error(
        `Error getting file metadata: ${relativePath}`,
        error
      );
      return null;
    }
  }

  /**
   * Utility method to delete a file from storage
   * @param relativePath Path relative to the storage prefix
   * @returns Whether the deletion was successful
   */
  async deleteFile(relativePath: string): Promise<boolean> {
    try {
      const fullPath = `${this.storagePrefix}/${relativePath}`;
      const file = this.bucket.file(fullPath);
      await file.delete();
      return true;
    } catch (error) {
      functions.logger.error(`Error deleting file: ${relativePath}`, error);
      return false;
    }
  }

  /**
   * Utility method to download a file from storage
   * @param relativePath Path relative to the storage prefix
   * @returns The file as a Buffer, or null if not found or error
   */
  async downloadFile(relativePath: string): Promise<Buffer | null> {
    try {
      const fullPath = `${this.storagePrefix}/${relativePath}`;
      const file = this.bucket.file(fullPath);
      const [buffer] = await file.download();
      return buffer;
    } catch (error) {
      functions.logger.error(`Error downloading file: ${relativePath}`, error);
      return null;
    }
  }

  /**
   * Utility method to save a file to storage
   * @param relativePath Path relative to the storage prefix
   * @param data The data to save (string, Buffer, or stream)
   * @param options Options for the save operation
   * @param compress Whether to apply gzip compression (default: true)
   * @returns Whether the save was successful
   */
  async saveFile(
    relativePath: string,
    data: string | Buffer,
    options?: any,
    compress: boolean = true
  ): Promise<boolean> {
    try {
      const fullPath = `${this.storagePrefix}/${relativePath}`;
      const file = this.bucket.file(fullPath);

      // Only compress Buffer data, not strings (which are typically small)
      // Strings like temp file markers or metadata shouldn't be compressed
      if (compress && Buffer.isBuffer(data) && data.length > 0) {
        // For large files (>5MB), use streaming compression
        if (data.length > 5 * 1024 * 1024) {
          return new Promise<boolean>((resolve, reject) => {
            const gzip = createGzip({ level: 6 });
            const bufferStream = Readable.from(data);

            const writeOptions = {
              ...options,
              metadata: {
                ...(options?.metadata || {}),
                contentEncoding: "gzip",
              },
            };

            const writeStream = file.createWriteStream(writeOptions);

            writeStream.on("error", (err) => {
              functions.logger.error(
                `Error saving compressed file: ${relativePath}`,
                err
              );
              reject(err);
            });

            writeStream.on("finish", () => {
              resolve(true);
            });

            // Setup error handling on the streams
            gzip.on("error", (err) => {
              functions.logger.error(
                `Error compressing data for file: ${relativePath}`,
                err
              );
              reject(err);
            });

            bufferStream.on("error", (err) => {
              functions.logger.error(
                `Error reading buffer for file: ${relativePath}`,
                err
              );
              reject(err);
            });

            // Pipe the data
            bufferStream.pipe(gzip).pipe(writeStream);
          });
        } else {
          // For smaller files, use synchronous compression
          const compressedData = await new Promise<Buffer>(
            (resolve, reject) => {
              gzip(data, { level: 9 }, (err, result) => {
                if (err) {
                  reject(err);
                } else {
                  resolve(result);
                }
              });
            }
          );

          const saveOptions = {
            ...options,
            metadata: {
              ...(options?.metadata || {}),
              contentEncoding: "gzip",
            },
          };

          await file.save(compressedData, saveOptions);
        }
      } else {
        // Skip compression for strings (like temp files) or if explicitly disabled
        functions.logger.debug(
          `Saving file without compression: ${relativePath} (${
            typeof data === "string"
              ? "string data"
              : "buffer with compression disabled"
          })`
        );
        await file.save(data, options);
      }

      return true;
    } catch (error) {
      functions.logger.error(`Error saving file: ${relativePath}`, error);
      return false;
    }
  }
}
