import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import { BundleSpec, build } from "./build_bundle";
import { Readable } from "stream";
import * as path from "path";
import * as fs from "fs";
import * as os from "os";
import { StorageService } from "./storage";
import { sortQuery, createHash } from "./utils";

// How long a build lock should be considered valid
const LOCK_TTL_MS = 9 * 60 * 1000; // 9 minutes

export class BundleBuilder {
  private db: FirebaseFirestore.Firestore;
  private storageService: StorageService;
  private bundleBuildsInProgress: { [key: string]: Promise<Buffer> } = {};

  constructor(db: FirebaseFirestore.Firestore, storageService: StorageService) {
    this.db = db;
    this.storageService = storageService;
  }

  /**
   * Checks if a bundle is already being built locally
   */
  isBuilding(cacheKey: string): boolean {
    return !!this.bundleBuildsInProgress[cacheKey];
  }

  /**
   * Gets the path for a bundle file in storage
   */
  getBundlePath(cacheKey: string): string {
    return `${cacheKey}`; // The prefix is handled by the StorageService
  }

  /**
   * Gets the path for a temporary lock file in storage
   */
  getTempFilePath(cacheKey: string): string {
    return `${this.getBundlePath(cacheKey)}.temp`;
  }

  /**
   * Checks if a bundle is being built by any function instance
   * by checking for a temporary file in storage
   */
  async isGloballyBuilding(
    cacheKey: string,
    requestId: string
  ): Promise<boolean> {
    try {
      const tempFilePath = this.getTempFilePath(cacheKey);
      const exists = await this.storageService.fileExists(tempFilePath);

      if (!exists) {
        return false;
      }

      // Get the metadata to check when the temp file was created
      const metadata = await this.storageService.getFileMetadata(tempFilePath);
      if (!metadata || !metadata.timeCreated) {
        return false;
      }

      const createTime = new Date(metadata.timeCreated).getTime();

      // If the lock file is too old, consider it stale
      if (Date.now() - createTime > LOCK_TTL_MS) {
        functions.logger.info(
          `[${requestId}] Found stale temp file for ${cacheKey}, considering it not building`
        );

        // Try to delete the stale lock
        try {
          await this.storageService.deleteFile(tempFilePath);
        } catch (deleteError) {
          // Ignore delete errors - another instance might have deleted it already
        }

        return false;
      }

      functions.logger.info(
        `[${requestId}] Bundle ${cacheKey} is being built by another instance since ${new Date(
          createTime
        ).toISOString()}`
      );
      return true;
    } catch (error) {
      functions.logger.error(
        `[${requestId}] Error checking for temp file for ${cacheKey}`,
        error
      );
      // On error, be conservative and assume it's not being built
      return false;
    }
  }

  /**
   * Creates a temp file in storage to indicate a build is in progress
   */
  async createTempFile(cacheKey: string, requestId: string): Promise<boolean> {
    try {
      const tempFilePath = this.getTempFilePath(cacheKey);

      // Create an empty temp file with just the request ID
      const metadata = {
        metadata: {
          requestId: requestId,
          timeCreated: new Date().toISOString(),
        },
      };

      await this.storageService.saveFile(tempFilePath, requestId, metadata);

      functions.logger.info(`[${requestId}] Created temp file for ${cacheKey}`);
      return true;
    } catch (error) {
      functions.logger.error(
        `[${requestId}] Error creating temp file for ${cacheKey}`,
        error
      );
      return false;
    }
  }

  /**
   * Deletes the temp file after the build is complete
   */
  async deleteTempFile(cacheKey: string, requestId: string): Promise<void> {
    try {
      const tempFilePath = this.getTempFilePath(cacheKey);
      const exists = await this.storageService.fileExists(tempFilePath);

      if (exists) {
        await this.storageService.deleteFile(tempFilePath);
        functions.logger.info(
          `[${requestId}] Deleted temp file for ${cacheKey}`
        );
      }
    } catch (error) {
      functions.logger.error(
        `[${requestId}] Error deleting temp file for ${cacheKey}`,
        error
      );
      // Continue even if this fails - it's just cleanup
    }
  }

  /**
   * Gets a reference to an in-progress build from local memory
   */
  getInProgressBuild(cacheKey: string): Promise<Buffer> | null {
    return this.bundleBuildsInProgress[cacheKey] || null;
  }

  /**
   * Registers an in-progress build in local memory
   */
  registerBuild(cacheKey: string, buildPromise: Promise<Buffer>): void {
    this.bundleBuildsInProgress[cacheKey] = buildPromise;
  }

  /**
   * Unregisters an in-progress build from local memory
   */
  unregisterBuild(cacheKey: string): void {
    delete this.bundleBuildsInProgress[cacheKey];
  }

  /**
   * Builds a bundle and returns it as a buffer.
   * Used for handling concurrent requests for the same bundle.
   */
  async buildBundle(
    bundleId: string,
    bundleSpec: BundleSpec,
    paramValues: { [key: string]: any },
    requestId: string,
    res?: any // Add optional response object parameter for sending heartbeats
  ): Promise<Buffer> {
    // Create a cache key from the bundle ID and parameters
    const cacheKey = this.createCacheKey(bundleId, paramValues);
    const bundlePath = this.getBundlePath(cacheKey);

    try {
      // Check if the bundle already exists in storage
      try {
        const exists = await this.storageService.fileExists(bundlePath);

        if (exists) {
          functions.logger.info(
            `[${requestId}] Bundle ${cacheKey} already exists in storage, checking if it's valid`
          );

          // Get cache max age from bundleSpec
          const maxAgeSeconds = bundleSpec.fileCache || 0;

          // If fileCache is specified and greater than 0, check if the file is still valid
          if (maxAgeSeconds > 0) {
            // Check if the file is recent enough to use
            const metadata = await this.storageService.getFileMetadata(
              bundlePath
            );
            if (!metadata || !metadata.timeCreated) {
              functions.logger.warn(
                `[${requestId}] Could not get metadata for bundle ${cacheKey}`
              );
            } else {
              const createTime = new Date(metadata.timeCreated).getTime();
              const fileAge = Date.now() - createTime;
              const maxAgeMs = maxAgeSeconds * 1000;

              // If the file is recent enough, just download and return it
              if (fileAge < maxAgeMs) {
                functions.logger.info(
                  `[${requestId}] Bundle ${cacheKey} is still valid (age: ${Math.round(
                    fileAge / 1000
                  )}s, max age: ${maxAgeSeconds}s), using cached version`
                );
                const buffer = await this.storageService.downloadFile(
                  bundlePath
                );
                return buffer;
              } else {
                functions.logger.info(
                  `[${requestId}] Bundle ${cacheKey} is too old (age: ${Math.round(
                    fileAge / 1000
                  )}s, max age: ${maxAgeSeconds}s), rebuilding`
                );
              }
            }
          } else {
            functions.logger.info(
              `[${requestId}] No fileCache setting or set to 0 for ${cacheKey}, rebuilding regardless of age`
            );
          }
        }
      } catch (storageError) {
        // If there's an error checking storage, just continue with the build
        functions.logger.warn(
          `[${requestId}] Error checking if bundle exists in storage: ${storageError.message}`
        );
      }

      // Check if this bundle is already being built in this instance
      if (this.isBuilding(cacheKey)) {
        functions.logger.info(
          `[${requestId}] Bundle ${cacheKey} is already being built locally, reusing in-progress build`
        );
        const cachedBuild = this.getInProgressBuild(cacheKey);
        if (cachedBuild) return cachedBuild;
      }

      // Check if another instance is building this bundle by looking for a temp file
      const isGloballyBuilding = await this.isGloballyBuilding(
        cacheKey,
        requestId
      );

      if (isGloballyBuilding) {
        // Wait and check for the result
        functions.logger.info(
          `[${requestId}] Bundle ${cacheKey} is being built by another instance, waiting...`
        );

        // We'll use a different approach for keeping the connection alive
        // Instead of modifying the response directly, we'll just keep the request active
        // by continuing to poll and waiting for the build to complete

        // Poll for completion (simple approach)
        let attempts = 0;
        const maxAttempts = 270; // 270 attempts x 2 seconds = ~9 minutes max wait (matching timeoutSeconds)

        while (attempts < maxAttempts) {
          // Wait 2 seconds between checks
          await new Promise((resolve) => setTimeout(resolve, 2000));

          // Log heartbeat attempt to keep the server active
          if (attempts > 0 && attempts % 15 === 0) {
            // Every 30 seconds
            functions.logger.debug(
              `[${requestId}] Still waiting after ${attempts * 2}s...`
            );
          }

          // Check if the temp file is still there
          const stillBuilding = await this.isGloballyBuilding(
            cacheKey,
            requestId
          );

          if (!stillBuilding) {
            // Check if the bundle now exists in storage
            try {
              const exists = await this.storageService.fileExists(bundlePath);

              if (exists) {
                functions.logger.info(
                  `[${requestId}] Bundle ${cacheKey} was successfully built by another instance`
                );

                // Download and return the bundle
                const buffer = await this.storageService.downloadFile(
                  bundlePath
                );
                return buffer;
              }
            } catch (storageError) {
              functions.logger.warn(
                `[${requestId}] Error checking if bundle was created: ${storageError.message}`
              );
            }

            // If we get here, either the temp file disappeared but the bundle wasn't created,
            // or there was an error checking. Either way, we'll build it ourselves.
            functions.logger.info(
              `[${requestId}] Temp file gone but bundle not found. Building locally...`
            );
            break;
          }

          attempts++;
        }

        if (attempts >= maxAttempts) {
          functions.logger.warn(
            `[${requestId}] Waited too long for bundle ${cacheKey} from another instance, building locally`
          );
        }
      }

      // Create a temp file to indicate we're building this bundle
      await this.createTempFile(cacheKey, requestId);

      // Build the bundle
      const buildPromise = this._buildBundleInternal(
        bundleId,
        bundleSpec,
        paramValues,
        requestId,
        cacheKey
      );

      // Register in local cache
      this.registerBuild(cacheKey, buildPromise);

      return buildPromise;
    } catch (error) {
      functions.logger.error(
        `[${requestId}] Error in buildBundle for ${cacheKey}`,
        error
      );

      // Make sure to clean up the temp file if there was an error
      await this.deleteTempFile(cacheKey, requestId);

      throw error;
    }
  }

  /**
   * Internal method that handles the actual bundle building process
   */
  private async _buildBundleInternal(
    bundleId: string,
    bundleSpec: BundleSpec,
    paramValues: { [key: string]: any },
    requestId: string,
    cacheKey: string
  ): Promise<Buffer> {
    try {
      functions.logger.info(`[${requestId}] Building bundle: ${bundleId}`);
      const startTime = Date.now();

      // Measure the time spent in the build function
      const buildFunctionStartTime = Date.now();
      const bundleBuilder = await build(
        this.db,
        bundleId,
        bundleSpec,
        paramValues,
        requestId
      );
      const buildFunctionDuration = Date.now() - buildFunctionStartTime;
      functions.logger.info(
        `[${requestId}] Bundle builder created in ${buildFunctionDuration}ms`
      );

      // Measure the time spent in bundleBuilder.build()
      const buildDataStartTime = Date.now();
      const bundleData = bundleBuilder.build();
      const buildDataDuration = Date.now() - buildDataStartTime;
      functions.logger.info(
        `[${requestId}] Bundle data serialization took ${buildDataDuration}ms`
      );

      // Measure the time spent in Buffer.from
      const bufferCreationStartTime = Date.now();
      const bundleBuffer = Buffer.from(bundleData);
      const bufferCreationDuration = Date.now() - bufferCreationStartTime;
      functions.logger.info(
        `[${requestId}] Buffer creation took ${bufferCreationDuration}ms`
      );

      const duration = Date.now() - startTime;
      functions.logger.info(
        `[${requestId}] Bundle built successfully: ${bundleId}, size: ${bundleBuffer.length} bytes, total duration: ${duration}ms (build: ${buildFunctionDuration}ms, serialize: ${buildDataDuration}ms, buffer: ${bufferCreationDuration}ms)`
      );

      // Save the bundle to storage
      try {
        const bundlePath = this.getBundlePath(cacheKey);
        const metadata = {
          metadata: {
            requestId: requestId,
            buildTime: duration,
            bundleSize: bundleBuffer.length,
          },
        };

        await this.storageService.saveFile(bundlePath, bundleBuffer, metadata);

        functions.logger.info(
          `[${requestId}] Saved bundle ${cacheKey} to storage`
        );
      } catch (storageError) {
        functions.logger.error(
          `[${requestId}] Error saving bundle to storage: ${storageError.message}`
        );
        // Continue even if storage fails - we'll still return the buffer directly
      }

      return bundleBuffer;
    } catch (error) {
      functions.logger.error(
        `[${requestId}] Error building bundle: ${bundleId}`,
        error
      );

      throw error;
    } finally {
      // Always clean up the temp file and local cache
      await this.deleteTempFile(cacheKey, requestId);
      this.unregisterBuild(cacheKey);
    }
  }

  /**
   * Creates a unique cache key from bundle ID and parameters
   * That matches the storage path created by createStoragePath
   */
  createCacheKey(
    bundleId: string,
    paramValues: { [key: string]: any }
  ): string {
    // Sanitize bundle ID to avoid path issues
    const sanitizedBundleId = bundleId.replace(/[^a-zA-Z0-9_-]/g, "_");

    // Create query hash (same as in createStoragePath)
    const queryString = sortQuery(paramValues);
    const queryHash = createHash(queryString);

    // Create sorted params hash (same as in createStoragePath)
    const sortedParams = Object.keys(paramValues)
      .sort()
      .map((key) => `${key}:${JSON.stringify(paramValues[key])}`)
      .join(",");
    const sortedParamsHash = createHash(sortedParams);

    // Use the same pattern as createStoragePath but without the prefix
    // Since the storage prefix is handled by the StorageService
    return `${sanitizedBundleId}_${queryHash}_${sortedParamsHash}`;
  }

  /**
   * Creates a readable stream from a buffer
   */
  createStream(buffer: Buffer): NodeJS.ReadableStream {
    return Readable.from(buffer);
  }
}
