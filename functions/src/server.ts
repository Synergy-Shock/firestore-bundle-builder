import * as functions from "firebase-functions";
import { IncomingMessage, ServerResponse } from "http";
import { createGzip } from "zlib";
import { Readable } from "stream";
import { filterQuery, setCacheControlHeaders } from "./utils";
import { StorageService } from "./storage";
import { BundleBuilder } from "./bundleBuilder";
import { SpecManager } from "./specManager";

// Define a type that includes Node.js stream properties we need
interface NodeStream {
  destroyed?: boolean;
  destroy?: () => void;
}

// Extend IncomingMessage to add Firebase Functions properties
interface FunctionRequest extends IncomingMessage {
  path?: string;
  query?: any;
  headers: {
    [key: string]: string | string[] | undefined;
  };
}

// Extend ServerResponse to add Firebase Functions properties
interface FunctionResponse extends ServerResponse {
  headersSent: boolean;
}

export class BundleServer {
  private storageService: StorageService;
  private bundleBuilder: BundleBuilder;
  private specManager: SpecManager;

  constructor(
    storageService: StorageService,
    bundleBuilder: BundleBuilder,
    specManager: SpecManager
  ) {
    this.storageService = storageService;
    this.bundleBuilder = bundleBuilder;
    this.specManager = specManager;
  }

  /**
   * Handles HTTP requests for bundles
   */
  async handleRequest(
    req: FunctionRequest,
    res: FunctionResponse
  ): Promise<void> {
    const requestId = Math.random().toString(36).substring(2, 15);
    const requestStartTime = Date.now();
    functions.logger.info(`[${requestId}] Request started: ${req.path}`, {
      path: req.path,
      query: req.query,
      userAgent: req.headers["user-agent"],
      referer: req.headers["referer"],
    });

    // Set a default timeout to prevent request timeouts from killing connections
    // This keeps the connection alive by setting a timeout that will never fire
    // since we'll resolve the request before then
    const keepAliveTimeout = setTimeout(() => {
      // This will never execute, but it keeps the request open
    }, 9 * 60 * 1000); // 9 minutes timeout (matching the function timeout)

    // Track streams to ensure they're properly destroyed at the end
    const streamsToCleanup: Array<NodeJS.ReadableStream & NodeStream> = [];

    try {
      // Set CORS headers for browser compatibility
      res.setHeader("Access-Control-Allow-Origin", "*");
      if (req.method === "OPTIONS") {
        res.setHeader("Access-Control-Allow-Methods", "GET");
        res.setHeader("Access-Control-Allow-Headers", "Content-Type");
        res.setHeader("Access-Control-Max-Age", "3600");
        res.statusCode = 204;
        res.end("");

        // Use the same pattern for clearing the timeout
        const timeoutToCleanOptions = keepAliveTimeout;
        clearTimeout(timeoutToCleanOptions);

        functions.logger.info(
          `[${requestId}] OPTIONS request completed in ${
            Date.now() - requestStartTime
          }ms`
        );
        return;
      }

      functions.logger.debug(`[${requestId}] Request headers:`, req.headers);
      const acceptEncoding = req.headers["accept-encoding"];
      const canGzip =
        acceptEncoding && typeof acceptEncoding === "string"
          ? acceptEncoding.includes("gzip")
          : false;

      // Parse bundle ID from path
      const path = req.path || req.url || "";
      const parts = path.split("/").filter((p) => p.length > 0);
      if (parts.length === 0) {
        functions.logger.warn(`[${requestId}] Invalid request path: ${path}`);
        res.statusCode = 400;
        res.end("Invalid request path. Expected format: /[bundleId]");

        // Use the same pattern for clearing the timeout
        const timeoutToCleanPath = keepAliveTimeout;
        clearTimeout(timeoutToCleanPath);

        return;
      }

      const bundleId = parts[parts.length - 1];
      functions.logger.info(`[${requestId}] Processing bundle: ${bundleId}`);

      // Wait for bundle specifications to be loaded
      const specStartTime = Date.now();
      let bundleSpec = await this.specManager.getSpec(bundleId);
      const specDuration = Date.now() - specStartTime;
      functions.logger.info(
        `[${requestId}] Spec retrieval took ${specDuration}ms`
      );

      // Check if bundle exists
      if (!bundleSpec) {
        functions.logger.warn(`[${requestId}] Bundle not found: ${bundleId}`);
        res.statusCode = 404;
        res.end(`Could not find bundle with ID ${bundleId}`);

        // Use the same pattern for clearing the timeout
        const timeoutToCleanNotFound = keepAliveTimeout;
        clearTimeout(timeoutToCleanNotFound);

        return;
      }

      // Extract parameter values from query string
      const paramValues = filterQuery(req.query, bundleSpec.params || {});
      functions.logger.debug(`[${requestId}] Parameter values:`, paramValues);

      // Set cache control headers
      setCacheControlHeaders(
        res,
        bundleSpec.serverCache,
        bundleSpec.clientCache
      );

      // Create a cache key for this bundle request (including params)
      const cacheKey = this.bundleBuilder.createCacheKey(bundleId, paramValues);

      // Try to serve from file cache if enabled
      if (bundleSpec.fileCache && typeof bundleSpec.fileCache === "number") {
        functions.logger.debug(
          `[${requestId}] Checking file cache (TTL: ${bundleSpec.fileCache}s)`
        );

        try {
          const cacheStartTime = Date.now();
          const cachedBundle = await this.storageService.getBundle(
            bundleId,
            paramValues,
            bundleSpec.fileCache,
            requestId,
            canGzip
          );
          const cacheCheckDuration = Date.now() - cacheStartTime;
          functions.logger.info(
            `[${requestId}] Cache check took ${cacheCheckDuration}ms, hit: ${!!cachedBundle}`
          );

          if (cachedBundle) {
            functions.logger.info(
              `[${requestId}] Serving from file cache (size: ${cachedBundle.size} bytes)`
            );

            // Track the stream for cleanup
            streamsToCleanup.push(cachedBundle.stream);

            // Handle compression header if needed - explicitly lowercase to match browser expectations
            if (cachedBundle.isCompressed) {
              res.setHeader("content-encoding", "gzip");
            } else if (cachedBundle.size > 0) {
              // For uncompressed content with known size, set Content-Length
              res.setHeader("content-length", cachedBundle.size.toString());
            }

            // Note: We don't set content-type as requested by the user

            // Simplify to mirror original implementation - directly pipe to response
            // IMPORTANT: Create a Promise that resolves when the stream completes or errors
            const streamStartTime = Date.now();

            // Clear the keep-alive timeout just before starting to stream
            const timeoutToClean = keepAliveTimeout;
            clearTimeout(timeoutToClean);
            return new Promise<void>((resolve, reject) => {
              cachedBundle.stream
                .on("error", (err) => {
                  functions.logger.error(
                    `[${requestId}] Error in stream: ${err}`
                  );
                  // Destroy stream to clean up resources
                  if (!(cachedBundle.stream as NodeStream).destroyed) {
                    (cachedBundle.stream as NodeStream).destroy?.();
                  }
                  reject(err);
                })
                .pipe(res)
                .on("finish", () => {
                  const streamDuration = Date.now() - streamStartTime;
                  functions.logger.debug(
                    `[${requestId}] Stream finished successfully in ${streamDuration}ms`
                  );
                  // Destroy stream to clean up resources
                  if (!(cachedBundle.stream as NodeStream).destroyed) {
                    (cachedBundle.stream as NodeStream).destroy?.();
                  }
                  const totalDuration = Date.now() - requestStartTime;
                  functions.logger.info(
                    `[${requestId}] Total cached response time: ${totalDuration}ms (cache check: ${cacheCheckDuration}ms, streaming: ${streamDuration}ms)`
                  );
                  resolve();
                })
                .on("error", (err) => {
                  functions.logger.error(
                    `[${requestId}] Error in response stream: ${err}`
                  );
                  // Destroy stream to clean up resources
                  if (!(cachedBundle.stream as NodeStream).destroyed) {
                    (cachedBundle.stream as NodeStream).destroy?.();
                  }
                  reject(err);
                });

              // Also handle response close/end events
              res.on("close", () => {
                functions.logger.debug(`[${requestId}] Response closed`);
                // Destroy stream to clean up resources
                if (!(cachedBundle.stream as NodeStream).destroyed) {
                  (cachedBundle.stream as NodeStream).destroy?.();
                }
                resolve();
              });
            });
          }

          functions.logger.debug(
            `[${requestId}] File cache miss, building bundle`
          );
        } catch (error) {
          // Log cache error but continue to build bundle
          functions.logger.warn(
            `[${requestId}] Error accessing file cache:`,
            error
          );
        }
      }

      // Check if this bundle is already being built by another request
      if (this.bundleBuilder.isBuilding(cacheKey)) {
        functions.logger.info(
          `[${requestId}] Bundle build already in progress for ${bundleId}, reusing...`
        );

        try {
          // Reuse the in-progress build
          const inProgressBuild =
            this.bundleBuilder.getInProgressBuild(cacheKey);
          if (!inProgressBuild) {
            throw new Error("Expected in-progress build not found");
          }

          const sharedBuildStartTime = Date.now();
          const bundleBuffer = await inProgressBuild;
          const sharedBuildDuration = Date.now() - sharedBuildStartTime;
          functions.logger.info(
            `[${requestId}] Shared build completed in ${sharedBuildDuration}ms`
          );

          // Clear the keep-alive timeout before serving bundle
          const timeoutToClean = keepAliveTimeout;
          clearTimeout(timeoutToClean);
          return this.serveBundle(
            bundleBuffer,
            bundleId,
            requestId,
            res,
            canGzip,
            requestStartTime,
            streamsToCleanup,
            bundleSpec,
            paramValues,
            cacheKey
          );
        } catch (error) {
          functions.logger.error(
            `[${requestId}] Error waiting for shared build: ${error}`
          );
          res.statusCode = 500;
          res.end(`Error building bundle: ${error.message}`);

          // Use the same pattern for clearing the timeout
          const timeoutToCleanError = keepAliveTimeout;
          clearTimeout(timeoutToCleanError);

          return;
        }
      }

      // If we get here, we need to build the bundle ourselves
      functions.logger.info(
        `[${requestId}] Building bundle ${bundleId} with params:`,
        paramValues
      );

      try {
        // Create a cache key for this bundle request
        const cacheKey = this.bundleBuilder.createCacheKey(
          bundleId,
          paramValues
        );

        // Standard approach - build and then serve
        const buildStartTime = Date.now();
        const bundleBuffer = await this.bundleBuilder.buildBundle(
          bundleId,
          bundleSpec,
          paramValues,
          requestId
        );
        const buildDuration = Date.now() - buildStartTime;

        functions.logger.info(
          `[${requestId}] Bundle built in ${buildDuration}ms, size: ${bundleBuffer.length} bytes`
        );

        // Clear the keep-alive timeout before serving bundle
        const timeoutToClean = keepAliveTimeout;
        clearTimeout(timeoutToClean);
        return this.serveBundle(
          bundleBuffer,
          bundleId,
          requestId,
          res,
          canGzip,
          requestStartTime,
          streamsToCleanup,
          bundleSpec, // Pass bundleSpec
          paramValues, // Pass paramValues
          cacheKey // Pass cacheKey
        );
      } catch (error) {
        // Create a cache key for cleanup
        const cacheKey = this.bundleBuilder.createCacheKey(
          bundleId,
          paramValues
        );

        // Clean up the in-progress tracker on error
        this.bundleBuilder.unregisterBuild(cacheKey);

        functions.logger.error(`[${requestId}] Error building bundle:`, error);
        res.statusCode = 500;
        res.end(`Error building bundle: ${error.message}`);

        // Use the same pattern for clearing the timeout
        const timeoutToClean = keepAliveTimeout;
        clearTimeout(timeoutToClean);
        return;
      }
    } catch (error) {
      functions.logger.error(`[${requestId}] Unhandled error:`, error);

      // Only send response if headers haven't been sent yet
      if (!res.headersSent) {
        res.statusCode = 500;
        res.end(`Internal server error: ${error.message}`);
      }

      const totalDuration = Date.now() - requestStartTime;
      functions.logger.error(
        `[${requestId}] Request failed in ${totalDuration}ms`
      );

      // Clean up any streams that might still be open
      streamsToCleanup.forEach((stream) => {
        try {
          if (stream && !stream.destroyed) {
            stream.destroy?.();
          }
        } catch (err) {
          functions.logger.error(
            `[${requestId}] Error destroying stream: ${err}`
          );
        }
      });

      // Use the same pattern for clearing the timeout
      const timeoutToCleanFinal = keepAliveTimeout;
      clearTimeout(timeoutToCleanFinal);
    }
  }

  private async serveBundle(
    bundleBuffer: Buffer,
    bundleId: string,
    requestId: string,
    res: FunctionResponse,
    canGzip: boolean,
    requestStartTime: number,
    streamsToCleanup: Array<NodeJS.ReadableStream & NodeStream>,
    bundleSpec?: any,
    paramValues?: { [key: string]: any },
    cacheKey?: string
  ): Promise<void> {
    // Save to file cache if enabled - in parallel, don't wait for it to complete
    if (
      bundleSpec &&
      bundleSpec.fileCache &&
      typeof bundleSpec.fileCache === "number" &&
      paramValues
    ) {
      // Don't await this - it will run in parallel with sending the response
      const cacheWriteStartTime = Date.now();
      this.storageService
        .saveBundle(bundleId, paramValues, bundleBuffer, requestId)
        .then((success) => {
          const cacheWriteDuration = Date.now() - cacheWriteStartTime;
          if (success) {
            functions.logger.info(
              `[${requestId}] Bundle saved to cache successfully in ${cacheWriteDuration}ms`
            );
          } else {
            functions.logger.warn(
              `[${requestId}] Failed to save bundle to cache after ${cacheWriteDuration}ms`
            );
          }
          // Clean up the in-progress tracker
          if (cacheKey) {
            this.bundleBuilder.unregisterBuild(cacheKey);
          }
        })
        .catch((error) => {
          const cacheWriteDuration = Date.now() - cacheWriteStartTime;
          functions.logger.error(
            `[${requestId}] Failed to save bundle to storage after ${cacheWriteDuration}ms:`,
            error
          );
          // Clean up the in-progress tracker even on error
          if (cacheKey) {
            this.bundleBuilder.unregisterBuild(cacheKey);
          }
        });
    } else {
      // Clean up the in-progress tracker if we're not caching
      if (cacheKey) {
        this.bundleBuilder.unregisterBuild(cacheKey);
      }
    }

    // SEND THE RESPONSE ----

    // Apply compression if needed
    if (canGzip) {
      functions.logger.debug(
        `[${requestId}] Compressing freshly built bundle for client`
      );

      // Check if it's a large bundle and handle differently
      const isLargeFile = bundleBuffer.length > 5 * 1024 * 1024; // 5MB threshold

      if (isLargeFile) {
        functions.logger.debug(
          `[${requestId}] Large file detected (${bundleBuffer.length} bytes), using streaming compression`
        );

        // Create gzip stream BEFORE any data is written
        const gzip = createGzip({ level: 6 }); // Lower level for better streaming performance

        // Set headers BEFORE any data is written
        res.setHeader("content-encoding", "gzip");

        // Track for cleanup
        streamsToCleanup.push(
          gzip as unknown as NodeJS.ReadableStream & NodeStream
        );

        const sourceStream = Readable.from(bundleBuffer);
        // Track for cleanup
        streamsToCleanup.push(
          sourceStream as unknown as NodeJS.ReadableStream & NodeStream
        );

        // We're doing compression on-the-fly, so we can't predict the final Content-Length
        // Do NOT set Content-Length header for compressed streaming responses

        // IMPORTANT: Create a Promise that resolves when the stream completes or errors
        const streamStartTime = Date.now();
        return new Promise<void>((resolve, reject) => {
          sourceStream
            .on("error", (err) => {
              functions.logger.error(
                `[${requestId}] Error in source stream: ${err}`
              );
              // Destroy streams to clean up resources
              if (!(sourceStream as NodeStream).destroyed)
                (sourceStream as NodeStream).destroy?.();
              if (!(gzip as NodeStream).destroyed)
                (gzip as NodeStream).destroy?.();
              reject(err);
            })
            .pipe(gzip)
            .on("error", (err) => {
              functions.logger.error(
                `[${requestId}] Error in gzip stream: ${err}`
              );
              // Destroy streams to clean up resources
              if (!(sourceStream as NodeStream).destroyed)
                (sourceStream as NodeStream).destroy?.();
              if (!(gzip as NodeStream).destroyed)
                (gzip as NodeStream).destroy?.();
              reject(err);
            })
            .pipe(res)
            .on("finish", () => {
              const streamDuration = Date.now() - streamStartTime;
              functions.logger.debug(
                `[${requestId}] Stream finished successfully in ${streamDuration}ms`
              );
              // Destroy streams to clean up resources
              if (!(sourceStream as NodeStream).destroyed)
                (sourceStream as NodeStream).destroy?.();
              if (!(gzip as NodeStream).destroyed)
                (gzip as NodeStream).destroy?.();
              const totalDuration = Date.now() - requestStartTime;
              functions.logger.info(
                `[${requestId}] Total fresh build response time: ${totalDuration}ms (stream with compression: ${streamDuration}ms)`
              );
              resolve();
            })
            .on("error", (err) => {
              functions.logger.error(
                `[${requestId}] Error in response stream: ${err}`
              );
              // Destroy streams to clean up resources
              if (!(sourceStream as NodeStream).destroyed)
                (sourceStream as NodeStream).destroy?.();
              if (!(gzip as NodeStream).destroyed)
                (gzip as NodeStream).destroy?.();
              reject(err);
            });

          // Also handle response close/end events
          res.on("close", () => {
            functions.logger.debug(`[${requestId}] Response closed`);
            // Destroy streams to clean up resources
            if (!(sourceStream as NodeStream).destroyed)
              (sourceStream as NodeStream).destroy?.();
            if (!(gzip as NodeStream).destroyed)
              (gzip as NodeStream).destroy?.();
            resolve();
          });
        });
      } else {
        // For smaller files, use higher compression
        // Create gzip stream BEFORE any data is written
        const gzip = createGzip({ level: 9 });

        // Set headers BEFORE any data is written
        res.setHeader("content-encoding", "gzip");

        // Track for cleanup
        streamsToCleanup.push(
          gzip as unknown as NodeJS.ReadableStream & NodeStream
        );

        const sourceStream = Readable.from(bundleBuffer);
        // Track for cleanup
        streamsToCleanup.push(
          sourceStream as unknown as NodeJS.ReadableStream & NodeStream
        );

        // We're doing compression on-the-fly, so we can't predict the final Content-Length
        // Do NOT set Content-Length header for compressed streaming responses

        // IMPORTANT: Create a Promise that resolves when the stream completes or errors
        const streamStartTime = Date.now();
        return new Promise<void>((resolve, reject) => {
          sourceStream
            .on("error", (err) => {
              functions.logger.error(
                `[${requestId}] Error in source stream: ${err}`
              );
              // Destroy streams to clean up resources
              if (!(sourceStream as NodeStream).destroyed)
                (sourceStream as NodeStream).destroy?.();
              if (!(gzip as NodeStream).destroyed)
                (gzip as NodeStream).destroy?.();
              reject(err);
            })
            .pipe(gzip)
            .on("error", (err) => {
              functions.logger.error(
                `[${requestId}] Error in gzip stream: ${err}`
              );
              // Destroy streams to clean up resources
              if (!(sourceStream as NodeStream).destroyed)
                (sourceStream as NodeStream).destroy?.();
              if (!(gzip as NodeStream).destroyed)
                (gzip as NodeStream).destroy?.();
              reject(err);
            })
            .pipe(res)
            .on("finish", () => {
              const streamDuration = Date.now() - streamStartTime;
              functions.logger.debug(
                `[${requestId}] Stream finished successfully in ${streamDuration}ms`
              );
              // Destroy streams to clean up resources
              if (!(sourceStream as NodeStream).destroyed)
                (sourceStream as NodeStream).destroy?.();
              if (!(gzip as NodeStream).destroyed)
                (gzip as NodeStream).destroy?.();
              const totalDuration = Date.now() - requestStartTime;
              functions.logger.info(
                `[${requestId}] Total fresh build response time: ${totalDuration}ms (stream with compression: ${streamDuration}ms)`
              );
              resolve();
            })
            .on("error", (err) => {
              functions.logger.error(
                `[${requestId}] Error in response stream: ${err}`
              );
              // Destroy streams to clean up resources
              if (!(sourceStream as NodeStream).destroyed)
                (sourceStream as NodeStream).destroy?.();
              if (!(gzip as NodeStream).destroyed)
                (gzip as NodeStream).destroy?.();
              reject(err);
            });

          // Also handle response close/end events
          res.on("close", () => {
            functions.logger.debug(`[${requestId}] Response closed`);
            // Destroy streams to clean up resources
            if (!(sourceStream as NodeStream).destroyed)
              (sourceStream as NodeStream).destroy?.();
            if (!(gzip as NodeStream).destroyed)
              (gzip as NodeStream).destroy?.();
            resolve();
          });
        });
      }
    } else {
      functions.logger.debug(
        `[${requestId}] Sending uncompressed freshly built bundle`
      );

      const sourceStream = Readable.from(bundleBuffer);
      // Track for cleanup
      streamsToCleanup.push(
        sourceStream as unknown as NodeJS.ReadableStream & NodeStream
      );

      // Set Content-Length header for uncompressed content
      res.setHeader("content-length", bundleBuffer.length.toString());

      // IMPORTANT: Create a Promise that resolves when the stream completes or errors
      const streamStartTime = Date.now();
      return new Promise<void>((resolve, reject) => {
        sourceStream
          .on("error", (err) => {
            functions.logger.error(`[${requestId}] Error in stream: ${err}`);
            // Destroy stream to clean up resources
            if (!(sourceStream as NodeStream).destroyed)
              (sourceStream as NodeStream).destroy?.();
            reject(err);
          })
          .pipe(res)
          .on("finish", () => {
            const streamDuration = Date.now() - streamStartTime;
            functions.logger.debug(
              `[${requestId}] Stream finished successfully in ${streamDuration}ms`
            );
            // Destroy stream to clean up resources
            if (!(sourceStream as NodeStream).destroyed)
              (sourceStream as NodeStream).destroy?.();
            const totalDuration = Date.now() - requestStartTime;
            functions.logger.info(
              `[${requestId}] Total fresh build response time: ${totalDuration}ms (stream: ${streamDuration}ms)`
            );
            resolve();
          })
          .on("error", (err) => {
            functions.logger.error(
              `[${requestId}] Error in response stream: ${err}`
            );
            // Destroy stream to clean up resources
            if (!(sourceStream as NodeStream).destroyed)
              (sourceStream as NodeStream).destroy?.();
            reject(err);
          });

        // Also handle response close/end events
        res.on("close", () => {
          functions.logger.debug(`[${requestId}] Response closed`);
          // Destroy stream to clean up resources
          if (!(sourceStream as NodeStream).destroyed)
            (sourceStream as NodeStream).destroy?.();
          resolve();
        });
      });
    }
  }
}
