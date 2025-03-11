import * as functions from "firebase-functions";
import { createGzip } from "zlib";
import { Readable } from "stream";
import { BundleSpec } from "./build_bundle";
import { filterQuery, setCacheControlHeaders } from "./utils";
import { StorageService } from "./storage";
import { BundleBuilder } from "./bundleBuilder";
import { SpecManager } from "./specManager";

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
    req: functions.Request,
    res: functions.Response
  ): Promise<void> {
    const requestId = Math.random().toString(36).substring(2, 15);
    const startTime = Date.now();
    functions.logger.info(`[${requestId}] Request started: ${req.path}`, {
      path: req.path,
      query: req.query,
      userAgent: req.get("user-agent"),
      referer: req.get("referer"),
    });

    try {
      // Set CORS headers for browser compatibility
      res.set("Access-Control-Allow-Origin", "*");
      if (req.method === "OPTIONS") {
        res.set("Access-Control-Allow-Methods", "GET");
        res.set("Access-Control-Allow-Headers", "Content-Type");
        res.set("Access-Control-Max-Age", "3600");
        res.status(204).send("");
        functions.logger.info(
          `[${requestId}] OPTIONS request completed in ${
            Date.now() - startTime
          }ms`
        );
        return;
      }

      functions.logger.debug(`[${requestId}] Request headers:`, req.headers);
      const canGzip = req.get("accept-encoding")?.includes("gzip") || false;

      // Parse bundle ID from path
      const path = req.path;
      const parts = path.split("/").filter((p) => p.length > 0);
      if (parts.length === 0) {
        functions.logger.warn(`[${requestId}] Invalid request path: ${path}`);
        res
          .status(400)
          .send("Invalid request path. Expected format: /[bundleId]");
        return;
      }

      const bundleId = parts[parts.length - 1];
      functions.logger.info(`[${requestId}] Processing bundle: ${bundleId}`);

      // Wait for bundle specifications to be loaded
      let bundleSpec = await this.specManager.getSpec(bundleId);

      // Check if bundle exists
      if (!bundleSpec) {
        functions.logger.warn(`[${requestId}] Bundle not found: ${bundleId}`);
        res.status(404).send(`Could not find bundle with ID ${bundleId}`);
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
      const cacheKey = `${bundleId}_${JSON.stringify(paramValues)}`;

      // Try to serve from file cache if enabled
      if (bundleSpec.fileCache && typeof bundleSpec.fileCache === "number") {
        functions.logger.debug(
          `[${requestId}] Checking file cache (TTL: ${bundleSpec.fileCache}s)`
        );

        try {
          const cachedBundle = await this.storageService.getBundle(
            bundleId,
            paramValues,
            bundleSpec.fileCache,
            requestId,
            canGzip
          );

          if (cachedBundle) {
            functions.logger.info(
              `[${requestId}] Serving from file cache (size: ${cachedBundle.size} bytes)`
            );

            // Set appropriate content type
            if (cachedBundle.contentType) {
              res.set("Content-Type", cachedBundle.contentType);
            }

            // Handle compression based on the state of the cached bundle
            if (cachedBundle.isCompressed) {
              // Data is already compressed, set the header
              functions.logger.debug(
                `[${requestId}] Streaming pre-compressed bundle to client`
              );
              res.set("content-encoding", "gzip");

              // Stream it directly to the response
              cachedBundle.stream.pipe(res);
            } else if (canGzip) {
              // Data is not compressed but client accepts gzip - compress it on-the-fly
              functions.logger.debug(
                `[${requestId}] Compressing cached bundle for client on-the-fly`
              );
              const gzip = createGzip({ level: 6 }); // Lower compression level for streaming
              res.set("content-encoding", "gzip");

              // Set up error handling for compression stream
              gzip.on("error", (err) => {
                functions.logger.error(
                  `[${requestId}] Error compressing stream: ${err}`
                );
                // If headers haven't been sent, we can still return an error
                if (!res.headersSent) {
                  res
                    .status(500)
                    .send(`Error processing bundle: ${err.message}`);
                }
              });

              cachedBundle.stream.pipe(gzip).pipe(res);
            } else {
              // Send uncompressed stream
              functions.logger.debug(
                `[${requestId}] Streaming uncompressed cached bundle`
              );
              cachedBundle.stream.pipe(res);
            }

            functions.logger.info(
              `[${requestId}] Response stream started from cache in ${
                Date.now() - startTime
              }ms`
            );
            return;
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

          const bundleBuffer = await inProgressBuild;

          // Apply compression if needed and send response
          if (canGzip) {
            const gzip = createGzip({ level: 9 });
            res.set("content-encoding", "gzip");
            Readable.from(bundleBuffer).pipe(gzip).pipe(res);
          } else {
            Readable.from(bundleBuffer).pipe(res);
          }

          functions.logger.info(
            `[${requestId}] Response sent from shared build in ${
              Date.now() - startTime
            }ms`
          );
          return;
        } catch (error) {
          functions.logger.error(
            `[${requestId}] Error using shared build:`,
            error
          );
          // Continue to build our own copy if shared build fails
          this.bundleBuilder.unregisterBuild(cacheKey);
        }
      }

      // Build the bundle
      let bundleBuffer: Buffer;

      try {
        // Start building and register it for other requests
        const buildPromise = this.bundleBuilder.buildBundle(
          bundleId,
          bundleSpec,
          paramValues,
          requestId
        );
        this.bundleBuilder.registerBuild(cacheKey, buildPromise);

        // Wait for the build to complete
        bundleBuffer = await buildPromise;
      } catch (error) {
        // Clean up the in-progress tracker on error
        this.bundleBuilder.unregisterBuild(cacheKey);

        functions.logger.error(`[${requestId}] Error building bundle:`, error);
        res.status(500).send(`Error building bundle: ${error.message}`);
        return;
      }

      // Save to file cache if enabled - in parallel, don't wait for it to complete
      if (bundleSpec.fileCache && typeof bundleSpec.fileCache === "number") {
        // Don't await this - it will run in parallel with sending the response
        this.storageService
          .saveBundle(bundleId, paramValues, bundleBuffer, requestId)
          .then((success) => {
            if (success) {
              functions.logger.info(
                `[${requestId}] Bundle saved to cache successfully`
              );
            } else {
              functions.logger.warn(
                `[${requestId}] Failed to save bundle to cache`
              );
            }
            // Clean up the in-progress tracker
            this.bundleBuilder.unregisterBuild(cacheKey);
          })
          .catch((error) => {
            functions.logger.error(
              `[${requestId}] Failed to save bundle to storage:`,
              error
            );
            // Clean up the in-progress tracker even on error
            this.bundleBuilder.unregisterBuild(cacheKey);
          });
      } else {
        // Clean up the in-progress tracker if we're not caching
        this.bundleBuilder.unregisterBuild(cacheKey);
      }

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
          const gzip = createGzip({ level: 6 }); // Lower level for better streaming performance

          // Setup error handler
          gzip.on("error", (err) => {
            functions.logger.error(
              `[${requestId}] Error compressing stream: ${err}`
            );
            // If headers haven't been sent, we can still return an error
            if (!res.headersSent) {
              res.status(500).send(`Error processing bundle: ${err.message}`);
            }
          });

          res.set("content-encoding", "gzip");
          Readable.from(bundleBuffer).pipe(gzip).pipe(res);
        } else {
          // For smaller files, use higher compression
          const gzip = createGzip({ level: 9 });
          res.set("content-encoding", "gzip");
          Readable.from(bundleBuffer).pipe(gzip).pipe(res);
        }
      } else {
        functions.logger.debug(
          `[${requestId}] Sending uncompressed freshly built bundle`
        );
        Readable.from(bundleBuffer).pipe(res);
      }

      functions.logger.info(
        `[${requestId}] Response sent in ${Date.now() - startTime}ms`
      );
    } catch (error) {
      functions.logger.error(`[${requestId}] Unhandled error:`, error);

      // Only send response if headers haven't been sent yet
      if (!res.headersSent) {
        res.status(500).send(`Internal server error: ${error.message}`);
      }

      functions.logger.error(
        `[${requestId}] Request failed in ${Date.now() - startTime}ms`
      );
    }
  }
}
