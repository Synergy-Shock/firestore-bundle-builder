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
    const startTime = Date.now();
    functions.logger.info(`[${requestId}] Request started: ${req.path}`, {
      path: req.path,
      query: req.query,
      userAgent: req.headers["user-agent"],
      referer: req.headers["referer"],
    });

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
        functions.logger.info(
          `[${requestId}] OPTIONS request completed in ${
            Date.now() - startTime
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
        return;
      }

      const bundleId = parts[parts.length - 1];
      functions.logger.info(`[${requestId}] Processing bundle: ${bundleId}`);

      // Wait for bundle specifications to be loaded
      let bundleSpec = await this.specManager.getSpec(bundleId);

      // Check if bundle exists
      if (!bundleSpec) {
        functions.logger.warn(`[${requestId}] Bundle not found: ${bundleId}`);
        res.statusCode = 404;
        res.end(`Could not find bundle with ID ${bundleId}`);
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

            // Track the stream for cleanup
            streamsToCleanup.push(cachedBundle.stream);

            // Set appropriate content type
            if (cachedBundle.contentType) {
              res.setHeader("Content-Type", cachedBundle.contentType);
            }

            // Handle compression based on the state of the cached bundle
            if (cachedBundle.isCompressed) {
              // Data is already compressed, set the header
              functions.logger.debug(
                `[${requestId}] Streaming pre-compressed bundle to client`
              );
              res.setHeader("content-encoding", "gzip");

              // Set Content-Length header only if we're sure about the size
              // For pre-compressed content from storage, we do know the exact size
              res.setHeader("Content-Length", cachedBundle.size.toString());

              // IMPORTANT: Create a Promise that resolves when the stream completes or errors
              // This ensures the function doesn't exit until streaming is done
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
                    functions.logger.debug(
                      `[${requestId}] Stream finished successfully`
                    );
                    // Destroy stream to clean up resources
                    if (!(cachedBundle.stream as NodeStream).destroyed) {
                      (cachedBundle.stream as NodeStream).destroy?.();
                    }
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
            } else if (canGzip) {
              // Data is not compressed but client accepts gzip - compress it on-the-fly
              functions.logger.debug(
                `[${requestId}] Compressing cached bundle for client on-the-fly`
              );
              const gzip = createGzip({ level: 6 }); // Lower compression level for streaming

              // Track for cleanup
              streamsToCleanup.push(
                gzip as unknown as NodeJS.ReadableStream & NodeStream
              );

              res.setHeader("content-encoding", "gzip");

              // We're doing compression on-the-fly, so we can't predict the final Content-Length
              // Do NOT set Content-Length header for compressed streaming responses

              // IMPORTANT: Create a Promise that resolves when the stream completes or errors
              return new Promise<void>((resolve, reject) => {
                cachedBundle.stream
                  .on("error", (err) => {
                    functions.logger.error(
                      `[${requestId}] Error in source stream: ${err}`
                    );
                    // Destroy streams to clean up resources
                    if (!(cachedBundle.stream as NodeStream).destroyed) {
                      (cachedBundle.stream as NodeStream).destroy?.();
                    }
                    if (!(gzip as NodeStream).destroyed) {
                      (gzip as NodeStream).destroy?.();
                    }
                    reject(err);
                  })
                  .pipe(gzip)
                  .on("error", (err) => {
                    functions.logger.error(
                      `[${requestId}] Error in gzip stream: ${err}`
                    );
                    // Destroy streams to clean up resources
                    if (!(cachedBundle.stream as NodeStream).destroyed) {
                      (cachedBundle.stream as NodeStream).destroy?.();
                    }
                    if (!(gzip as NodeStream).destroyed) {
                      (gzip as NodeStream).destroy?.();
                    }
                    reject(err);
                  })
                  .pipe(res)
                  .on("finish", () => {
                    functions.logger.debug(
                      `[${requestId}] Stream finished successfully`
                    );
                    // Destroy streams to clean up resources
                    if (!(cachedBundle.stream as NodeStream).destroyed) {
                      (cachedBundle.stream as NodeStream).destroy?.();
                    }
                    if (!(gzip as NodeStream).destroyed) {
                      (gzip as NodeStream).destroy?.();
                    }
                    resolve();
                  })
                  .on("error", (err) => {
                    functions.logger.error(
                      `[${requestId}] Error in response stream: ${err}`
                    );
                    // Destroy streams to clean up resources
                    if (!(cachedBundle.stream as NodeStream).destroyed) {
                      (cachedBundle.stream as NodeStream).destroy?.();
                    }
                    if (!(gzip as NodeStream).destroyed) {
                      (gzip as NodeStream).destroy?.();
                    }
                    reject(err);
                  });

                // Also handle response close/end events
                res.on("close", () => {
                  functions.logger.debug(`[${requestId}] Response closed`);
                  // Destroy streams to clean up resources
                  if (!(cachedBundle.stream as NodeStream).destroyed) {
                    (cachedBundle.stream as NodeStream).destroy?.();
                  }
                  if (!(gzip as NodeStream).destroyed) {
                    (gzip as NodeStream).destroy?.();
                  }
                  resolve();
                });
              });
            } else {
              // Send uncompressed stream
              functions.logger.debug(
                `[${requestId}] Streaming uncompressed cached bundle`
              );

              // Set Content-Length header for uncompressed content
              res.setHeader("Content-Length", cachedBundle.size.toString());

              // IMPORTANT: Create a Promise that resolves when the stream completes or errors
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
                    functions.logger.debug(
                      `[${requestId}] Stream finished successfully`
                    );
                    // Destroy stream to clean up resources
                    if (!(cachedBundle.stream as NodeStream).destroyed) {
                      (cachedBundle.stream as NodeStream).destroy?.();
                    }
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
            // Track for cleanup
            streamsToCleanup.push(
              gzip as unknown as NodeJS.ReadableStream & NodeStream
            );

            const sourceStream = Readable.from(bundleBuffer);
            // Track for cleanup
            streamsToCleanup.push(
              sourceStream as unknown as NodeJS.ReadableStream & NodeStream
            );

            res.setHeader("content-encoding", "gzip");

            return new Promise<void>((resolve, reject) => {
              sourceStream
                .on("error", (err) => {
                  functions.logger.error(
                    `[${requestId}] Error in stream: ${err}`
                  );
                  // Destroy streams
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
                  // Destroy streams
                  if (!(sourceStream as NodeStream).destroyed)
                    (sourceStream as NodeStream).destroy?.();
                  if (!(gzip as NodeStream).destroyed)
                    (gzip as NodeStream).destroy?.();
                  reject(err);
                })
                .pipe(res)
                .on("finish", () => {
                  functions.logger.debug(
                    `[${requestId}] Stream finished successfully`
                  );
                  // Destroy streams
                  if (!(sourceStream as NodeStream).destroyed)
                    (sourceStream as NodeStream).destroy?.();
                  if (!(gzip as NodeStream).destroyed)
                    (gzip as NodeStream).destroy?.();
                  resolve();
                })
                .on("error", (err) => {
                  functions.logger.error(
                    `[${requestId}] Error in response stream: ${err}`
                  );
                  // Destroy streams
                  if (!(sourceStream as NodeStream).destroyed)
                    (sourceStream as NodeStream).destroy?.();
                  if (!(gzip as NodeStream).destroyed)
                    (gzip as NodeStream).destroy?.();
                  reject(err);
                });

              // Also handle response close/end events
              res.on("close", () => {
                functions.logger.debug(`[${requestId}] Response closed`);
                // Destroy streams
                if (!(sourceStream as NodeStream).destroyed)
                  (sourceStream as NodeStream).destroy?.();
                if (!(gzip as NodeStream).destroyed)
                  (gzip as NodeStream).destroy?.();
                resolve();
              });
            });
          } else {
            const sourceStream = Readable.from(bundleBuffer);
            // Track for cleanup
            streamsToCleanup.push(
              sourceStream as unknown as NodeJS.ReadableStream & NodeStream
            );

            // Set Content-Length for uncompressed content
            res.setHeader("Content-Length", bundleBuffer.length.toString());

            return new Promise<void>((resolve, reject) => {
              sourceStream
                .on("error", (err) => {
                  functions.logger.error(
                    `[${requestId}] Error in stream: ${err}`
                  );
                  if (!(sourceStream as NodeStream).destroyed)
                    (sourceStream as NodeStream).destroy?.();
                  reject(err);
                })
                .pipe(res)
                .on("finish", () => {
                  functions.logger.debug(
                    `[${requestId}] Stream finished successfully`
                  );
                  if (!(sourceStream as NodeStream).destroyed)
                    (sourceStream as NodeStream).destroy?.();
                  resolve();
                })
                .on("error", (err) => {
                  functions.logger.error(
                    `[${requestId}] Error in response stream: ${err}`
                  );
                  if (!(sourceStream as NodeStream).destroyed)
                    (sourceStream as NodeStream).destroy?.();
                  reject(err);
                });

              // Also handle response close/end events
              res.on("close", () => {
                functions.logger.debug(`[${requestId}] Response closed`);
                if (!(sourceStream as NodeStream).destroyed)
                  (sourceStream as NodeStream).destroy?.();
                resolve();
              });
            });
          }
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
        res.statusCode = 500;
        res.end(`Error building bundle: ${error.message}`);
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
          // Track for cleanup
          streamsToCleanup.push(
            gzip as unknown as NodeJS.ReadableStream & NodeStream
          );

          const sourceStream = Readable.from(bundleBuffer);
          // Track for cleanup
          streamsToCleanup.push(
            sourceStream as unknown as NodeJS.ReadableStream & NodeStream
          );

          res.setHeader("content-encoding", "gzip");

          // We're doing compression on-the-fly, so we can't predict the final Content-Length
          // Do NOT set Content-Length header for compressed streaming responses

          // IMPORTANT: Create a Promise that resolves when the stream completes or errors
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
                functions.logger.debug(
                  `[${requestId}] Stream finished successfully`
                );
                // Destroy streams to clean up resources
                if (!(sourceStream as NodeStream).destroyed)
                  (sourceStream as NodeStream).destroy?.();
                if (!(gzip as NodeStream).destroyed)
                  (gzip as NodeStream).destroy?.();
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
          const gzip = createGzip({ level: 9 });
          // Track for cleanup
          streamsToCleanup.push(
            gzip as unknown as NodeJS.ReadableStream & NodeStream
          );

          const sourceStream = Readable.from(bundleBuffer);
          // Track for cleanup
          streamsToCleanup.push(
            sourceStream as unknown as NodeJS.ReadableStream & NodeStream
          );

          res.setHeader("content-encoding", "gzip");

          // We're doing compression on-the-fly, so we can't predict the final Content-Length
          // Do NOT set Content-Length header for compressed streaming responses

          // IMPORTANT: Create a Promise that resolves when the stream completes or errors
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
                functions.logger.debug(
                  `[${requestId}] Stream finished successfully`
                );
                // Destroy streams to clean up resources
                if (!(sourceStream as NodeStream).destroyed)
                  (sourceStream as NodeStream).destroy?.();
                if (!(gzip as NodeStream).destroyed)
                  (gzip as NodeStream).destroy?.();
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
        res.setHeader("Content-Length", bundleBuffer.length.toString());

        // IMPORTANT: Create a Promise that resolves when the stream completes or errors
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
              functions.logger.debug(
                `[${requestId}] Stream finished successfully`
              );
              // Destroy stream to clean up resources
              if (!(sourceStream as NodeStream).destroyed)
                (sourceStream as NodeStream).destroy?.();
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

      functions.logger.info(
        `[${requestId}] Response sent in ${Date.now() - startTime}ms`
      );
    } catch (error) {
      functions.logger.error(`[${requestId}] Unhandled error:`, error);

      // Only send response if headers haven't been sent yet
      if (!res.headersSent) {
        res.statusCode = 500;
        res.end(`Internal server error: ${error.message}`);
      }

      functions.logger.error(
        `[${requestId}] Request failed in ${Date.now() - startTime}ms`
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
    }
  }
}
