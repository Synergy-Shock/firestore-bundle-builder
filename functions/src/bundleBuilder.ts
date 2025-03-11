import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import { BundleSpec, build } from "./build_bundle";
import { Readable } from "stream";

export class BundleBuilder {
  private db: FirebaseFirestore.Firestore;
  private bundleBuildsInProgress: { [key: string]: Promise<Buffer> } = {};

  constructor(db: FirebaseFirestore.Firestore) {
    this.db = db;
  }

  /**
   * Checks if a bundle is already being built
   */
  isBuilding(cacheKey: string): boolean {
    return !!this.bundleBuildsInProgress[cacheKey];
  }

  /**
   * Gets a reference to an in-progress build
   */
  getInProgressBuild(cacheKey: string): Promise<Buffer> | null {
    return this.bundleBuildsInProgress[cacheKey] || null;
  }

  /**
   * Registers an in-progress build
   */
  registerBuild(cacheKey: string, buildPromise: Promise<Buffer>): void {
    this.bundleBuildsInProgress[cacheKey] = buildPromise;
  }

  /**
   * Unregisters an in-progress build
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
    requestId: string
  ): Promise<Buffer> {
    try {
      functions.logger.info(`[${requestId}] Building bundle: ${bundleId}`);
      const startTime = Date.now();

      const bundleBuilder = await build(
        this.db,
        bundleId,
        bundleSpec,
        paramValues
      );
      const bundleData = bundleBuilder.build();
      const bundleBuffer = Buffer.from(bundleData);

      const duration = Date.now() - startTime;
      functions.logger.info(
        `[${requestId}] Bundle built successfully: ${bundleId}, size: ${bundleBuffer.length} bytes, duration: ${duration}ms`
      );

      return bundleBuffer;
    } catch (error) {
      functions.logger.error(
        `[${requestId}] Error building bundle: ${bundleId}`,
        error
      );
      throw error;
    }
  }

  /**
   * Creates a readable stream from a buffer
   */
  createStream(buffer: Buffer): NodeJS.ReadableStream {
    return Readable.from(buffer);
  }
}
