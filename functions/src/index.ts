/**
 * This extension sets up a http-serving Cloud Function.
 *
 * The function reads all bundle specifications under a configured Firestore
 * collection, and serves requests for bundle files specified by the
 * specifications.
 */

import * as admin from "firebase-admin";
import { onRequest } from "firebase-functions/v2/https";
import * as functions from "firebase-functions";
import { BundleBuilder } from "./bundleBuilder";
import { SpecManager } from "./specManager";
import { StorageService } from "./storage";
import { BundleServer } from "./server";

// Configuration constants
const BUNDLESPEC_COLLECTION = process.env.BUNDLESPEC_COLLECTION || "bundles";
const BUNDLE_STORAGE_BUCKET =
  process.env.BUNDLE_STORAGE_BUCKET || "bundle-builder-files";
const STORAGE_PREFIX = process.env.STORAGE_PREFIX || "bundles";

// Initialize the Firebase Admin SDK
admin.initializeApp();
const db = admin.firestore();

// Initialize services
const specManager = new SpecManager(db, BUNDLESPEC_COLLECTION);
const storageService = new StorageService({
  bucketName: BUNDLE_STORAGE_BUCKET,
  storagePrefix: STORAGE_PREFIX,
});
const bundleBuilder = new BundleBuilder(db, storageService);

// Create the bundle server
const bundleServer = new BundleServer(
  storageService,
  bundleBuilder,
  specManager
);

// Track initialization state
let initializationPromise: Promise<boolean> | null = null;
let isServiceReady = false;

// Initialize on demand when the first request comes in
function ensureInitialized(): Promise<boolean> {
  if (initializationPromise) {
    return initializationPromise;
  }

  functions.logger.info("Starting service initialization...");

  // Create a new initialization promise
  initializationPromise = storageService
    .validateBucket()
    .then((isValid) => {
      if (!isValid) {
        functions.logger.error(
          `Storage bucket ${BUNDLE_STORAGE_BUCKET} is not valid or accessible.`
        );
        return false;
      }
      isServiceReady = true;
      functions.logger.info(
        "Bundle service initialization complete - ready to serve requests"
      );
      return true;
    })
    .catch((error) => {
      functions.logger.error("Failed to validate storage bucket", error);
      return false;
    });

  return initializationPromise;
}

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
export const serve = onRequest(
  {
    memory: "512MiB",
    timeoutSeconds: 540,
    cors: true,
  },
  async (req, res): Promise<any> => {
    const requestId = Math.random().toString(36).substring(2, 15);

    try {
      // Initialize on first request or after failure
      const initialized = await ensureInitialized();

      if (!initialized) {
        functions.logger.error(
          `[${requestId}] Service not ready - bucket validation failed`
        );
        res
          .status(503) // Service Unavailable rather than 500
          .send(
            "Service initialization failed. Please check server logs and try again later."
          );
        return;
      }

      // Service is ready, handle the request
      return bundleServer.handleRequest(req, res);
    } catch (error) {
      functions.logger.error(
        `[${requestId}] Unexpected error during initialization or request handling:`,
        error
      );
      res
        .status(500)
        .send("An unexpected error occurred. Please try again later.");
    }
  }
);
