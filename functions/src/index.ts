/**
 * This extension sets up a http-serving Cloud Function.
 *
 * The function reads all bundle specifications under a configured Firestore
 * collection, and serves requests for bundle files specified by the
 * specifications.
 */

import * as admin from "firebase-admin";
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
const bundleBuilder = new BundleBuilder(db);
const storageService = new StorageService({
  bucketName: BUNDLE_STORAGE_BUCKET,
  storagePrefix: STORAGE_PREFIX,
});

// Flag to track if the service is ready
let isServiceReady = false;

// Verify the bucket is accessible on startup
storageService
  .validateBucket()
  .then((isValid) => {
    if (!isValid) {
      functions.logger.error(
        `Storage bucket ${BUNDLE_STORAGE_BUCKET} is not valid or accessible.`
      );
      return;
    }
    isServiceReady = true;
    functions.logger.info(
      "Bundle service initialization complete - ready to serve requests"
    );
  })
  .catch((error) => {
    functions.logger.error("Failed to validate storage bucket", error);
  });

// Create the bundle server
const bundleServer = new BundleServer(
  storageService,
  bundleBuilder,
  specManager
);

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
    // Check if service is ready before handling the request
    if (!isServiceReady) {
      functions.logger.error(
        "Service not ready - storage bucket validation failed"
      );
      res
        .status(500)
        .send("Service not initialized properly. Please check server logs.");
      return;
    }

    return bundleServer.handleRequest(req, res);
  }
);
