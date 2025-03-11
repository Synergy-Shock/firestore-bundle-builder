import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import { BundleSpec } from "./build_bundle";

export class SpecManager {
  private specs: { [name: string]: { data: BundleSpec; timestamp: number } } =
    {};
  private db: FirebaseFirestore.Firestore;
  private collectionName: string;
  private cacheTTLMs: number;

  constructor(
    db: FirebaseFirestore.Firestore,
    collectionName: string,
    cacheTTLMs = 60000
  ) {
    this.db = db;
    this.collectionName = collectionName;
    this.cacheTTLMs = cacheTTLMs; // Default 1 minute cache TTL
  }

  /**
   * Fetches a bundle spec from Firestore or from cache if it's still valid
   */
  async getSpec(name: string): Promise<BundleSpec | null> {
    // Check if we have a cached version that's still fresh
    const cached = this.specs[name];
    const now = Date.now();

    if (cached && now - cached.timestamp < this.cacheTTLMs) {
      functions.logger.debug(
        `Using cached bundle spec for ${name}, age: ${
          (now - cached.timestamp) / 1000
        }s`
      );
      return cached.data;
    }

    // Fetch from Firestore if not in cache or cache is stale
    try {
      const docRef = this.db.collection(this.collectionName).doc(name);
      const docSnap = await docRef.get();

      if (!docSnap.exists) {
        functions.logger.debug(`Bundle spec ${name} not found in Firestore`);
        return null;
      }

      const data = docSnap.data() as BundleSpec;

      // Update cache
      this.specs[name] = {
        data,
        timestamp: now,
      };

      functions.logger.debug(
        `Fetched fresh bundle spec for ${name} from Firestore`
      );
      return data;
    } catch (error) {
      functions.logger.error(`Error fetching bundle spec ${name}:`, error);
      throw error;
    }
  }

  /**
   * Clears the spec cache
   */
  clearCache(): void {
    this.specs = {};
    functions.logger.debug("Bundle spec cache cleared");
  }
}
