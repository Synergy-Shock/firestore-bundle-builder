import { firestore } from "firebase-admin";
import {
  BundleBuilder,
  Firestore,
  Query,
  Timestamp,
  WhereFilterOp,
} from "@google-cloud/firestore";

/**
 * Specification of a condition associated to a Firestore query.
 */
export interface QueryConditionSpec {
  where?: [string, WhereFilterOp, any];
  orderBy?: [string, ("asc" | "desc")?];
  limit?: unknown;
  limitToLast?: unknown;
  offset?: unknown;
  startAt?: unknown;
  startAfter?: unknown;
  endAt?: unknown;
  endBefore?: unknown;
}

export interface ParamsSpec {
  [name: string]: ParamSpec;
}

export interface ParamSpec {
  type?:
    | "string"
    | "integer"
    | "float"
    | "boolean"
    | "string-array"
    | "integer-array"
    | "float-array";
  required?: boolean;
}

export interface ParamValues {
  [key: string]: any;
}

export interface QuerySpec {
  collection: string;
  collectionGroupQuery?: boolean;
  conditions?: QueryConditionSpec[];
}

/**
 * Returns the parameterized result for the given `value`.
 *
 * If `value` is a parameter, and it could find proper parameter specification
 * and value for the parameter, it will return the parameter value.
 *
 * Otherwise, it simply return the `value` itself.
 *
 * If `value` is a parameter, the spec's `required` is true and it cannot find
 * the value, it will throw an error.
 */
export function parameterize(
  value: any,
  params: ParamsSpec,
  paramValues: { [key: string]: any }
): any {
  if (typeof value !== "string" || !value.startsWith("$")) {
    return value;
  }

  for (const p in params) {
    if (value === "$" + p) {
      const pOpts = params[p];
      if (pOpts.required && typeof paramValues[p] === "undefined") {
        throw new Error(`Required param '${p}' was missing.`);
      }

      switch (pOpts.type || "string") {
        case "integer":
          return parseInt(paramValues[p], 10);
        case "float":
          return parseFloat(paramValues[p]);
        case "boolean":
          return paramValues[p] === "true";
        case "integer-array":
          return (paramValues[p] as Array<string>).map((s) => parseInt(s));
        case "float-array":
          return (paramValues[p] as Array<string>).map((s) => parseFloat(s));
        case "string":
          return paramValues[p];
        case "string-array":
          return paramValues[p];
      }
    }
  }

  return value;
}

export function parameterizePath(
  path: string,
  params: ParamsSpec,
  paramValues: { [key: string]: any }
): string {
  return path
    .split("/")
    .map((part) => parameterize(part, params, paramValues))
    .join("/");
}

export interface BundleSpec {
  /**
   * Full path of documents to add to the bundle.
   */
  docs?: string[];
  /**
   * Queries and associated names to add to the bundle.
   */
  queries?: {
    [queryName: string]: QuerySpec;
  };
  /**
   * Parameter values to build the bundle.
   */
  params?: ParamsSpec;
  /**
   * How long to keep the bundle in client's cache, in seconds.
   *
   * Leaving it undefined will disable client side cache.
   */
  clientCache?: number;
  /**
   * How long to keep the bundle in Hosting's CDN cache, in seconds.
   *
   * Leaving it undefined will disable server side cache.
   */
  serverCache?: number;
  /**
   * If specified, the built bundle will be saved to GCS, and keep alive for
   * the specified number of seconds.
   */
  fileCache?: number;
}

/**
 * Builds a Firestore `BundleBuilder`, for the given bundle ID and associated
 * bundle spec, along with parameter values passed by the client via http request's
 * query parameters.
 */
export async function build(
  db: Firestore,
  bundleId: string,
  bundleSpec: BundleSpec,
  paramValues: { [key: string]: any },
  requestId?: string // Make optional to maintain backward compatibility
): Promise<BundleBuilder> {
  const buildStartTime = Date.now();

  // Use a logging prefix if requestId is provided
  const logPrefix = requestId ? `[${requestId}]` : "";

  if (logPrefix) {
    console.debug(`${logPrefix} Starting bundle build for ${bundleId}`);
  }

  const bundle = db.bundle(bundleId);
  const promises: Promise<void>[] = [];

  // Process individual documents
  const docsStartTime = Date.now();
  const docs = bundleSpec.docs || [];

  if (logPrefix && docs.length > 0) {
    console.debug(`${logPrefix} Adding ${docs.length} documents to bundle`);
  }

  for (const docName of docs) {
    const pathStartTime = Date.now();
    const resolvedDocName = parameterizePath(
      docName,
      bundleSpec.params || {},
      paramValues
    );
    const pathResolveTime = Date.now() - pathStartTime;

    if (logPrefix) {
      console.debug(
        `${logPrefix} bundle.add [doc]: ${resolvedDocName} (path resolution: ${pathResolveTime}ms)`
      );
    } else {
      console.debug("bundle.add [doc]:", resolvedDocName);
    }

    const docPromise = db
      .doc(resolvedDocName)
      .get()
      .then((snap) => {
        const docGetTime = Date.now() - pathStartTime;
        if (logPrefix) {
          console.debug(
            `${logPrefix} Retrieved document ${resolvedDocName} in ${docGetTime}ms`
          );
        }
        bundle.add(snap);
      });

    promises.push(docPromise);
  }

  const docsProcessTime = Date.now() - docsStartTime;
  if (logPrefix) {
    console.debug(
      `${logPrefix} Document processing setup took ${docsProcessTime}ms`
    );
  }

  // Process queries
  const queriesStartTime = Date.now();
  const queries = bundleSpec.queries || {};
  const queryCount = Object.keys(queries).length;

  if (logPrefix && queryCount > 0) {
    console.debug(`${logPrefix} Adding ${queryCount} queries to bundle`);
  }

  for (const qName in queries) {
    const queryBuildStartTime = Date.now();
    if (logPrefix) {
      console.debug(`${logPrefix} bundle.add [query]: ${qName}`);
    } else {
      console.debug("bundle.add [query]:", qName);
    }

    const query = buildQuery(
      db,
      queries[qName],
      bundleSpec.params || {},
      paramValues
    );
    const queryBuildTime = Date.now() - queryBuildStartTime;

    if (logPrefix) {
      console.debug(`${logPrefix} Query ${qName} built in ${queryBuildTime}ms`);
    }

    const queryPromise = query.get().then((snap) => {
      const queryGetTime = Date.now() - queryBuildStartTime;
      if (logPrefix) {
        console.debug(
          `${logPrefix} Query ${qName} executed in ${queryGetTime}ms, returned ${snap.size} documents`
        );
      }
      bundle.add(qName, snap);
    });

    promises.push(queryPromise);
  }

  const queriesProcessTime = Date.now() - queriesStartTime;
  if (logPrefix) {
    console.debug(
      `${logPrefix} Query processing setup took ${queriesProcessTime}ms`
    );
  }

  // Wait for all promises to resolve
  const waitStartTime = Date.now();
  if (logPrefix) {
    console.debug(
      `${logPrefix} Waiting for ${promises.length} operations to complete`
    );
  }

  await Promise.all(promises);

  const waitTime = Date.now() - waitStartTime;
  const totalBuildTime = Date.now() - buildStartTime;

  if (logPrefix) {
    console.debug(`${logPrefix} All operations completed in ${waitTime}ms`);
    console.debug(
      `${logPrefix} Total build preparation time: ${totalBuildTime}ms (docs: ${docsProcessTime}ms, queries: ${queriesProcessTime}ms, wait: ${waitTime}ms)`
    );
  }

  return bundle;
}

// Exports for testing purpose only.
export function buildQuery(
  db: Firestore,
  qSpec: QuerySpec,
  params: ParamsSpec,
  paramValues: ParamValues
): Query {
  const parameterizedPath = parameterizePath(
    qSpec.collection,
    params,
    paramValues
  );
  let result: Query = !!qSpec.collectionGroupQuery
    ? db.collectionGroup(parameterizedPath)
    : db.collection(parameterizedPath);

  (qSpec.conditions || []).forEach((c) => {
    result = handleCondition(result, c, params, paramValues);
  });

  return result;
}

function handleCondition(
  ref: firestore.Query,
  c: QueryConditionSpec,
  params: ParamsSpec,
  paramValues: { [key: string]: string }
): firestore.Query {
  if (Object.keys(c).length !== 1) {
    throw new Error(
      `Query 'conditions' may only have one key each. Found: ${JSON.stringify(
        Object.keys(c)
      )}`
    );
  }
  if (c.where) {
    console.debug(
      `.where('${parameterize(c.where[0], params, paramValues)}','${
        c.where[1]
      }','${parameterize(c.where[2], params, paramValues)}')`
    );
    let value = parameterize(c.where[2], params, paramValues);
    switch (c.where[1]) {
      case "array-contains-any":
      case "in":
      case "not-in": {
        // Since array values cannot be an array, we need to detect whether the user has specifically chosen
        // an array of values which are strings or ints.

        value = (value as string).split(",").map((value) => {
          const maybeNumber = parseFloat(value);
          if (!isNaN(maybeNumber)) {
            return maybeNumber;
          }

          if (
            (value.startsWith(`"`) && value.endsWith(`"`)) ||
            (value.startsWith(`'`) && value.endsWith(`'`))
          ) {
            // Remove first and last character
            return value.substring(1, value.length - 1);
          }

          return value;
        });
        break;
      }
    }

    return ref.where(
      parameterize(c.where[0], params, paramValues),
      c.where[1],
      value
    );
  } else if (c.orderBy) {
    return ref.orderBy(
      parameterize(c.orderBy[0], params, paramValues),
      parameterize(c.orderBy[1], params, paramValues)
    );
  } else if (c.limit) {
    return ref.limit(parameterize(c.limit, params, paramValues));
  } else if (c.limitToLast) {
    return ref.limitToLast(parameterize(c.limitToLast, params, paramValues));
  } else if (c.offset) {
    return ref.offset(parameterize(c.offset, params, paramValues));
  } else if (c.startAt) {
    return ref.startAt(parameterize(c.startAt, params, paramValues));
  } else if (c.startAfter) {
    return ref.startAfter(parameterize(c.startAfter, params, paramValues));
  } else if (c.endAt) {
    return ref.endAt(parameterize(c.endAt, params, paramValues));
  } else if (c.endBefore) {
    return ref.endBefore(parameterize(c.endBefore, params, paramValues));
  }

  return ref;
}
