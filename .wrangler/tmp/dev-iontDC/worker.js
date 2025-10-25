var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// worker/mongo.js
function buildMongoClient(env) {
  const apiUrl = env.MONGODB_DATA_API_URL;
  const apiKey = env.MONGODB_DATA_API_KEY;
  const dataSource = env.MONGODB_DATA_API_DATA_SOURCE || env.MONGODB_DATA_API_CLUSTER;
  const database = env.MONGODB_DB || "shopify_preorder";
  if (!apiUrl || !apiKey || !dataSource) {
    throw new Error("Missing MongoDB Data API configuration");
  }
  async function call(action, payload) {
    const res = await fetch(`${apiUrl}/action/${action}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "api-key": apiKey
      },
      body: JSON.stringify({ dataSource, database, ...payload })
    });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Mongo Data API ${action} failed: ${res.status} ${text}`);
    }
    return await res.json();
  }
  __name(call, "call");
  return {
    insertOne: /* @__PURE__ */ __name((collection, document) => call("insertOne", { collection, document }), "insertOne"),
    insertMany: /* @__PURE__ */ __name((collection, documents) => call("insertMany", { collection, documents }), "insertMany"),
    updateOne: /* @__PURE__ */ __name((collection, filter, update, upsert = false) => call("updateOne", { collection, filter, update, upsert }), "updateOne")
  };
}
__name(buildMongoClient, "buildMongoClient");

// worker/worker.js
async function shopifyGraphQL(env, query, variables) {
  const apiVersion = env.SHOPIFY_API_VERSION || "2024-10";
  const store = env.SHOPIFY_STORE_DOMAIN.startsWith("http") ? env.SHOPIFY_STORE_DOMAIN : `https://${env.SHOPIFY_STORE_DOMAIN}`;
  const url = `${store.replace(/\/$/, "")}/admin/api/${apiVersion}/graphql.json`;
  const res = await fetch(url, { method: "POST", headers: { "X-Shopify-Access-Token": env.SHOPIFY_ADMIN_ACCESS_TOKEN, "Content-Type": "application/json" }, body: JSON.stringify({ query, variables }) });
  const data = await res.json();
  if (data.errors) throw new Error(JSON.stringify(data.errors));
  return data.data;
}
__name(shopifyGraphQL, "shopifyGraphQL");
function normalizeHeader(name) {
  const base = String(name).replace(/^\uFEFF/, "").trim().toLowerCase();
  const key = base.replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
  if (["variant_sku", "sku", "variantid", "variant_id_sku"].includes(key)) return "sku";
  if (["handle", "product_handle", "handle_url"].includes(key)) return "handle";
  if (["is_preorder", "ispreorder", "preorder", "is_pre_order"].includes(key)) return "is_preorder";
  if (["preorder_limit", "pre_order_limit", "limit"].includes(key)) return "preorder_limit";
  if (["preorder_message", "pre_order_message", "message"].includes(key)) return "preorder_message";
  return key;
}
__name(normalizeHeader, "normalizeHeader");
function parseCsv(text) {
  text = text.replace(/^\uFEFF/, "");
  const lines = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (c === '"') {
      if (inQ && text[i + 1] === '"') {
        cur += '"';
        i++;
      } else {
        inQ = !inQ;
      }
    } else if (c === "\n" || c === "\r") {
      if (inQ) {
        cur += c;
      } else {
        lines.push(cur);
        cur = "";
        if (c === "\r" && text[i + 1] === "\n") {
          i++;
        }
      }
    } else {
      cur += c;
    }
  }
  if (cur.length) lines.push(cur);
  if (!lines.length) return { rows: [], stats: { totalRows: 0, skippedRows: 0 } };
  const headers = lines[0].split(",").map(normalizeHeader);
  const rows = [];
  let skipped = 0;
  for (let li = 1; li < lines.length; li++) {
    const rowLine = lines[li];
    if (!rowLine.trim()) {
      skipped++;
      continue;
    }
    const cols = [];
    let v = "";
    inQ = false;
    for (let i = 0; i < rowLine.length; i++) {
      const c = rowLine[i];
      if (c === '"') {
        if (inQ && rowLine[i + 1] === '"') {
          v += '"';
          i++;
        } else {
          inQ = !inQ;
        }
      } else if (c === "," && !inQ) {
        cols.push(v);
        v = "";
      } else {
        v += c;
      }
    }
    cols.push(v);
    const o = {};
    headers.forEach((h, idx) => {
      o[h] = cols[idx] !== void 0 ? cols[idx].trim() : "";
    });
    const handle = (o.handle || "").trim();
    const sku = (o.sku || "").trim();
    if (!handle || !sku) {
      skipped++;
      continue;
    }
    const out = { handle, sku };
    if (o.is_preorder !== void 0 && o.is_preorder !== "") out.is_preorder = ["true", "1", "yes", "y"].includes(String(o.is_preorder).toLowerCase());
    if (o.preorder_limit !== void 0 && o.preorder_limit !== "") {
      const n = Number(o.preorder_limit);
      if (Number.isInteger(n) && n >= 0) out.preorder_limit = n;
    }
    if (o.preorder_message !== void 0 && o.preorder_message !== "") out.preorder_message = o.preorder_message;
    rows.push(out);
  }
  return { rows, stats: { totalRows: lines.length - 1, skippedRows: skipped } };
}
__name(parseCsv, "parseCsv");
async function getProductByHandle(env, handle) {
  const q = `#graphql
		query ProductByHandle($handle: String!) {
			productByHandle(handle: $handle) {
				id title
				variants(first: 250) { edges { node { id sku } } }
			}
		}
	`;
  const data = await shopifyGraphQL(env, q, { handle });
  return data.productByHandle;
}
__name(getProductByHandle, "getProductByHandle");
async function metafieldsSet(env, inputs) {
  const m = `#graphql
		mutation Set($metafields: [MetafieldsSetInput!]!) {
			metafieldsSet(metafields: $metafields) { userErrors { field message code } }
		}
	`;
  const data = await shopifyGraphQL(env, m, { metafields: inputs });
  return data.metafieldsSet.userErrors || [];
}
__name(metafieldsSet, "metafieldsSet");
async function listPreorderVariants(env, limit = 1e3) {
  const ns = env.METAFIELD_NAMESPACE || "preorder";
  const q = `#graphql
		query Products($after: String){
			products(first: 50, after: $after, sortKey: TITLE){
				edges{ cursor node{ id title handle variants(first: 100){ edges{ node{ id sku title inventoryQuantity metafield(namespace: "${ns}", key: "is_preorder"){ value } metafieldPreMsg: metafield(namespace: "${ns}", key: "preorder_message"){ value } metafieldPreLimit: metafield(namespace: "${ns}", key: "preorder_limit"){ value } } } } }
				pageInfo{ hasNextPage }
			}
		}
	`;
  const out = [];
  let cursor = null;
  let hasNext = true;
  while (hasNext && out.length < limit) {
    const data = await shopifyGraphQL(env, q, { after: cursor });
    const page = data.products;
    for (const edge of page.edges) {
      cursor = edge.cursor;
      const p = edge.node;
      const vars = (p.variants?.edges || []).map((e) => e.node);
      for (const v of vars) {
        const isPre = v.metafield && String(v.metafield.value).toLowerCase() === "true";
        if (!isPre) continue;
        out.push({ productId: p.id, productTitle: p.title, productHandle: p.handle, variantId: v.id, variantTitle: v.title, sku: v.sku, stockAvailable: typeof v.inventoryQuantity === "number" ? v.inventoryQuantity : void 0, preorderMessage: v.metafieldPreMsg?.value || "", preorderLimit: v.metafieldPreLimit?.value || "" });
        if (out.length >= limit) break;
      }
      if (out.length >= limit) break;
    }
    hasNext = page.pageInfo?.hasNextPage;
  }
  return out;
}
__name(listPreorderVariants, "listPreorderVariants");
async function handleUpload(request, env) {
  const form = await request.formData();
  const file = form.get("file");
  if (!file) return json({ error: "CSV file is required" }, 400);
  const text = await file.text();
  const parsed = parseCsv(text);
  const rows = parsed.rows;
  const namespace = env.METAFIELD_NAMESPACE || "preorder";
  const mongo = buildMongoClient(env);
  let uploadId = null;
  try {
    const up = await mongo.insertOne("uploads", { createdAt: (/* @__PURE__ */ new Date()).toISOString(), filename: file.name || "upload.csv", stats: { totalRows: rows.length, skippedRows: parsed.stats.skippedRows }, status: "processing" });
    uploadId = up.insertedId;
  } catch (_) {
  }
  const productCache = /* @__PURE__ */ new Map();
  const limit = Number(env.CONCURRENCY || 5);
  let idx = 0;
  let results = { totalRows: rows.length, skippedRows: parsed.stats.skippedRows, successCount: 0, notFoundProduct: [], notFoundVariant: [], errors: [] };
  async function next() {
    const i = idx++;
    if (i >= rows.length) return;
    const row = rows[i];
    const handle = row.handle;
    const sku = row.sku;
    try {
      let product = productCache.has(handle) ? productCache.get(handle) : void 0;
      if (product === void 0) {
        product = await getProductByHandle(env, handle);
        productCache.set(handle, product || null);
      }
      if (!product) {
        results.notFoundProduct.push({ handle });
        try {
          await mongo.insertOne("upload_rows", { uploadId, handle, sku, status: "no_product", createdAt: (/* @__PURE__ */ new Date()).toISOString() });
        } catch (_) {
        }
        return;
      }
      const variants = (product.variants?.edges || []).map((e) => e.node);
      const variant = variants.find((v) => (v.sku || "").trim() === sku.trim());
      if (!variant) {
        results.notFoundVariant.push({ handle, sku });
        try {
          await mongo.insertOne("upload_rows", { uploadId, handle, sku, status: "no_variant", createdAt: (/* @__PURE__ */ new Date()).toISOString(), productId: product.id });
        } catch (_) {
        }
        return;
      }
      const isPreorder = row.is_preorder === void 0 ? true : !!row.is_preorder;
      const metafields = [];
      metafields.push({ ownerId: variant.id, namespace, key: "is_preorder", type: "boolean", value: isPreorder ? "true" : "false" });
      if (row.preorder_limit !== void 0 && row.preorder_limit !== null) metafields.push({ ownerId: variant.id, namespace, key: "preorder_limit", type: "number_integer", value: String(row.preorder_limit) });
      if (row.preorder_message !== void 0) metafields.push({ ownerId: variant.id, namespace, key: "preorder_message", type: "single_line_text_field", value: row.preorder_message });
      const errs = await metafieldsSet(env, metafields);
      if (errs && errs.length) {
        results.errors.push({ handle, sku, message: JSON.stringify(errs) });
        try {
          await mongo.insertOne("upload_rows", { uploadId, handle, sku, status: "error", error: errs, createdAt: (/* @__PURE__ */ new Date()).toISOString(), variantId: variant.id });
        } catch (_) {
        }
        return;
      }
      results.successCount++;
      try {
        await mongo.updateOne("variants", { variantId: variant.id }, { $set: { variantId: variant.id, productId: product.id, handle, sku, isPreorder, preorderLimit: (metafields.find((m) => m.key === "preorder_limit") || {}).value || null, preorderMessage: (metafields.find((m) => m.key === "preorder_message") || {}).value, updatedAt: (/* @__PURE__ */ new Date()).toISOString() } }, true);
      } catch (_) {
      }
      try {
        await mongo.insertOne("upload_rows", { uploadId, handle, sku, status: "updated", metafields, createdAt: (/* @__PURE__ */ new Date()).toISOString(), variantId: variant.id });
      } catch (_) {
      }
    } catch (e) {
      results.errors.push({ handle, sku, message: String(e && e.message ? e.message : e) });
      try {
        await mongo.insertOne("upload_rows", { uploadId, handle, sku, status: "exception", error: String(e && e.message ? e.message : e), createdAt: (/* @__PURE__ */ new Date()).toISOString() });
      } catch (_) {
      }
    }
  }
  __name(next, "next");
  const workers = Array.from({ length: Math.min(limit, rows.length) }, () => next());
  await Promise.all(workers);
  while (idx < rows.length) {
    await next();
  }
  try {
    if (uploadId) await mongo.updateOne("uploads", { _id: uploadId }, { $set: { status: "done", finishedAt: (/* @__PURE__ */ new Date()).toISOString(), results } }, false);
  } catch (_) {
  }
  return json({ uploadId, ...results });
}
__name(handleUpload, "handleUpload");
function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), { status, headers: { "content-type": "application/json" } });
}
__name(json, "json");
var worker_default = { async fetch(request, env) {
  const url = new URL(request.url);
  if (url.pathname === "/health") return json({ status: "ok" });
  if (url.pathname === "/api/preorder-products") {
    try {
      const variants = await listPreorderVariants(env, Math.min(Number(url.searchParams.get("limit") || "1000"), 2e3));
      return json({ count: variants.length, variants });
    } catch (e) {
      return json({ error: "Failed to load preorder variants", details: String(e && e.message ? e.message : e) }, 500);
    }
  }
  if (url.pathname === "/upload" && request.method === "POST") {
    try {
      return await handleUpload(request, env);
    } catch (e) {
      return json({ error: "Failed to process upload", details: String(e && e.message ? e.message : e) }, 500);
    }
  }
  if (url.pathname === "/webhooks/shopify/inventory" && request.method === "POST") {
    const secret = env.SHOPIFY_WEBHOOK_SECRET;
    if (!secret) return new Response("Missing secret", { status: 500 });
    const hmac = request.headers.get("x-shopify-hmac-sha256");
    if (!hmac) return new Response("Unauthorized", { status: 401 });
    const body = await request.arrayBuffer();
    const key = await crypto.subtle.importKey("raw", new TextEncoder().encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
    const sig = await crypto.subtle.sign("HMAC", key, body);
    const digest = btoa(String.fromCharCode(...new Uint8Array(sig)));
    if (digest !== hmac) return new Response("Unauthorized", { status: 401 });
    const mongo = buildMongoClient(env);
    const payload = JSON.parse(new TextDecoder().decode(body));
    await mongo.insertOne("inventory_events", { receivedAt: (/* @__PURE__ */ new Date()).toISOString(), topic: request.headers.get("x-shopify-topic"), shopDomain: request.headers.get("x-shopify-shop-domain"), payload });
    return new Response("", { status: 200 });
  }
  if (request.method === "GET") {
    if (!env.ASSETS) return new Response("Assets binding missing", { status: 500 });
    return env.ASSETS.fetch(request);
  }
  return new Response("Not found", { status: 404 });
} };

// ../../../../../AppData/Roaming/npm/node_modules/wrangler/templates/middleware/middleware-ensure-req-body-drained.ts
var drainBody = /* @__PURE__ */ __name(async (request, env, _ctx, middlewareCtx) => {
  try {
    return await middlewareCtx.next(request, env);
  } finally {
    try {
      if (request.body !== null && !request.bodyUsed) {
        const reader = request.body.getReader();
        while (!(await reader.read()).done) {
        }
      }
    } catch (e) {
      console.error("Failed to drain the unused request body.", e);
    }
  }
}, "drainBody");
var middleware_ensure_req_body_drained_default = drainBody;

// ../../../../../AppData/Roaming/npm/node_modules/wrangler/templates/middleware/middleware-miniflare3-json-error.ts
function reduceError(e) {
  return {
    name: e?.name,
    message: e?.message ?? String(e),
    stack: e?.stack,
    cause: e?.cause === void 0 ? void 0 : reduceError(e.cause)
  };
}
__name(reduceError, "reduceError");
var jsonError = /* @__PURE__ */ __name(async (request, env, _ctx, middlewareCtx) => {
  try {
    return await middlewareCtx.next(request, env);
  } catch (e) {
    const error = reduceError(e);
    return Response.json(error, {
      status: 500,
      headers: { "MF-Experimental-Error-Stack": "true" }
    });
  }
}, "jsonError");
var middleware_miniflare3_json_error_default = jsonError;

// .wrangler/tmp/bundle-9uFqoa/middleware-insertion-facade.js
var __INTERNAL_WRANGLER_MIDDLEWARE__ = [
  middleware_ensure_req_body_drained_default,
  middleware_miniflare3_json_error_default
];
var middleware_insertion_facade_default = worker_default;

// ../../../../../AppData/Roaming/npm/node_modules/wrangler/templates/middleware/common.ts
var __facade_middleware__ = [];
function __facade_register__(...args) {
  __facade_middleware__.push(...args.flat());
}
__name(__facade_register__, "__facade_register__");
function __facade_invokeChain__(request, env, ctx, dispatch, middlewareChain) {
  const [head, ...tail] = middlewareChain;
  const middlewareCtx = {
    dispatch,
    next(newRequest, newEnv) {
      return __facade_invokeChain__(newRequest, newEnv, ctx, dispatch, tail);
    }
  };
  return head(request, env, ctx, middlewareCtx);
}
__name(__facade_invokeChain__, "__facade_invokeChain__");
function __facade_invoke__(request, env, ctx, dispatch, finalMiddleware) {
  return __facade_invokeChain__(request, env, ctx, dispatch, [
    ...__facade_middleware__,
    finalMiddleware
  ]);
}
__name(__facade_invoke__, "__facade_invoke__");

// .wrangler/tmp/bundle-9uFqoa/middleware-loader.entry.ts
var __Facade_ScheduledController__ = class ___Facade_ScheduledController__ {
  constructor(scheduledTime, cron, noRetry) {
    this.scheduledTime = scheduledTime;
    this.cron = cron;
    this.#noRetry = noRetry;
  }
  static {
    __name(this, "__Facade_ScheduledController__");
  }
  #noRetry;
  noRetry() {
    if (!(this instanceof ___Facade_ScheduledController__)) {
      throw new TypeError("Illegal invocation");
    }
    this.#noRetry();
  }
};
function wrapExportedHandler(worker) {
  if (__INTERNAL_WRANGLER_MIDDLEWARE__ === void 0 || __INTERNAL_WRANGLER_MIDDLEWARE__.length === 0) {
    return worker;
  }
  for (const middleware of __INTERNAL_WRANGLER_MIDDLEWARE__) {
    __facade_register__(middleware);
  }
  const fetchDispatcher = /* @__PURE__ */ __name(function(request, env, ctx) {
    if (worker.fetch === void 0) {
      throw new Error("Handler does not export a fetch() function.");
    }
    return worker.fetch(request, env, ctx);
  }, "fetchDispatcher");
  return {
    ...worker,
    fetch(request, env, ctx) {
      const dispatcher = /* @__PURE__ */ __name(function(type, init) {
        if (type === "scheduled" && worker.scheduled !== void 0) {
          const controller = new __Facade_ScheduledController__(
            Date.now(),
            init.cron ?? "",
            () => {
            }
          );
          return worker.scheduled(controller, env, ctx);
        }
      }, "dispatcher");
      return __facade_invoke__(request, env, ctx, dispatcher, fetchDispatcher);
    }
  };
}
__name(wrapExportedHandler, "wrapExportedHandler");
function wrapWorkerEntrypoint(klass) {
  if (__INTERNAL_WRANGLER_MIDDLEWARE__ === void 0 || __INTERNAL_WRANGLER_MIDDLEWARE__.length === 0) {
    return klass;
  }
  for (const middleware of __INTERNAL_WRANGLER_MIDDLEWARE__) {
    __facade_register__(middleware);
  }
  return class extends klass {
    #fetchDispatcher = /* @__PURE__ */ __name((request, env, ctx) => {
      this.env = env;
      this.ctx = ctx;
      if (super.fetch === void 0) {
        throw new Error("Entrypoint class does not define a fetch() function.");
      }
      return super.fetch(request);
    }, "#fetchDispatcher");
    #dispatcher = /* @__PURE__ */ __name((type, init) => {
      if (type === "scheduled" && super.scheduled !== void 0) {
        const controller = new __Facade_ScheduledController__(
          Date.now(),
          init.cron ?? "",
          () => {
          }
        );
        return super.scheduled(controller);
      }
    }, "#dispatcher");
    fetch(request) {
      return __facade_invoke__(
        request,
        this.env,
        this.ctx,
        this.#dispatcher,
        this.#fetchDispatcher
      );
    }
  };
}
__name(wrapWorkerEntrypoint, "wrapWorkerEntrypoint");
var WRAPPED_ENTRY;
if (typeof middleware_insertion_facade_default === "object") {
  WRAPPED_ENTRY = wrapExportedHandler(middleware_insertion_facade_default);
} else if (typeof middleware_insertion_facade_default === "function") {
  WRAPPED_ENTRY = wrapWorkerEntrypoint(middleware_insertion_facade_default);
}
var middleware_loader_entry_default = WRAPPED_ENTRY;
export {
  __INTERNAL_WRANGLER_MIDDLEWARE__,
  middleware_loader_entry_default as default
};
//# sourceMappingURL=worker.js.map
