let schemaInitialized = false;

async function ensureSchema(db) {
    if (schemaInitialized) return;
    // Uploads & rows
    await db.prepare(`CREATE TABLE IF NOT EXISTS uploads (
        _id TEXT PRIMARY KEY,
        createdAt TEXT,
        filename TEXT,
        status TEXT,
        stats TEXT,
        progress TEXT,
        startedAt TEXT,
        finishedAt TEXT,
        results TEXT
    );`).run();
    // Add startedAt column if it doesn't exist (for existing tables)
    try { await db.prepare(`ALTER TABLE uploads ADD COLUMN startedAt TEXT;`).run(); } catch (_) {}
    await db.prepare(`CREATE TABLE IF NOT EXISTS upload_rows (
        id TEXT PRIMARY KEY,
        uploadId TEXT,
        handle TEXT,
        sku TEXT,
        status TEXT,
        error TEXT,
        metafields TEXT,
        createdAt TEXT,
        productId TEXT,
        variantId TEXT
    );`).run();

    // Variants mapping (add inventoryItemId column if missing)
    await db.prepare(`CREATE TABLE IF NOT EXISTS variants (
        variantId TEXT PRIMARY KEY,
        productId TEXT,
        handle TEXT,
        sku TEXT,
        isPreorder INTEGER,
        preorderLimit TEXT,
        preorderMessage TEXT,
        updatedAt TEXT
    );`).run();
    try { await db.prepare(`ALTER TABLE variants ADD COLUMN inventoryItemId TEXT;`).run(); } catch (_) {}

    // Legacy generic events table (still available)
    await db.prepare(`CREATE TABLE IF NOT EXISTS inventory_events (
        id TEXT PRIMARY KEY,
        receivedAt TEXT,
        topic TEXT,
        shopDomain TEXT,
        payload TEXT
    );`).run();

    // Upload queue for background processing
    await db.prepare(`CREATE TABLE IF NOT EXISTS upload_queue (
        id TEXT PRIMARY KEY,
        uploadId TEXT,
        rows TEXT,
        namespace TEXT,
        status TEXT DEFAULT 'pending',
        createdAt TEXT,
        startedAt TEXT,
        completedAt TEXT,
        error TEXT
    );`).run();

    // Inventory levels snapshot per inventory_item_id (latest available)
    await db.prepare(`CREATE TABLE IF NOT EXISTS inventory_levels (
        inventory_item_id TEXT PRIMARY KEY,
        available INTEGER,
        updatedAt TEXT
    );`).run();

    // Restock events (positive deltas only)
    await db.prepare(`CREATE TABLE IF NOT EXISTS inventory_restock_events (
        id TEXT PRIMARY KEY,
        inventory_item_id TEXT,
        location_id TEXT,
        oldStock INTEGER,
        newStock INTEGER,
        delta INTEGER,
        receivedAt TEXT,
        source TEXT
    );`).run();

    await db.prepare(`CREATE TABLE IF NOT EXISTS webhook_debug (
        id TEXT PRIMARY KEY,
        inventory_item_id TEXT,
        available INTEGER,
        location_id TEXT,
        receivedAt TEXT,
        headers TEXT,
        payload TEXT
    );`).run();

    await db.prepare(`CREATE TABLE IF NOT EXISTS webhook_debug (
        id TEXT PRIMARY KEY,
        inventory_item_id TEXT,
        available INTEGER,
        location_id TEXT,
        receivedAt TEXT,
        headers TEXT,
        payload TEXT
    );`).run();

    // Ordered quantities by SKU (and optional mapping)
    await db.prepare(`CREATE TABLE IF NOT EXISTS ordered (
        sku TEXT PRIMARY KEY,
        variantId TEXT,
        inventoryItemId TEXT,
        ordered INTEGER,
        updatedAt TEXT
    );`).run();

    schemaInitialized = true;
}

function asJson(val) {
    if (val === undefined) return null;
    try { return JSON.stringify(val); } catch (_) { return null; }
}

export function buildMongoClient(env) {
    const db = env.rjs_preorder;
    if (!db) throw new Error('D1 binding not found. Configure [[d1_databases]] in wrangler.toml.');

    async function insertOne(collection, doc) {
        await ensureSchema(db);
        if (collection === 'uploads') {
            const id = crypto.randomUUID();
            await db.prepare(`INSERT INTO uploads (_id, createdAt, filename, status, stats, progress, startedAt, finishedAt, results) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)`)
                .bind(id, doc.createdAt || null, doc.filename || null, doc.status || null, asJson(doc.stats), asJson(doc.progress), doc.startedAt || null, doc.finishedAt || null, asJson(doc.results))
                .run();
            return { insertedId: id };
        }
        if (collection === 'upload_rows') {
            const id = crypto.randomUUID();
            await db.prepare(`INSERT INTO upload_rows (id, uploadId, handle, sku, status, error, metafields, createdAt, productId, variantId) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)`) 
                .bind(id, doc.uploadId || null, doc.handle || null, doc.sku || null, doc.status || null, asJson(doc.error), asJson(doc.metafields), doc.createdAt || null, doc.productId || null, doc.variantId || null)
                .run();
            return { insertedId: id };
        }
        if (collection === 'inventory_events') {
            const id = crypto.randomUUID();
            await db.prepare(`INSERT INTO inventory_events (id, receivedAt, topic, shopDomain, payload) VALUES (?1, ?2, ?3, ?4, ?5)`) 
                .bind(id, doc.receivedAt || null, doc.topic || null, doc.shopDomain || null, asJson(doc.payload))
                .run();
            return { insertedId: id };
        }
        if (collection === 'variants') {
            await db.prepare(`INSERT OR REPLACE INTO variants (variantId, productId, handle, sku, isPreorder, preorderLimit, preorderMessage, updatedAt) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)`) 
                .bind(doc.variantId || null, doc.productId || null, doc.handle || null, doc.sku || null, doc.isPreorder ? 1 : 0, doc.preorderLimit ?? null, doc.preorderMessage ?? null, doc.updatedAt || null)
                .run();
            return { insertedId: doc.variantId || null };
        }
        if (collection === 'inventory_restock_events') {
            const id = crypto.randomUUID();
            await db.prepare(`INSERT INTO inventory_restock_events (id, inventory_item_id, location_id, oldStock, newStock, delta, receivedAt, source) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)`) 
                .bind(id, doc.inventory_item_id || null, doc.location_id || null, doc.oldStock ?? null, doc.newStock ?? null, doc.delta ?? null, doc.receivedAt || null, doc.source || null)
                .run();
            return { insertedId: id };
        }
        if (collection === 'webhook_debug') {
            const id = crypto.randomUUID();
            await db.prepare(`INSERT INTO webhook_debug (id, inventory_item_id, available, location_id, receivedAt, headers, payload) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)`)
                .bind(
                    id,
                    doc.inventory_item_id || null,
                    doc.available ?? null,
                    doc.location_id || null,
                    doc.receivedAt || null,
                    asJson(doc.headers),
                    asJson(doc.rawBody ?? doc.payload ?? null)
                )
                .run();
            return { insertedId: id };
        }
        if (collection === 'webhook_debug') {
            const id = crypto.randomUUID();
            await db.prepare(`INSERT INTO webhook_debug (id, inventory_item_id, available, location_id, receivedAt, headers, payload) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)`)
                .bind(
                    id,
                    doc.inventory_item_id || null,
                    doc.available ?? null,
                    doc.location_id || null,
                    doc.receivedAt || null,
                    asJson(doc.headers),
                    asJson(doc.rawBody ?? doc.payload ?? null)
                )
                .run();
            return { insertedId: id };
        }
        if (collection === 'ordered') {
            await db.prepare(`INSERT OR REPLACE INTO ordered (sku, variantId, inventoryItemId, ordered, updatedAt) VALUES (?1, ?2, ?3, ?4, ?5)`) 
                .bind(doc.sku || null, doc.variantId || null, doc.inventoryItemId || null, Number(doc.ordered || 0), doc.updatedAt || null)
                .run();
            return { insertedId: doc.sku || null };
        }
        throw new Error(`Unknown collection: ${collection}`);
    }

    async function insertMany(collection, documents) {
        const ids = [];
        for (const d of documents) {
            const r = await insertOne(collection, d);
            ids.push(r.insertedId ?? null);
        }
        return { insertedIds: ids };
    }

    async function updateOne(collection, filter, update, upsert = false) {
        await ensureSchema(db);
        const set = (update && update.$set) || {};
        if (collection === 'uploads') {
            const id = filter && filter._id;
            if (!id) throw new Error('uploads.updateOne requires filter._id');
            await db.prepare(`UPDATE uploads SET status = COALESCE(?2, status), progress = COALESCE(?3, progress), startedAt = COALESCE(?4, startedAt), finishedAt = COALESCE(?5, finishedAt), results = COALESCE(?6, results) WHERE _id = ?1`)
                .bind(id, set.status ?? null, asJson(set.progress), set.startedAt ?? null, set.finishedAt ?? null, asJson(set.results))
                .run();
            return { matchedCount: 1, modifiedCount: 1 };
        }
        if (collection === 'variants') {
            const variantId = filter && filter.variantId;
            if (!variantId) throw new Error('variants.updateOne requires filter.variantId');
            await db.prepare(`INSERT OR REPLACE INTO variants (variantId, productId, handle, sku, isPreorder, preorderLimit, preorderMessage, updatedAt, inventoryItemId) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, COALESCE(?9, (SELECT inventoryItemId FROM variants WHERE variantId = ?1)))`) 
                .bind(
                    variantId,
                    set.productId ?? null,
                    set.handle ?? null,
                    set.sku ?? null,
                    set.isPreorder ? 1 : 0,
                    set.preorderLimit ?? null,
                    set.preorderMessage ?? null,
                    set.updatedAt ?? null,
                    set.inventoryItemId ?? null
                )
                .run();
            return { upsertedId: variantId, matchedCount: 1, modifiedCount: 1 };
        }
        if (collection === 'inventory_levels') {
            const inventory_item_id = filter && filter.inventory_item_id;
            if (!inventory_item_id) throw new Error('inventory_levels.updateOne requires filter.inventory_item_id');
            await db.prepare(`INSERT OR REPLACE INTO inventory_levels (inventory_item_id, available, updatedAt) VALUES (?1, ?2, ?3)`) 
                .bind(inventory_item_id, Number(set.available ?? 0), set.updatedAt || null)
                .run();
            return { upsertedId: inventory_item_id, matchedCount: 1, modifiedCount: 1 };
        }
        if (collection === 'ordered') {
            const sku = filter && filter.sku;
            if (!sku) throw new Error('ordered.updateOne requires filter.sku');
            await db.prepare(`INSERT OR REPLACE INTO ordered (sku, variantId, inventoryItemId, ordered, updatedAt) VALUES (?1, COALESCE(?2, (SELECT variantId FROM ordered WHERE sku=?1)), COALESCE(?3, (SELECT inventoryItemId FROM ordered WHERE sku=?1)), COALESCE(?4, (SELECT ordered FROM ordered WHERE sku=?1)), COALESCE(?5, (SELECT updatedAt FROM ordered WHERE sku=?1)))`) 
                .bind(sku, set.variantId ?? null, set.inventoryItemId ?? null, set.ordered !== undefined ? Number(set.ordered) : null, set.updatedAt || null)
                .run();
            return { upsertedId: sku, matchedCount: 1, modifiedCount: 1 };
        }
        throw new Error(`Unsupported update on collection: ${collection}`);
    }

    async function findOne(collection, filter) {
        await ensureSchema(db);
        if (collection === 'uploads') {
            const id = filter && filter._id;
            if (!id) throw new Error('uploads.findOne requires filter._id');
            const r = await db.prepare(`SELECT _id, createdAt, filename, status, stats, progress, startedAt, finishedAt, results FROM uploads WHERE _id = ?1`).bind(id).first();
            return r || null;
        }
        if (collection === 'inventory_levels') {
            const id = filter && filter.inventory_item_id;
            if (!id) throw new Error('inventory_levels.findOne requires filter.inventory_item_id');
            const r = await db.prepare(`SELECT inventory_item_id, available, updatedAt FROM inventory_levels WHERE inventory_item_id = ?1`).bind(id).first();
            return r || null;
        }
        if (collection === 'ordered') {
            const sku = filter && filter.sku;
            if (!sku) throw new Error('ordered.findOne requires filter.sku');
            const r = await db.prepare(`SELECT sku, variantId, inventoryItemId, ordered, updatedAt FROM ordered WHERE sku = ?1`).bind(sku).first();
            return r || null;
        }
        if (collection === 'variants') {
            const variantId = filter && filter.variantId;
            if (!variantId) throw new Error('variants.findOne requires filter.variantId');
            const r = await db.prepare(`SELECT variantId, productId, handle, sku, isPreorder, preorderLimit, preorderMessage, updatedAt, inventoryItemId FROM variants WHERE variantId = ?1`).bind(variantId).first();
            return r || null;
        }
        throw new Error(`Unsupported findOne on collection: ${collection}`);
    }

    async function aggregate(collection, pipeline) {
        await ensureSchema(db);
        if (collection === 'upload_rows' && pipeline && pipeline.length >= 2) {
            const matchStage = pipeline[0];
            const groupStage = pipeline[1];

            if (matchStage.$match && matchStage.$match.uploadId && groupStage.$group) {
                const uploadId = matchStage.$match.uploadId;
                // Count successful records
                const successfulResult = await db.prepare(`
                    SELECT COUNT(*) as count FROM upload_rows
                    WHERE uploadId = ?1 AND status = 'success'
                `).bind(uploadId).first();

                // Count failed records (error, policy_error, exception, no_variant)
                const failedResult = await db.prepare(`
                    SELECT COUNT(*) as count FROM upload_rows
                    WHERE uploadId = ?1 AND status IN ('error', 'policy_error', 'exception', 'no_variant')
                `).bind(uploadId).first();

                return [{
                    successful: successfulResult ? successfulResult.count : 0,
                    failed: failedResult ? failedResult.count : 0
                }];
            }
        }
        throw new Error(`Unsupported aggregate on collection: ${collection}`);
    }

    async function find(collection, filter, options = {}) {
        await ensureSchema(db);
        if (collection === 'upload_rows') {
            const uploadId = filter && filter.uploadId;
            const statusIn = filter && filter.status && filter.status.$in;

            if (uploadId && statusIn && Array.isArray(statusIn)) {
                const placeholders = statusIn.map((_, i) => `?${i + 2}`).join(',');
                const sql = `SELECT * FROM upload_rows WHERE uploadId = ?1 AND status IN (${placeholders}) ORDER BY createdAt DESC LIMIT 10`;
                const rows = await db.prepare(sql).bind(uploadId, ...statusIn).all();
                return rows && rows.results ? rows.results : [];
            }
        }
        throw new Error(`Unsupported find on collection: ${collection}`);
    }

    async function findVariantsByInventoryItemId(inventoryItemId) {
        await ensureSchema(db);
        const rows = await db.prepare(`SELECT variantId, productId, handle, sku, isPreorder, preorderLimit, preorderMessage, updatedAt, inventoryItemId FROM variants WHERE inventoryItemId = ?1`).bind(inventoryItemId).all();
        return rows && rows.results ? rows.results : [];
    }

    async function findVariantBySku(sku) {
        await ensureSchema(db);
        const r = await db.prepare(`SELECT variantId, productId, handle, sku, isPreorder, preorderLimit, preorderMessage, updatedAt, inventoryItemId FROM variants WHERE sku = ?1`).bind(sku).first();
        return r || null;
    }

    async function sumRestockDelta(inventory_item_id) {
        await ensureSchema(db);
        const r = await db.prepare(`SELECT COALESCE(SUM(delta), 0) as total FROM inventory_restock_events WHERE inventory_item_id = ?1`).bind(inventory_item_id).first();
        return Number((r && r.total) || 0);
    }

    async function clearPreorderDataTables() {
        await ensureSchema(db);
        const tables = ['uploads', 'upload_rows', 'variants', 'inventory_levels', 'inventory_restock_events', 'webhook_debug', 'ordered'];
        const cleared = {};
        for (const table of tables) {
            try {
                const row = await db.prepare(`SELECT COUNT(*) AS count FROM ${table}`).first();
                cleared[table] = Number(row && row.count ? row.count : 0);
                await db.prepare(`DELETE FROM ${table}`).run();
            } catch (_) {
                cleared[table] = 0;
            }
        }
        return cleared;
    }

    async function deleteVariantData(variantId) {
        await ensureSchema(db);
        await db.prepare(`DELETE FROM variants WHERE variantId = ?1`).bind(variantId).run();
        await db.prepare(`DELETE FROM upload_rows WHERE variantId = ?1`).bind(variantId).run();
        return true;
    }

    // Queue functions for background processing
    async function queueUpload(uploadId, rows, namespace) {
        await ensureSchema(db);
        const queueId = `${uploadId}_${Date.now()}`;
        await db.prepare(`
            INSERT INTO upload_queue (id, uploadId, rows, namespace, createdAt)
            VALUES (?1, ?2, ?3, ?4, ?5)
        `).bind(queueId, uploadId, JSON.stringify(rows), namespace, new Date().toISOString()).run();
        return queueId;
    }

    async function dequeueUpload() {
        await ensureSchema(db);
        const result = await db.prepare(`
            SELECT * FROM upload_queue
            WHERE status = 'pending'
            ORDER BY createdAt ASC
            LIMIT 1
        `).first();

        if (result) {
            // Mark as processing
            await db.prepare(`
                UPDATE upload_queue
                SET status = 'processing', startedAt = ?1
                WHERE id = ?2
            `).bind(new Date().toISOString(), result.id).run();

            return {
                ...result,
                rows: JSON.parse(result.rows)
            };
        }
        return null;
    }

    async function completeUpload(queueId, error = null) {
        await ensureSchema(db);
        const status = error ? 'failed' : 'completed';
        await db.prepare(`
            UPDATE upload_queue
            SET status = ?1, completedAt = ?2, error = ?3
            WHERE id = ?4
        `).bind(status, new Date().toISOString(), error, queueId).run();
    }

    return { insertOne, insertMany, updateOne, findOne, aggregate, find, findVariantsByInventoryItemId, findVariantBySku, sumRestockDelta, clearPreorderDataTables, deleteVariantData, queueUpload, dequeueUpload, completeUpload };
}


