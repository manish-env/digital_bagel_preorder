const path = require('path');
const express = require('express');
const multer = require('multer');
const dotenv = require('dotenv');
const pLimitLib = require('p-limit');
const pLimit = typeof pLimitLib === 'function' ? pLimitLib : pLimitLib.default;

dotenv.config();

const { parseCsvFromBuffer } = require('./csv');
const { getProductByHandle, setVariantMetafields, listPreorderVariants } = require('./shopify');
const { withCollections } = require('./db');

const app = express();
const port = process.env.PORT || 3000;

const upload = multer({
	storage: multer.memoryStorage(),
	limits: { fileSize: 10 * 1024 * 1024 },
});

app.use(express.static(path.join(__dirname, '..', 'public')));
app.use('/webhooks/shopify', express.raw({ type: '*/*' }));

app.get('/health', (_req, res) => {
	res.json({ status: 'ok' });
});

app.get('/health/db', async (_req, res) => {
	try {
		const { db } = await withCollections();
		await db.command({ ping: 1 });
		return res.json({ mongo: 'ok' });
	} catch (e) {
		return res.status(500).json({ mongo: 'error', details: String(e && e.message ? e.message : e) });
	}
});

// Shopify webhook: inventory updates (inventory_levels/update)
app.post('/webhooks/shopify/inventory', async (req, res) => {
	try {
		const hmac = req.headers['x-shopify-hmac-sha256'];
		const topic = req.headers['x-shopify-topic'];
		const domain = req.headers['x-shopify-shop-domain'];
		if (!hmac) return res.status(401).end();
		const secret = process.env.SHOPIFY_WEBHOOK_SECRET;
		if (!secret) return res.status(500).json({ error: 'Missing webhook secret' });
		const crypto = require('crypto');
		const digest = crypto.createHmac('sha256', secret).update(req.body).digest('base64');
		if (digest !== hmac) return res.status(401).end();

		const payload = JSON.parse(req.body.toString('utf8'));
		const { inventoryEvents } = await withCollections();
		await inventoryEvents.insertOne({
			receivedAt: new Date(),
			topic: topic,
			shopDomain: domain,
			payload,
		});
		return res.status(200).end();
	} catch (e) {
		return res.status(500).json({ error: 'Failed to handle webhook', details: String(e && e.message ? e.message : e) });
	}
});

// Helper endpoint to register webhook subscription (call once)
app.post('/admin/register-inventory-webhook', express.json(), async (req, res) => {
	try {
		const { graphql } = require('./shopify');
		const callbackBase = process.env.PUBLIC_BASE_URL;
		if (!callbackBase) return res.status(400).json({ error: 'Set PUBLIC_BASE_URL (https URL) for webhooks' });
		const callbackUrl = `${callbackBase.replace(/\/$/, '')}/webhooks/shopify/inventory`;
		const mutation = `#graphql
			mutation webhookSubscriptionCreate($topic: WebhookSubscriptionTopic!, $callbackUrl: URL!) {
				webhookSubscriptionCreate(topic: $topic, webhookSubscription: {callbackUrl: $callbackUrl, format: JSON}) {
					userErrors { field message }
					webhookSubscription { id }
				}
			}
		`;
		const data = await graphql(mutation, { topic: 'INVENTORY_LEVELS_UPDATE', callbackUrl });
		const payload = data && data.data ? data.data.webhookSubscriptionCreate : null;
		if (!payload || (payload.userErrors && payload.userErrors.length)) {
			return res.status(400).json({ error: 'Failed to register webhook', details: payload && payload.userErrors });
		}
		res.json({ ok: true, id: payload.webhookSubscription.id, callbackUrl });
	} catch (e) {
		res.status(500).json({ error: 'Registration failed', details: String(e && e.message ? e.message : e) });
	}
});

app.get('/api/preorder-products', async (req, res) => {
	try {
		const limit = Math.min(Number(req.query.limit || 1000), 2000);
		const variants = await listPreorderVariants(limit);
		res.json({ count: variants.length, variants });
	} catch (e) {
		res.status(500).json({ error: 'Failed to load preorder products', details: String(e && e.message ? e.message : e) });
	}
});

app.post('/upload', upload.single('file'), async (req, res) => {
	if (!req.file) {
		return res.status(400).json({ error: 'CSV file is required' });
	}

	let parsed;
	try {
    parsed = parseCsvFromBuffer(req.file.buffer);
	} catch (err) {
		return res.status(400).json({ error: 'Failed to parse CSV', details: String(err && err.message ? err.message : err) });
	}
  const rows = Array.isArray(parsed) ? parsed : parsed.rows;

	const namespace = process.env.METAFIELD_NAMESPACE || 'preorder';
	const concurrency = Number(process.env.CONCURRENCY || 5);
	const limit = pLimit(concurrency);
	const productCache = new Map(); // handle -> product or null if not found

	const results = {
    totalRows: rows.length,
    skippedRows: parsed && parsed.stats ? parsed.stats.skippedRows : 0,
		successCount: 0,
		notFoundProduct: [], // { handle }
		notFoundVariant: [], // { handle, sku }
		errors: [], // { handle, sku, message }
	};

	let mongoConnected = true;
	const { uploads, uploadRows, variants } = await withCollections().catch(() => { mongoConnected = false; return { uploads: null, uploadRows: null, variants: null }; });
	const uploadId = uploads ? (await uploads.insertOne({
		createdAt: new Date(),
		filename: req.file.originalname,
		stats: { totalRows: rows.length, skippedRows: results.skippedRows },
		status: 'processing',
	})).insertedId : null;

	const tasks = rows.map((row, index) => limit(async () => {
		const handle = row.handle;
		const sku = row.sku;
		try {
			let product = productCache.has(handle) ? productCache.get(handle) : undefined;
			if (product === undefined) {
				product = await getProductByHandle(handle);
				productCache.set(handle, product || null);
			}

			if (!product) {
				results.notFoundProduct.push({ handle });
				if (uploadRows) await uploadRows.insertOne({ uploadId, handle, sku, status: 'no_product', createdAt: new Date() });
				return;
			}

			const variants = (product.variants && product.variants.edges ? product.variants.edges : []).map(e => e.node);
			const variant = variants.find(v => (v.sku || '').trim() === sku.trim());
			if (!variant) {
				results.notFoundVariant.push({ handle, sku });
				if (uploadRows) await uploadRows.insertOne({ uploadId, handle, sku, status: 'no_variant', createdAt: new Date(), productId: product.id });
				return;
			}

			const metafields = [];
			const isPreorder = (row.is_preorder === undefined ? true : !!row.is_preorder);
			metafields.push({
				ownerId: variant.id,
				namespace: namespace,
				key: 'is_preorder',
				type: 'boolean',
				value: isPreorder ? 'true' : 'false',
			});
			if (row.preorder_limit !== undefined && row.preorder_limit !== null) {
				metafields.push({
					ownerId: variant.id,
					namespace: namespace,
					key: 'preorder_limit',
					type: 'number_integer',
					value: String(row.preorder_limit),
				});
			}
			if (row.preorder_message !== undefined) {
				metafields.push({
					ownerId: variant.id,
					namespace: namespace,
					key: 'preorder_message',
					type: 'single_line_text_field',
					value: row.preorder_message,
				});
			}

			if (metafields.length === 0) {
				return; // nothing to update
			}

			const userErrors = await setVariantMetafields(metafields);
			if (userErrors && userErrors.length) {
				results.errors.push({ handle, sku, message: JSON.stringify(userErrors) });
				if (uploadRows) await uploadRows.insertOne({ uploadId, handle, sku, status: 'error', error: userErrors, createdAt: new Date(), variantId: variant.id });
				return;
			}
			results.successCount += 1;
			if (variants) {
				const preorderLimit = (metafields.find(m => m.key === 'preorder_limit') || {}).value || null;
				const preorderMessage = (metafields.find(m => m.key === 'preorder_message') || {}).value;
				const setDoc = {
					variantId: variant.id,
					productId: product.id,
					handle: handle,
					sku: sku,
					isPreorder: isPreorder,
					preorderLimit: preorderLimit,
					updatedAt: new Date(),
				};
				if (preorderMessage !== undefined) setDoc.preorderMessage = preorderMessage;
				await variants.updateOne(
					{ variantId: variant.id },
					{ $set: setDoc },
					{ upsert: true }
				);
			}
			if (uploadRows) await uploadRows.insertOne({ uploadId, handle, sku, status: 'updated', createdAt: new Date(), variantId: variant.id, metafields });
		} catch (e) {
			results.errors.push({ handle: row.handle, sku: row.sku, message: String(e && e.message ? e.message : e) });
			if (uploadRows) await uploadRows.insertOne({ uploadId, handle, sku, status: 'exception', error: String(e && e.message ? e.message : e), createdAt: new Date() });
		}
	}));

	await Promise.all(tasks);
	if (uploadId && uploads) {
		await uploads.updateOne({ _id: uploadId }, { $set: { status: 'done', finishedAt: new Date(), results } });
	}
	return res.json({ uploadId, mongoConnected, ...results });
});

app.listen(port, () => {
	// eslint-disable-next-line no-console
	console.log(`Server listening on http://localhost:${port}`);
});


