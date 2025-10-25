const { MongoClient } = require('mongodb');

let cachedClient = null;
let cachedDb = null;

async function getDb() {
	if (cachedDb) return cachedDb;
	const uri = process.env.MONGODB_URI;
	if (!uri) throw new Error('Missing required env: MONGODB_URI');
	const client = new MongoClient(uri, { maxPoolSize: 10 });
	await client.connect();
	cachedClient = client;
	// If URI includes a db name, driver will use it; otherwise default
	const dbNameFromUri = (() => {
		try {
			const path = new URL(uri).pathname;
			return path && path.length > 1 ? decodeURIComponent(path.slice(1)) : '';
		} catch (_) { return ''; }
	})();
	const dbName = dbNameFromUri || process.env.MONGODB_DB || 'shopify_preorder';
	cachedDb = client.db(dbName);
	try {
		await cachedDb.command({ ping: 1 });
		// eslint-disable-next-line no-console
		console.log(`[mongo] Connected to ${dbName}`);
	} catch (e) {
		// eslint-disable-next-line no-console
		console.error('[mongo] Ping failed:', e);
	}
	return cachedDb;
}

async function withCollections() {
	const db = await getDb();
	const uploads = db.collection('uploads');
	const uploadRows = db.collection('upload_rows');
	const variants = db.collection('variants');
	const inventoryEvents = db.collection('inventory_events');
	return { db, uploads, uploadRows, variants, inventoryEvents };
}

module.exports = { getDb, withCollections };


