export function buildMongoClient(env) {
	const apiUrl = env.MONGODB_DATA_API_URL;
	const apiKey = env.MONGODB_DATA_API_KEY;
	const dataSource = env.MONGODB_DATA_API_DATA_SOURCE || env.MONGODB_DATA_API_CLUSTER;
	const database = env.MONGODB_DB || 'shopify_preorder';
	if (!apiUrl || !apiKey || !dataSource) {
		throw new Error('Missing MongoDB Data API configuration');
	}

	async function call(action, payload) {
		const res = await fetch(`${apiUrl}/action/${action}`, {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
				'api-key': apiKey,
			},
			body: JSON.stringify({ dataSource, database, ...payload }),
		});
		if (!res.ok) {
			const text = await res.text();
			throw new Error(`Mongo Data API ${action} failed: ${res.status} ${text}`);
		}
		return await res.json();
	}

	return {
		insertOne: (collection, document) => call('insertOne', { collection, document }),
		insertMany: (collection, documents) => call('insertMany', { collection, documents }),
		updateOne: (collection, filter, update, upsert = false) => call('updateOne', { collection, filter, update, upsert }),
	};
}


