const axios = require('axios');

function getEnv(name) {
	const v = process.env[name];
	if (!v) throw new Error(`Missing required env: ${name}`);
	return v;
}

const apiVersion = process.env.SHOPIFY_API_VERSION || '2024-10';
const storeDomainRaw = getEnv('SHOPIFY_STORE_DOMAIN');
const adminToken = getEnv('SHOPIFY_ADMIN_ACCESS_TOKEN');

const storeBase = storeDomainRaw.startsWith('http') ? storeDomainRaw : `https://${storeDomainRaw}`;
const graphqlUrl = `${storeBase.replace(/\/$/, '')}/admin/api/${apiVersion}/graphql.json`;

async function graphql(query, variables) {
	const res = await axios.post(
		graphqlUrl,
		{ query, variables },
		{
			headers: {
				'X-Shopify-Access-Token': adminToken,
				'Content-Type': 'application/json',
			},
			timeout: 30000,
		}
	);
	if (res.data && res.data.errors) {
		throw new Error(`GraphQL errors: ${JSON.stringify(res.data.errors)}`);
	}
	return res.data;
}

async function getProductByHandle(handle) {
	const query = `#graphql
		query ProductByHandle($handle: String!) {
			productByHandle(handle: $handle) {
				id
				title
				variants(first: 250) {
					edges {
						node { id sku }
					}
				}
			}
		}
	`;
	const data = await graphql(query, { handle });
	return data && data.data ? data.data.productByHandle : null;
}

async function setVariantMetafields(metafieldsInputs) {
	const mutation = `#graphql
		mutation SetMetafields($metafields: [MetafieldsSetInput!]!) {
			metafieldsSet(metafields: $metafields) {
				metafields { id key namespace type }
				userErrors { field message code }
			}
		}
	`;
	const data = await graphql(mutation, { metafields: metafieldsInputs });
	const payload = data && data.data ? data.data.metafieldsSet : null;
	return payload ? payload.userErrors : [{ message: 'Unknown response from Shopify' }];
}

async function listProductsWithVariantsPage(cursor) {
	const query = `#graphql
		query ProductsWithVariants($after: String) {
			products(first: 50, after: $after, sortKey: TITLE) {
				edges {
					cursor
					node {
						id
						title
						handle
						variants(first: 100) {
							edges {
								node {
									id
									sku
									title
									inventoryQuantity
									metafield(namespace: "${process.env.METAFIELD_NAMESPACE || 'preorder'}", key: "is_preorder") {
										value
										type
									}
									metafieldPreMsg: metafield(namespace: "${process.env.METAFIELD_NAMESPACE || 'preorder'}", key: "preorder_message") { value }
									metafieldPreLimit: metafield(namespace: "${process.env.METAFIELD_NAMESPACE || 'preorder'}", key: "preorder_limit") { value }
								}
							}
						}
					}
				}
				pageInfo { hasNextPage }
			}
		}
	`;
	const data = await graphql(query, { after: cursor || null });
	return data && data.data ? data.data.products : { edges: [], pageInfo: { hasNextPage: false } };
}

async function listPreorderVariants(limit = 1000) {
	const out = [];
	let cursor = undefined;
	let hasNext = true;
	while (hasNext && out.length < limit) {
		const page = await listProductsWithVariantsPage(cursor);
		for (const edge of page.edges) {
			cursor = edge.cursor;
			const p = edge.node;
			const variants = (p.variants?.edges || []).map(e => e.node);
			for (const v of variants) {
				const isPre = v.metafield && String(v.metafield.value).toLowerCase() === 'true';
				if (!isPre) continue;
				out.push({
					productId: p.id,
					productTitle: p.title,
					productHandle: p.handle,
					variantId: v.id,
					variantTitle: v.title,
					sku: v.sku,
					stockAvailable: typeof v.inventoryQuantity === 'number' ? v.inventoryQuantity : undefined,
					preorderMessage: v.metafieldPreMsg?.value || '',
					preorderLimit: v.metafieldPreLimit?.value || '',
				});
				if (out.length >= limit) break;
			}
			if (out.length >= limit) break;
		}
		hasNext = page.pageInfo?.hasNextPage;
	}
	return out;
}

module.exports = { graphql, getProductByHandle, setVariantMetafields, listProductsWithVariantsPage, listPreorderVariants };


