async function shopifyGraphQL(env, query, variables) {
    const apiVersion = env.SHOPIFY_API_VERSION || '2024-10';
    const domain = (env.SHOPIFY_STORE_DOMAIN || '').trim();
    const token = (env.SHOPIFY_ADMIN_ACCESS_TOKEN || '').trim();
    if (!domain || !token) throw new Error('Missing Shopify config. Set SHOPIFY_STORE_DOMAIN and SHOPIFY_ADMIN_ACCESS_TOKEN.');
    const store = domain.startsWith('http') ? domain : `https://${domain}`;
    const url = `${store.replace(/\/$/,'')}/admin/api/${apiVersion}/graphql.json`;
    const res = await fetch(url, { method: 'POST', headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' }, body: JSON.stringify({ query, variables }) });
    const text = await res.text();
    let data; try { data = text ? JSON.parse(text) : {}; } catch (_) { throw new Error(text); }
    if (!res.ok) throw new Error(data && data.errors ? JSON.stringify(data.errors) : (data && data.message) || text || res.statusText);
    if (data.errors) throw new Error(JSON.stringify(data.errors));
    return data.data;
}

async function getProductByHandle(env, handle) {
    const q = `#graphql
        query ProductByHandle($handle: String!) {
            productByHandle(handle: $handle) {
                id
                title
                variants(first: 250) {
                    edges {
                        node { id sku inventoryItem { id } }
                    }
                }
            }
        }
    `;
    const data = await shopifyGraphQL(env, q, { handle });
    return data.productByHandle;
}

async function metafieldsSet(env, inputs) {
    // Filter out null values - these should delete the metafield
    const validInputs = inputs.filter(input => input.value !== null);
    const nullInputs = inputs.filter(input => input.value === null);

    const errors = [];

    // Handle non-null values with metafieldsSet
    if (validInputs.length > 0) {
        const m = `#graphql
            mutation Set($metafields: [MetafieldsSetInput!]!) {
                metafieldsSet(metafields: $metafields) {
                    userErrors { field message code }
                }
            }
        `;
        const data = await shopifyGraphQL(env, m, { metafields: validInputs });
        if (data.metafieldsSet.userErrors && data.metafieldsSet.userErrors.length) {
            errors.push(...data.metafieldsSet.userErrors);
        }
    }

    // Handle null values (deletions) by getting and deleting existing metafields
    for (const input of nullInputs) {
        try {
            const existing = await getVariantMetafield(env, input.ownerId, input.key, input.namespace);
            if (existing && existing.id) {
                const delErrors = await deleteVariantMetafield(env, input.ownerId, input.key, input.namespace);
                if (delErrors && delErrors.length) {
                    errors.push(...delErrors);
                }
            }
        } catch (error) {
            errors.push({ field: 'metafield', message: `Delete failed: ${error.message}` });
        }
    }

    return errors;
}

async function listPreorderVariants(env, limit = 1000) {
    const ns = env.METAFIELD_NAMESPACE || 'preorder';
    const q = `#graphql
        query Products($after: String) {
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
                                    inventoryItem { id }
                                    metafield(namespace: "${ns}", key: "is_preorder") { value }
                                    metafieldPreMsg: metafield(namespace: "${ns}", key: "preorder_message") { value }
                                    metafieldPreLimit: metafield(namespace: "${ns}", key: "preorder_limit") { value }
                                }
                            }
                        }
                    }
                }
                pageInfo { hasNextPage endCursor }
            }
        }
    `;
    const out = []; let cursor = null; let hasNext = true;
    while (hasNext && out.length < limit) {
        const data = await shopifyGraphQL(env, q, { after: cursor });
        const page = data.products;
        for (const edge of page.edges) {
            const p = edge.node;
            const vars = (p.variants?.edges || []).map(e => e.node);
            for (const v of vars) {
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
                    inventoryItemId: v.inventoryItem?.id || null,
                    isPreorder: true
                });
                if (out.length >= limit) break;
            }
            if (out.length >= limit) break;
        }
        hasNext = !!(page.pageInfo && page.pageInfo.hasNextPage);
        cursor = page.pageInfo ? page.pageInfo.endCursor : null;
    }
    return out;
}

// Helper to get variant and product by SKU
async function getVariantBySku(env, sku) {
    if (!sku || !sku.trim()) return null;
    const searchSku = sku.trim();
    const q = `#graphql
        query Products($after: String) {
            products(first: 50, after: $after, sortKey: TITLE) {
                edges {
                    cursor
                    node {
                        id
                        title
                        handle
                        variants(first: 250) {
                            edges {
                                node {
                                    id
                                    sku
                                    inventoryItem { id }
                                }
                            }
                        }
                    }
                }
                pageInfo { hasNextPage endCursor }
            }
        }
    `;
    let cursor = null;
    let hasNext = true;
    const maxPages = 100; // Limit search to prevent infinite loops
    let pageCount = 0;
    
    while (hasNext && pageCount < maxPages) {
        pageCount++;
        const data = await shopifyGraphQL(env, q, { after: cursor });
        const page = data.products;
        
        for (const edge of page.edges) {
            const product = edge.node;
            const variants = (product.variants?.edges || []).map(e => e.node);
            
            for (const variant of variants) {
                if ((variant.sku || '').trim() === searchSku) {
                    return {
                        variant: {
                            id: variant.id,
                            sku: variant.sku,
                            inventoryItem: variant.inventoryItem
                        },
                        product: {
                            id: product.id,
                            title: product.title,
                            handle: product.handle
                        }
                    };
                }
            }
        }
        
        hasNext = !!(page.pageInfo && page.pageInfo.hasNextPage);
        cursor = page.pageInfo ? page.pageInfo.endCursor : null;
    }
    
    return null;
}

async function deleteVariantMetafield(env, variantId, key, namespace = 'preorder') {
    const metafield = await getVariantMetafield(env, variantId, key, namespace);
    if (!metafield || !metafield.id) return [];

    // Extract the metafield ID from the GID
    const metafieldId = metafield.id.replace('gid://shopify/Metafield/', '');

    // Use REST API to delete the metafield
    const apiVersion = env.SHOPIFY_API_VERSION || '2024-10';
    const url = `https://${env.SHOPIFY_STORE_DOMAIN}/admin/api/${apiVersion}/metafields/${metafieldId}.json`;

    const response = await fetch(url, {
        method: 'DELETE',
        headers: {
            'X-Shopify-Access-Token': env.SHOPIFY_ADMIN_ACCESS_TOKEN,
            'Content-Type': 'application/json',
        },
    });

    if (!response.ok) {
        let errorDetails = null;
        try {
            errorDetails = await response.json();
        } catch (_) {}
        return [{
            message: `REST delete failed: ${response.status} ${response.statusText}`,
            details: errorDetails,
        }];
    }

    return [];
}

// REST API based variant lookup by SKU (more efficient than GraphQL for large catalogs)
async function getVariantBySkuREST(env, sku) {
    if (!sku || !sku.trim()) return null;
    const searchSku = sku.trim();

    try {
        // Use a more targeted GraphQL query with SKU filter
        const q = `#graphql
            query GetVariantBySku($query: String!) {
                productVariants(first: 1, query: $query) {
                    edges {
                        node {
                            id
                            sku
                            product {
                                id
                                title
                                handle
                            }
                            inventoryItem {
                                id
                            }
                        }
                    }
                }
            }
        `;

        const data = await shopifyGraphQL(env, q, { query: `sku:${searchSku}` });

        if (data.productVariants?.edges?.length > 0) {
            const variant = data.productVariants.edges[0].node;
            return {
                variant: {
                    id: variant.id,
                    sku: variant.sku,
                    inventoryItem: variant.inventoryItem
                },
                product: {
                    id: variant.product.id,
                    title: variant.product.title,
                    handle: variant.product.handle
                }
            };
        }

        return null;
    } catch (error) {
        console.warn('Error in getVariantBySkuREST:', error.message);
        return null;
    }
}

export { shopifyGraphQL, getProductByHandle, metafieldsSet, listPreorderVariants, getVariantBySku, getVariantBySkuREST, updateVariantInventoryPolicy, getVariantMetafield, deleteVariantMetafield };

// Helper to resolve a variant by InventoryItem ID, including current preorder_limit metafield
export async function getVariantByInventoryItemId(env, inventoryItemId, namespace = 'preorder') {
    const ns = env.METAFIELD_NAMESPACE || namespace;
    const gid = String(inventoryItemId || '').startsWith('gid://')
        ? String(inventoryItemId)
        : `gid://shopify/InventoryItem/${String(inventoryItemId).replace(/[^0-9]/g,'')}`;
    const q = `#graphql
        query VariantByInventoryItem($id: ID!) {
            node(id: $id) {
                ... on InventoryItem {
                    id
                    variant {
                        id
                        sku
                        inventoryPolicy
                        product { id handle title }
                        metafieldLimit: metafield(namespace: "${ns}", key: "preorder_limit") { value }
                        metafieldIsPreorder: metafield(namespace: "${ns}", key: "is_preorder") { value }
                    }
                }
            }
        }
    `;
    const data = await shopifyGraphQL(env, q, { id: gid });
    const node = data && data.node;
    if (!node || !node.variant) return null;
    const v = node.variant;
    const isPreorder = v.metafieldIsPreorder && String(v.metafieldIsPreorder.value).toLowerCase() === 'true';
    return {
        variantId: v.id,
        sku: v.sku,
        productId: v.product?.id || null,
        handle: v.product?.handle || null,
        productTitle: v.product?.title || null,
        preorderLimit: (v.metafieldLimit && v.metafieldLimit.value) || null,
        isPreorder: isPreorder,
        inventoryPolicy: v.inventoryPolicy || null,
        inventoryItemId: gid
    };
}

// Helper to get variant metafield details
async function getVariantMetafield(env, variantId, key, namespace = 'preorder') {
    const ns = env.METAFIELD_NAMESPACE || namespace;
    const q = `#graphql
        query GetVariantMetafield($id: ID!, $key: String!) {
            node(id: $id) {
                ... on ProductVariant {
                    metafield(namespace: "${ns}", key: $key) {
                        id
                        value
                    }
                }
            }
        }
    `;
    const data = await shopifyGraphQL(env, q, { id: variantId, key });
    const node = data && data.node;
    if (!node || !node.metafield) return null;
    return node.metafield;
}

// Helper to update variant inventory policy (e.g., CONTINUE or DENY)
async function updateVariantInventoryPolicy(env, variantId, policy = 'CONTINUE') {
  const apiVersion = env.SHOPIFY_API_VERSION || '2024-10';
  const domain = (env.SHOPIFY_STORE_DOMAIN || '').trim();
  const token = (env.SHOPIFY_ADMIN_ACCESS_TOKEN || '').trim();

  if (!domain || !token) {
    return [{ message: 'Missing Shopify configuration (domain or token)' }];
  }

  // Extract the numeric ID from the Shopify GID
  const match = String(variantId || '').match(/(\d+)$/);
  if (!match) {
    return [{ message: `Unable to parse variant ID from ${variantId}` }];
  }
  const numericId = match[1];

  const store = domain.startsWith('http') ? domain : `https://${domain}`;
  const url = `${store.replace(/\/$/, '')}/admin/api/${apiVersion}/variants/${numericId}.json`;

  const body = {
    variant: {
      id: Number(numericId),
      inventory_policy: String(policy || 'CONTINUE').toLowerCase(),
    },
  };

  try {
    const res = await fetch(url, {
      method: 'PUT',
      headers: {
        'X-Shopify-Access-Token': token,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      let errorDetails = null;
      try {
        errorDetails = await res.json();
      } catch (_) {}
      return [{
        message: `REST update failed: ${res.status} ${res.statusText}`,
        details: errorDetails,
      }];
    }

    return [];
  } catch (error) {
    return [{ message: `REST update network error: ${error.message}` }];
  }
}


