import { json } from '../utils/http.js';
import { shopifyGraphQL, listPreorderVariants, metafieldsSet, updateVariantInventoryPolicy } from '../services/shopifyService.js';
import { buildMongoClient } from '../models/mongoClient.js';

export async function registerInventoryWebhook(_request, env) {
    const callbackBase = (env.PUBLIC_BASE_URL || '').trim();
    if (!callbackBase) return json({ error: 'Set PUBLIC_BASE_URL (https URL) for webhooks' }, 400);
    const callbackUrl = `${callbackBase.replace(/\/$/, '')}/webhooks/shopify/inventory`;
    const mutation = `#graphql\n    mutation webhookSubscriptionCreate($topic: WebhookSubscriptionTopic!, $callbackUrl: URL!) {\n        webhookSubscriptionCreate(topic: $topic, webhookSubscription: {callbackUrl: $callbackUrl, format: JSON}) {\n            userErrors { field message }\n            webhookSubscription { id }\n        }\n    }`;
    const data = await shopifyGraphQL(env, mutation, { topic: 'INVENTORY_LEVELS_UPDATE', callbackUrl });
    const payload = data && data.webhookSubscriptionCreate;
    if (!payload || (payload.userErrors && payload.userErrors.length)) {
        return json({ error: 'Failed to register webhook', details: payload && payload.userErrors }, 400);
    }
    return json({ ok: true, id: payload.webhookSubscription.id, callbackUrl });
}

export async function initDb(_request, env) {
    // Touch the client to run ensureSchema
    const db = buildMongoClient(env);
    // Write a tiny row to verify
    try {
        await db.insertOne('uploads', { createdAt: new Date().toISOString(), filename: 'init', status: 'ok', stats: { init: true } });
    } catch (_) {}
    return json({ ok: true });
}

function normalizeNumber(val){
    if(val===undefined || val===null || val==='') return null;
    const n = Number(String(val).replace(/[^0-9.-]/g,''));
    return Number.isFinite(n) ? Math.max(0, Math.floor(n)) : null;
}

function parseCsvOrdered(text){
    text = String(text||'').replace(/^\uFEFF/, '');
    const lines = text.split(/\r?\n/).filter(l=>l.trim().length);
    if(!lines.length) return [];
    const headers = lines[0].split(',').map(h=>h.trim().toLowerCase());
    const skuIdx = headers.findIndex(h=>['sku','variant_sku','variantid','variant_id_sku'].includes(h));
    const qtyIdx = headers.findIndex(h=>['ordered','ordered_qty','qty','quantity'].includes(h));
    if(skuIdx===-1 || qtyIdx===-1) return [];
    const rows = [];
    for(let i=1;i<lines.length;i++){
        const cols = lines[i].split(',');
        const sku = (cols[skuIdx]||'').trim();
        const ordered = normalizeNumber(cols[qtyIdx]);
        if(!sku || ordered===null) continue;
        rows.push({ sku, ordered });
    }
    return rows;
}

export async function uploadOrderedCsv(request, env){
    const mongo = buildMongoClient(env);
    const form = await request.formData();
    const file = form.get('file');
    if(!file) return json({ error: 'CSV file is required' }, 400);
    const text = await file.text();
    const rows = parseCsvOrdered(text);
    let updated = 0; const skipped = [];
    for(const r of rows){
        try{
            // Try to link to an existing variant by SKU if present
            const v = await mongo.findVariantBySku(r.sku);
            await mongo.updateOne('ordered', { sku: r.sku }, { $set: { sku: r.sku, variantId: v?.variantId ?? null, inventoryItemId: v?.inventoryItemId ?? null, ordered: r.ordered, updatedAt: new Date().toISOString() } }, true);
            updated++;
        }catch(e){ skipped.push({ sku: r.sku, error: String(e && e.message ? e.message : e) }); }
    }
    return json({ ok: true, updated, skipped });
}

export async function clearPreorderData(_request, env) {
    const namespace = env.METAFIELD_NAMESPACE || 'preorder';
    const mongo = buildMongoClient(env);
    const summary = {
        variantsProcessed: 0,
        metafieldErrors: [],
        policyErrors: [],
        tablesCleared: {},
    };

    try {
        const variants = await listPreorderVariants(env, 5000);
        for (const variant of variants) {
            const metafields = [
                { ownerId: variant.variantId, namespace, key: 'is_preorder', type: 'boolean', value: null },
                { ownerId: variant.variantId, namespace, key: 'preorder_limit', type: 'number_integer', value: null },
                { ownerId: variant.variantId, namespace, key: 'preorder_message', type: 'single_line_text_field', value: null },
            ];

            try {
                const mfErrs = await metafieldsSet(env, metafields);
                if (mfErrs && mfErrs.length) {
                    summary.metafieldErrors.push({ variantId: variant.variantId, errors: mfErrs });
                    continue;
                }
            } catch (error) {
                summary.metafieldErrors.push({ variantId: variant.variantId, errors: [{ message: String(error?.message || error) }] });
                continue;
            }

            try {
                const policyErrs = await updateVariantInventoryPolicy(env, variant.variantId, 'DENY');
                if (policyErrs && policyErrs.length) {
                    summary.policyErrors.push({ variantId: variant.variantId, errors: policyErrs });
                }
            } catch (error) {
                summary.policyErrors.push({ variantId: variant.variantId, errors: [{ message: String(error?.message || error) }] });
            }

            summary.variantsProcessed++;
        }
    } catch (error) {
        return json({ error: 'Failed to clear Shopify preorder data', details: String(error?.message || error) }, 500);
    }

    try {
        summary.tablesCleared = await mongo.clearPreorderDataTables();
    } catch (error) {
        summary.tablesClearedError = String(error?.message || error);
    }

    return json({ ok: true, ...summary });
}


