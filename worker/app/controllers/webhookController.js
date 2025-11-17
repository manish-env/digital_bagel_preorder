import { buildMongoClient } from '../models/mongoClient.js';
import { metafieldsSet, getVariantByInventoryItemId, updateVariantInventoryPolicy } from '../services/shopifyService.js';

export async function shopifyInventoryWebhook(request, env) {
  const secret = (env.SHOPIFY_WEBHOOK_SECRET || '').trim();
  if (!secret) return new Response('Missing secret', { status: 500 });

  const hmacHeader = request.headers.get('x-shopify-hmac-sha256');
  if (!hmacHeader) return new Response('Unauthorized', { status: 401 });

  const bodyBuffer = await request.arrayBuffer();
  const bodyText = new TextDecoder().decode(bodyBuffer);

  // Compute HMAC verification
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    'raw',
    encoder.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );
  const signature = await crypto.subtle.sign('HMAC', key, encoder.encode(bodyText));
  const computedHmac = btoa(String.fromCharCode(...new Uint8Array(signature)));

  if (computedHmac !== hmacHeader) {
    return new Response('Unauthorized', { status: 401 });
  }

  const payload = JSON.parse(bodyText);
  const mongo = buildMongoClient(env);

  const { inventory_item_id, available, location_id } = payload;

  console.log('🔔 WEBHOOK FIRED:', { inventory_item_id, available, location_id });

  try {
    await mongo.insertOne('webhook_debug', {
      inventory_item_id,
      available,
      location_id,
      receivedAt: new Date().toISOString(),
      headers: {
        'x-shopify-hmac-sha256': hmacHeader,
        'x-shopify-shop-domain': request.headers.get('x-shopify-shop-domain'),
        'x-shopify-topic': request.headers.get('x-shopify-topic'),
      },
      rawBody: payload,
    });
  } catch (_) {
    // Debug write failures shouldn't break webhook handling
  }

  // Get last known stock
  const lastRecord = await mongo.findOne('inventory_levels', { inventory_item_id });
  const oldStock = lastRecord ? Number(lastRecord.available || 0) : 0;
  const newStock = Number(available || 0);

  // Detect replenishment and compute delta
  let delta = 0;
  if (newStock > oldStock) {
    delta = newStock - oldStock;
    try {
      await mongo.insertOne('inventory_restock_events', {
        inventory_item_id,
        location_id,
        oldStock,
        newStock,
        delta,
        receivedAt: new Date().toISOString(),
        source: request.headers.get('x-shopify-shop-domain'),
      });
    } catch (_) {}
  }

  // Update latest stock snapshot
  await mongo.updateOne(
    'inventory_levels',
    { inventory_item_id },
    { $set: { available: newStock, updatedAt: new Date().toISOString() } },
    true
  );

  // Get variant info for preorder operations (only if stock changed or became 0)
  let resolved = null;
  if (newStock === 0 && oldStock > 0) {
    // Stock became 0 - check if preorder and update inventory policy
    try {
      resolved = await getVariantByInventoryItemId(env, inventory_item_id);
      const preorderLimitNum = Number(resolved?.preorderLimit || 0);

      if (resolved && resolved.variantId && resolved.isPreorder && preorderLimitNum > 0) {
        console.log(`Preorder variant ${resolved.variantId} has limit ${preorderLimitNum}, current policy: ${resolved.inventoryPolicy}`);
        // Only update if current policy is not CONTINUE (check both cases)
        if (resolved.inventoryPolicy !== 'CONTINUE' && resolved.inventoryPolicy !== 'continue') {
          console.log('Attempting to update inventory policy to CONTINUE...');
          const policyErrs = await updateVariantInventoryPolicy(env, resolved.variantId, 'CONTINUE');
          console.log('Policy update errors:', policyErrs);
          if (!policyErrs || !policyErrs.length) {
            console.log('✅ Inventory policy updated to CONTINUE for preorder variant');
          } else {
            console.log('❌ Policy update failed:', policyErrs);
          }
        } else {
          console.log('Policy already set to CONTINUE');
        }
      } else {
        console.log(`Variant not eligible: isPreorder=${resolved?.isPreorder}, limit=${preorderLimitNum}`);
      }
    } catch (error) {
      console.error('Error in preorder logic:', error.message);
    }
  }

  // Compute preorder limit for related variants: always fetch current value from Shopify and subtract delta
  try {
    if (delta > 0) {
      // Reuse resolved if we already fetched it, otherwise fetch now
      if (!resolved) {
        resolved = await getVariantByInventoryItemId(env, inventory_item_id);
      }
      if (resolved && resolved.variantId) {
        const currentLimitNum = Number((resolved.preorderLimit ?? '0'));
        const currentLimit = Number.isFinite(currentLimitNum) ? currentLimitNum : 0;
        const limit = Math.max(0, currentLimit - delta);
        const namespace = env.METAFIELD_NAMESPACE || 'preorder';
        const errs = await metafieldsSet(env, [{ ownerId: resolved.variantId, namespace, key:'preorder_limit', type:'number_integer', value: String(limit) }]);
        if (!errs || !errs.length) {
          await mongo.updateOne('variants', { variantId: resolved.variantId }, { $set: { preorderLimit: String(limit), updatedAt: new Date().toISOString(), sku: resolved.sku ?? null, handle: resolved.handle ?? null, inventoryItemId: resolved.inventoryItemId ?? null, productId: resolved.productId ?? null } }, true);
        }
      }
    }
  } catch (_) {}

  return new Response('OK', { status: 200 });
}

