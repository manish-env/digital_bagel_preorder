import { json } from '../utils/http.js';
import { metafieldsSet, updateVariantInventoryPolicy, deleteVariantMetafield } from '../services/shopifyService.js';
import { buildMongoClient } from '../models/mongoClient.js';

export async function updateVariantMetafields(request, env) {
  let payload;
  try {
    payload = await request.json();
  } catch (error) {
    return json({ error: 'Invalid JSON body' }, 400);
  }

  if (!payload || typeof payload !== 'object') {
    return json({ error: 'Payload must be an object' }, 400);
  }

  const {
    variantId,
    isPreorder,
    preorderLimit,
    preorderMessage,
    sku = null,
    productId = null,
    productTitle = null,
    productHandle = null,
    inventoryItemId = null,
  } = payload;

  if (!variantId || typeof variantId !== 'string') {
    return json({ error: 'variantId is required' }, 400);
  }

  const namespace = env.METAFIELD_NAMESPACE || 'preorder';
  const metafields = [];

  if (typeof isPreorder === 'boolean') {
    metafields.push({
      ownerId: variantId,
      namespace,
      key: 'is_preorder',
      type: 'boolean',
      value: isPreorder ? 'true' : 'false',
    });
  }

  if (preorderLimit !== undefined) {
    const limitNumber =
      preorderLimit === null || preorderLimit === ''
        ? 0
        : Number(preorderLimit);

    if (!Number.isFinite(limitNumber) || limitNumber < 0) {
      return json({ error: 'preorderLimit must be a non-negative number' }, 400);
    }

    metafields.push({
      ownerId: variantId,
      namespace,
      key: 'preorder_limit',
      type: 'number_integer',
      value: String(Math.floor(limitNumber)),
    });
  }

  if (preorderMessage !== undefined) {
    const messageValue = preorderMessage === null ? '' : String(preorderMessage);
    metafields.push({
      ownerId: variantId,
      namespace,
      key: 'preorder_message',
      type: 'single_line_text_field',
      value: messageValue,
    });
  }

  if (!metafields.length) {
    return json({ error: 'No metafields were provided to update' }, 400);
  }

  const metafieldErrors = await metafieldsSet(env, metafields);
  if (metafieldErrors && metafieldErrors.length) {
    return json(
      {
        error: 'Failed to update metafields',
        details: metafieldErrors,
      },
      400,
    );
  }

  try {
    const mongo = buildMongoClient(env);
    const setDoc = {
      variantId,
      sku,
      productId,
      productTitle,
      handle: productHandle,
      updatedAt: new Date().toISOString(),
      inventoryItemId,
    };

    if (typeof isPreorder === 'boolean') {
      setDoc.isPreorder = isPreorder;
    }

    if (preorderLimit !== undefined) {
      setDoc.preorderLimit = String(
        preorderLimit === null || preorderLimit === ''
          ? 0
          : Math.floor(Number(preorderLimit)),
      );
    }

    if (preorderMessage !== undefined) {
      setDoc.preorderMessage =
        preorderMessage === null ? '' : String(preorderMessage);
    }

    await mongo.updateOne(
      'variants',
      { variantId },
      {
        $set: setDoc,
      },
      true,
    );
  } catch (_) {
    // Non-fatal: continue even if Mongo update fails
  }

  const responseVariant = { variantId };

  if (typeof isPreorder === 'boolean') {
    responseVariant.isPreorder = isPreorder;
  }

  if (preorderLimit !== undefined) {
    responseVariant.preorderLimit = String(
      preorderLimit === null || preorderLimit === ''
        ? 0
        : Math.floor(Number(preorderLimit)),
    );
  }

  if (preorderMessage !== undefined) {
    responseVariant.preorderMessage =
      preorderMessage === null ? '' : String(preorderMessage);
  }

  return json({
    ok: true,
    variant: responseVariant,
  });
}

export async function deleteVariantPreorder(request, env) {
  let payload;
  try {
    payload = await request.json();
  } catch (error) {
    return json({ error: 'Invalid JSON body' }, 400);
  }

  if (!payload || typeof payload !== 'object') {
    return json({ error: 'Payload must be an object' }, 400);
  }

  const { variantId } = payload;
  if (!variantId || typeof variantId !== 'string') {
    return json({ error: 'variantId is required' }, 400);
  }

  const namespace = env.METAFIELD_NAMESPACE || 'preorder';
  const clearResult = {
    metafieldsCleared: false,
    policyUpdated: false,
    tablesUpdated: false,
    errors: [],
  };

  try {
    const mfErrs = await metafieldsSet(env, [
      { ownerId: variantId, namespace, key: 'is_preorder', type: 'boolean', value: null },
      { ownerId: variantId, namespace, key: 'preorder_limit', type: 'number_integer', value: null },
      { ownerId: variantId, namespace, key: 'preorder_message', type: 'single_line_text_field', value: null },
    ]);
    if (mfErrs && mfErrs.length) {
      clearResult.errors.push({ type: 'metafields', details: mfErrs });
    } else {
      clearResult.metafieldsCleared = true;
    }
  } catch (error) {
    clearResult.errors.push({ type: 'metafields', details: String(error?.message || error) });
  }

  try {
    const policyErrs = await updateVariantInventoryPolicy(env, variantId, 'DENY');
    if (policyErrs && policyErrs.length) {
      clearResult.errors.push({ type: 'policy', details: policyErrs });
    } else {
      clearResult.policyUpdated = true;
    }
  } catch (error) {
    clearResult.errors.push({ type: 'policy', details: String(error?.message || error) });
  }

  try {
    const mongo = buildMongoClient(env);
    await mongo.deleteVariantData(variantId);
    clearResult.tablesUpdated = true;
  } catch (error) {
    clearResult.errors.push({ type: 'database', details: String(error?.message || error) });
  }

  return json({
    ok: clearResult.errors.length === 0,
    result: clearResult,
  }, clearResult.errors.length ? 207 : 200);
}

