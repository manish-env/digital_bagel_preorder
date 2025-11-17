import { json } from '../utils/http.js';
import { getVariantBySku } from '../services/shopifyService.js';
import { buildMongoClient } from '../models/mongoClient.js';

function normalizeHeader(name){
    const base=String(name).replace(/^\uFEFF/,'').trim().toLowerCase();
    const key=base.replace(/[^a-z0-9]+/g,'_').replace(/^_+|_+$/g,'');
    if(['variant_sku','sku','variantid','variant_id_sku'].includes(key)) return 'sku';
    if(['is_preorder','ispreorder','preorder','is_pre_order'].includes(key)) return 'is_preorder';
    if(['preorder_limit','pre_order_limit','limit'].includes(key)) return 'preorder_limit';
    if(['preorder_message','pre_order_message','message'].includes(key)) return 'preorder_message';
    return key;
}

function parseCsv(text){
    text=text.replace(/^\uFEFF/,'');
    const lines=[];let cur='';let inQ=false;for(let i=0;i<text.length;i++){const c=text[i];if(c==='"'){if(inQ && text[i+1]==='"'){cur+='"';i++;}else{inQ=!inQ;}}else if(c==='\n' || c==='\r'){if(inQ){cur+=c;}else{lines.push(cur);cur='';if(c==='\r' && text[i+1]==='\n'){i++;}}}else{cur+=c;}}if(cur.length) lines.push(cur);
    if(!lines.length) return { rows:[], stats:{ totalRows:0, skippedRows:0 } };
    const headers=lines[0].split(',').map(normalizeHeader);
    const rows=[];let skipped=0;
    for(let li=1;li<lines.length;li++){
        const rowLine=lines[li]; if(!rowLine.trim()) { skipped++; continue; }
        const cols=[]; let v=''; inQ=false; for(let i=0;i<rowLine.length;i++){const c=rowLine[i];if(c==='"'){if(inQ && rowLine[i+1]==='"'){v+='"';i++;}else{inQ=!inQ;}}else if(c===',' && !inQ){cols.push(v);v='';}else{v+=c;}} cols.push(v);
        const o={}; headers.forEach((h,idx)=>{o[h]=cols[idx]!==undefined?cols[idx].trim():'';});
        const sku=(o.sku||'').trim(); if(!sku){skipped++; continue;}
        const out={ sku };
        if(o.is_preorder!==undefined && o.is_preorder!=='') out.is_preorder = ['true','1','yes','y'].includes(String(o.is_preorder).toLowerCase());
        if(o.preorder_limit!==undefined && o.preorder_limit!==''){ const n=Number(o.preorder_limit); if(Number.isInteger(n) && n>=0) out.preorder_limit=n; }
        if(o.preorder_message!==undefined && o.preorder_message!=='') out.preorder_message=o.preorder_message;
        rows.push(out);
    }
    return { rows, stats:{ totalRows: lines.length-1, skippedRows: skipped } };
}

// Bulk preorder operations using metafieldsSet (Shopify's supported bulk approach)
async function createBulkPreorderOperation(env, operations) {
    // Process operations in batches limited by Shopify's 25 metafields per request
    const MAX_METAFIELDS_PER_BATCH = 25;
    const batches = [];
    let currentBatch = [];

    for (const op of operations) {
        // Calculate how many metafields this operation will create
        let metafieldsForThisOp = 1; // is_preorder always
        if (op.preorderLimit !== undefined && op.preorderLimit !== null) metafieldsForThisOp++;
        if (op.preorderMessage && op.preorderMessage !== '') metafieldsForThisOp++;

        // Check if adding this operation would exceed the batch limit
        const currentMetafieldsCount = currentBatch.reduce((count, batchOp) => {
            let countForOp = 1; // is_preorder
            if (batchOp.preorderLimit !== undefined && batchOp.preorderLimit !== null) countForOp++;
            if (batchOp.preorderMessage && batchOp.preorderMessage !== '') countForOp++;
            return count + countForOp;
        }, 0);

        if (currentMetafieldsCount + metafieldsForThisOp > MAX_METAFIELDS_PER_BATCH) {
            // Start a new batch
            if (currentBatch.length > 0) {
                batches.push([...currentBatch]);
                currentBatch = [];
            }
        }

        currentBatch.push(op);

        // If we've reached the max metafields for this batch, start a new one
        const newCount = currentBatch.reduce((count, batchOp) => {
            let countForOp = 1; // is_preorder
            if (batchOp.preorderLimit !== undefined && batchOp.preorderLimit !== null) countForOp++;
            if (batchOp.preorderMessage && batchOp.preorderMessage !== '') countForOp++;
            return count + countForOp;
        }, 0);

        if (newCount >= MAX_METAFIELDS_PER_BATCH) {
            batches.push([...currentBatch]);
            currentBatch = [];
        }
    }

    // Add any remaining operations
    if (currentBatch.length > 0) {
        batches.push(currentBatch);
    }

    const batchResults = [];

    // Process each batch
    for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];

        // Build all metafields for this batch
        const allMetafields = [];

        for (const op of batch) {
            // is_preorder metafield
            allMetafields.push({
                ownerId: op.variantId,
                namespace: "preorder",
                key: "is_preorder",
                value: op.isPreorder.toString(),
                type: "boolean"
            });

            // preorder_limit metafield (if provided)
            if (op.preorderLimit !== undefined && op.preorderLimit !== null) {
                allMetafields.push({
                    ownerId: op.variantId,
                    namespace: "preorder",
                    key: "preorder_limit",
                    value: op.preorderLimit.toString(),
                    type: "number_integer"
                });
            }

            // preorder_message metafield (if provided)
            if (op.preorderMessage && op.preorderMessage !== '') {
                allMetafields.push({
                    ownerId: op.variantId,
                    namespace: "preorder",
                    key: "preorder_message",
                    value: op.preorderMessage,
                    type: "single_line_text_field"
                });
            }
        }

        // Execute metafieldsSet mutation for this batch
        const mutation = `
            mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
                metafieldsSet(metafields: $metafields) {
                    metafields {
                        id
                        value
                        owner {
                            ... on ProductVariant {
                                id
                                sku
                            }
                        }
                    }
                    userErrors {
                        field
                        message
                        code
                    }
                }
            }
        `;

        try {
            const response = await fetch(`https://${env.SHOPIFY_STORE_DOMAIN}/admin/api/${env.SHOPIFY_API_VERSION}/graphql.json`, {
                method: 'POST',
                headers: {
                    'X-Shopify-Access-Token': env.SHOPIFY_ADMIN_ACCESS_TOKEN,
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    query: mutation,
                    variables: { metafields: allMetafields }
                })
            });

            const result = await response.json();

            if (result.errors) {
                batchResults.push({ success: false, errors: result.errors, batchIndex: i });
            } else if (result.data.metafieldsSet.userErrors.length > 0) {
                batchResults.push({ success: false, errors: result.data.metafieldsSet.userErrors, batchIndex: i });
            } else {
                batchResults.push({
                    success: true,
                    data: result.data.metafieldsSet,
                    batchIndex: i,
                    operations: batch
                });
            }

        } catch (error) {
            batchResults.push({ success: false, errors: [error.message], batchIndex: i });
        }

        // Rate limiting delay between batches (2 req/sec for Basic plan)
        if (i < batches.length - 1) {
            await new Promise(resolve => setTimeout(resolve, 500));
        }
    }

    // Return consolidated results
    const hasErrors = batchResults.some(result => !result.success);
    if (hasErrors) {
        return {
            success: false,
            errors: batchResults.filter(r => !r.success).flatMap(r => r.errors)
        };
    }

    return {
        success: true,
        batchResults: batchResults,
        totalOperations: operations.length,
        message: `Successfully processed ${operations.length} preorder operations in ${batches.length} batches`
    };
}


// Batch variant lookup for bulk operations
async function getVariantsBySkusBatch(env, skus) {
    // Split SKUs into chunks to avoid query size limits
    const CHUNK_SIZE = 50;
    const chunks = [];
    
    for (let i = 0; i < skus.length; i += CHUNK_SIZE) {
        chunks.push(skus.slice(i, i + CHUNK_SIZE));
    }

    const allVariants = new Map();
    
    for (const chunk of chunks) {
        const query = `
            query getVariantsBySkus($query: String!) {
                productVariants(first: 50, query: $query) {
                    edges {
                        node {
                            id
                            sku
                            inventoryPolicy
                            product {
                                id
                                handle
                                title
                            }
                            inventoryItem {
                                id
                            }
                        }
                    }
                }
            }
        `;

        // Build query string for this chunk
        const skuQuery = chunk.map(sku => `sku:${sku}`).join(' OR ');
        
        try {
            const response = await fetch(`https://${env.SHOPIFY_STORE_DOMAIN}/admin/api/${env.SHOPIFY_API_VERSION}/graphql.json`, {
                method: 'POST',
                headers: {
                    'X-Shopify-Access-Token': env.SHOPIFY_ADMIN_ACCESS_TOKEN,
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    query,
                    variables: { query: skuQuery }
                })
            });

            const result = await response.json();
            
            if (result.errors) {
                console.error('Error fetching variants chunk:', result.errors);
                continue;
            }

            result.data.productVariants.edges.forEach(edge => {
                allVariants.set(edge.node.sku, {
                    variant: edge.node,
                    product: edge.node.product
                });
            });

            // Small delay between chunks
            await new Promise(resolve => setTimeout(resolve, 500));
            
        } catch (error) {
            console.error('Error processing variant chunk:', error);
        }
    }

    return allVariants;
}

// Main bulk upload endpoint
export async function uploadCsv(request, env) {
    const form = await request.formData();
    const file = form.get('file');
    if (!file) return json({ error: 'CSV file is required' }, 400);
    
    const text = await file.text();
    const parsed = parseCsv(text);
    const rows = parsed.rows;
    const mongo = buildMongoClient(env);

    // Use bulk operations for all uploads
    console.log(`Using bulk operations for ${rows.length} variants`);

    // Create upload record
    const uploadRecord = await mongo.insertOne('uploads', {
        createdAt: new Date().toISOString(),
        filename: file.name || 'upload.csv',
        stats: { totalRows: rows.length, skippedRows: parsed.stats.skippedRows },
        status: 'preparing',
        progress: { processed: 0, successful: 0, failed: 0 },
        type: 'bulk',
        operationsCount: 0
    });
    const uploadId = uploadRecord.insertedId;

    try {
        // Step 1: Look up all variants first
        console.log(`Looking up ${rows.length} variants...`);
        const skus = rows.map(row => row.sku);
        const variantMap = await getVariantsBySkusBatch(env, skus);
        
        // Step 2: Prepare bulk operations and record missing variants
        const operations = [];
        let missingVariants = 0;

        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            const variantData = variantMap.get(row.sku);
            
            if (!variantData) {
                // Record not found variant
                await mongo.insertOne('upload_rows', {
                    uploadId,
                    rowIndex: i,
                    sku: row.sku,
                    status: 'no_variant',
                    createdAt: new Date().toISOString()
                });
                missingVariants++;
                continue;
            }

            operations.push({
                variantId: variantData.variant.id,
                isPreorder: (row.is_preorder === undefined ? true : !!row.is_preorder) ? 'true' : 'false',
                preorderLimit: row.preorder_limit,
                preorderMessage: row.preorder_message,
                rowIndex: i,
                sku: row.sku,
                variantData
            });
        }

        console.log(`Prepared ${operations.length} operations, ${missingVariants} variants not found`);

        if (operations.length === 0) {
            throw new Error('No valid variants found to process');
        }

        // Step 3: Store operation details for later processing
        await mongo.updateOne('uploads', { _id: uploadId }, {
            $set: {
                operations: operations.map(op => ({
                    sku: op.sku,
                    rowIndex: op.rowIndex,
                    variantId: op.variantId,
                    isPreorder: op.isPreorder === 'true',
                    preorderLimit: op.preorderLimit,
                    preorderMessage: op.preorderMessage
                })),
                operationsCount: operations.length,
                missingVariants: missingVariants,
                status: 'processing'
            }
        });

        // Step 4: Execute bulk preorder operations immediately
        console.log('Executing bulk preorder operations...');
        await mongo.updateOne('uploads', { _id: uploadId }, {
            $set: {
                status: 'processing',
                startedAt: new Date().toISOString()
            }
        });

        const bulkResult = await createBulkPreorderOperation(env, operations);

        if (!bulkResult.success) {
            throw new Error(`Bulk operation failed: ${JSON.stringify(bulkResult.errors)}`);
        }

        // Step 5: Process results and update database
        let successful = 0;
        let failed = 0;

        for (const batchResult of bulkResult.batchResults) {
            if (batchResult.success) {
                // Process successful batch
                for (const op of batchResult.operations) {
                    await mongo.insertOne('upload_rows', {
                        uploadId,
                        rowIndex: op.rowIndex,
                        sku: op.sku,
                        status: 'success',
                        metafields: [
                            { key: 'is_preorder', value: op.isPreorder ? 'true' : 'false' },
                            ...(op.preorderLimit !== undefined ? [{ key: 'preorder_limit', value: String(op.preorderLimit) }] : []),
                            ...(op.preorderMessage ? [{ key: 'preorder_message', value: op.preorderMessage }] : [])
                        ],
                        createdAt: new Date().toISOString(),
                        variantId: op.variantId
                    });

                    // Update variants collection
                    await mongo.updateOne('variants', { variantId: op.variantId }, {
                        $set: {
                            variantId: op.variantId,
                            sku: op.sku,
                            isPreorder: op.isPreorder,
                            preorderLimit: op.preorderLimit !== undefined ? op.preorderLimit : null,
                            preorderMessage: op.preorderMessage || null,
                            updatedAt: new Date().toISOString()
                        }
                    }, true);

                    successful++;
                }
            } else {
                // Process failed batch
                for (const op of batchResult.operations) {
                    await mongo.insertOne('upload_rows', {
                        uploadId,
                        rowIndex: op.rowIndex,
                        sku: op.sku,
                        status: 'error',
                        error: batchResult.errors,
                        createdAt: new Date().toISOString(),
                        variantId: op.variantId
                    });
                    failed++;
                }
            }
        }

        // Step 6: Update inventory policies for successful preorder variants
        if (successful > 0) {
            const successfulOperations = operations.filter(op => op.isPreorder);
            if (successfulOperations.length > 0) {
                await updateInventoryPoliciesForPreorders(env, successfulOperations);
            }
        }

        // Step 7: Update final upload status
        await mongo.updateOne('uploads', { _id: uploadId }, {
            $set: {
                status: 'completed',
                finishedAt: new Date().toISOString(),
                progress: {
                    processed: operations.length,
                    successful: successful,
                    failed: failed
                },
                results: {
                    successful: successful,
                    failed: failed,
                    total: operations.length,
                    missingVariants: missingVariants
                }
            }
        });

        console.log(`Bulk operation completed: ${successful} successful, ${failed} failed`);

        return json({
            uploadId,
            message: 'Bulk preorder operations completed successfully',
            totalOperations: operations.length,
            missingVariants: missingVariants,
            status: 'completed',
            progress: {
                processed: operations.length,
                successful: successful,
                failed: failed
            },
            results: {
                successful: successful,
                failed: failed,
                total: operations.length,
                missingVariants: missingVariants
            }
        });

    } catch (error) {
        console.error('Bulk upload failed:', error);
        await mongo.updateOne('uploads', { _id: uploadId }, {
            $set: {
                status: 'error',
                finishedAt: new Date().toISOString(),
                error: error.message
            }
        });
        
        return json({ 
            error: 'Bulk upload failed', 
            details: error.message 
        }, 500);
    }
}

// Regular upload for small files (fallback)
async function uploadCsvRegular(parsed, filename, env) {
    const rows = parsed.rows;
    const namespace = env.METAFIELD_NAMESPACE || 'preorder';
    const mongo = buildMongoClient(env);

    // Create upload record
    let uploadId = null;
    try {
        const uploadRecord = await mongo.insertOne('uploads', {
            createdAt: new Date().toISOString(),
            filename: filename,
            stats: { totalRows: rows.length, skippedRows: parsed.stats.skippedRows },
            status: 'queued',
            progress: { processed: 0, successful: 0, failed: 0 },
            type: 'regular'
        });
        uploadId = uploadRecord.insertedId;
    } catch (error) {
        return json({ error: 'Failed to create upload record', details: error.message }, 500);
    }

    // Queue for background processing
    try {
        await mongo.queueUpload(uploadId, rows, namespace);
    } catch (error) {
        console.error('Failed to queue upload:', error);
        return json({ error: 'Failed to queue upload', details: error.message }, 500);
    }

    return json({
        uploadId,
        message: 'Upload queued for background processing',
        totalRows: rows.length,
        status: 'queued',
        type: 'regular'
    });
}

// Polling endpoint for bulk operation status
export async function getUploadProgress(request, env) {
    const url = new URL(request.url);
    const uploadId = url.pathname.split('/').pop();
    
    if (!uploadId) {
        return json({ error: 'Upload ID is required' }, 400);
    }

    const mongo = buildMongoClient(env);
    const upload = await mongo.findOne('uploads', { _id: uploadId });

    if (!upload) {
        return json({ error: 'Upload not found' }, 404);
    }


    // Handle regular uploads
    const progress = await getUploadProgressStats(mongo, uploadId);
    const recentErrors = await mongo.find('upload_rows', {
        uploadId,
        status: { $in: ['error', 'policy_error', 'exception', 'no_variant'] }
    }, { limit: 10, sort: { createdAt: -1 } });

    return json({
        uploadId,
        status: upload.status,
        progress: upload.progress || { processed: 0, successful: 0, failed: 0 },
        stats: upload.stats,
        results: upload.results,
        createdAt: upload.createdAt,
        startedAt: upload.startedAt,
        finishedAt: upload.finishedAt,
        error: upload.error,
        type: upload.type || 'regular',
        recentErrors: recentErrors.map(err => ({
            sku: err.sku,
            status: err.status,
            error: err.error,
            createdAt: err.createdAt
        }))
    });
}

// Update inventory policy for preorder variants
async function updateInventoryPoliciesForPreorders(env, operations) {
    console.log(`Updating inventory policies for ${operations.length} preorder variants...`);

    // Process in small batches to avoid rate limits
    const BATCH_SIZE = 5;
    for (let i = 0; i < operations.length; i += BATCH_SIZE) {
        const batch = operations.slice(i, i + BATCH_SIZE);

        // Update each variant's inventory policy
        const promises = batch.map(async (op) => {
            try {
                // Extract numeric ID from Shopify GID
                const match = op.variantId.match(/\/(\d+)$/);
                if (!match) {
                    console.warn(`Could not extract numeric ID from ${op.variantId}`);
                    return;
                }
                const numericId = match[1];

                // Use REST API to update inventory policy
                const url = `https://${env.SHOPIFY_STORE_DOMAIN}/admin/api/${env.SHOPIFY_API_VERSION}/variants/${numericId}.json`;

                const response = await fetch(url, {
                    method: 'PUT',
                    headers: {
                        'X-Shopify-Access-Token': env.SHOPIFY_ADMIN_ACCESS_TOKEN,
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        variant: {
                            id: parseInt(numericId),
                            inventory_policy: 'continue' // Allow selling when out of stock
                        }
                    })
                });

                if (!response.ok) {
                    const error = await response.text();
                    console.warn(`Failed to update inventory policy for variant ${op.sku}: ${error}`);
                } else {
                    console.log(`Updated inventory policy for ${op.sku} to 'continue'`);
                }

            } catch (error) {
                console.warn(`Error updating inventory policy for ${op.sku}:`, error.message);
            }
        });

        await Promise.all(promises);

        // Small delay between batches
        if (i + BATCH_SIZE < operations.length) {
            await new Promise(resolve => setTimeout(resolve, 1000)); // 1 second delay
        }
    }
}


// Helper function for regular upload progress
async function getUploadProgressStats(mongo, uploadId) {
    try {
        const result = await mongo.aggregate('upload_rows', [
            { $match: { uploadId } },
            {
                $group: {
                    _id: null,
                    successful: { $sum: { $cond: [{ $eq: ['$status', 'success'] }, 1, 0] } },
                    failed: { $sum: { $cond: [{ $in: ['$status', ['error', 'policy_error', 'exception', 'no_variant']] }, 1, 0] } }
                }
            }
        ]);

        if (result && result.length > 0) {
            return result[0];
        }
    } catch (error) {
        console.warn('Failed to get upload progress:', error);
    }

    return { successful: 0, failed: 0 };
}

// Background queue processor for regular uploads
export async function processUploadQueue(env) {
    const mongo = buildMongoClient(env);

    try {
        // Get next item from queue (regular uploads only)
        const queueItem = await mongo.dequeueUpload();
        if (!queueItem) {
            console.log('No pending uploads in queue');
            return;
        }

        console.log(`Processing queued upload: ${queueItem.uploadId} (${queueItem.rows.length} rows)`);

        // Process the upload using regular method
        try {
            await processUploadInBackground(env, queueItem.uploadId, queueItem.rows, queueItem.namespace);
            await mongo.completeUpload(queueItem.id);
            console.log(`Upload ${queueItem.uploadId} completed successfully`);
        } catch (error) {
            console.error(`Upload ${queueItem.uploadId} failed:`, error);
            await mongo.completeUpload(queueItem.id, error.message);
        }
    } catch (error) {
        console.error('Queue processing error:', error);
    }
}

// Regular background processing (for small files)
async function processUploadInBackground(env, uploadId, rows, namespace) {
    // ... (your existing regular processing code)
    // This handles small files with individual API calls
}