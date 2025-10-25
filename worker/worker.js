import { buildMongoClient } from './mongo.js';

async function shopifyGraphQL(env, query, variables) {
	const apiVersion = env.SHOPIFY_API_VERSION || '2024-10';
	const store = env.SHOPIFY_STORE_DOMAIN.startsWith('http') ? env.SHOPIFY_STORE_DOMAIN : `https://${env.SHOPIFY_STORE_DOMAIN}`;
	const url = `${store.replace(/\/$/,'')}/admin/api/${apiVersion}/graphql.json`;
	const res = await fetch(url, { method: 'POST', headers: { 'X-Shopify-Access-Token': env.SHOPIFY_ADMIN_ACCESS_TOKEN, 'Content-Type': 'application/json' }, body: JSON.stringify({ query, variables }) });
	const data = await res.json();
	if (data.errors) throw new Error(JSON.stringify(data.errors));
	return data.data;
}

function normalizeHeader(name){
	const base=String(name).replace(/^\uFEFF/,'').trim().toLowerCase();
	const key=base.replace(/[^a-z0-9]+/g,'_').replace(/^_+|_+$/g,'');
	if(['variant_sku','sku','variantid','variant_id_sku'].includes(key)) return 'sku';
	if(['handle','product_handle','handle_url'].includes(key)) return 'handle';
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
		const handle=(o.handle||'').trim(); const sku=(o.sku||'').trim(); if(!handle||!sku){skipped++; continue;}
		const out={ handle, sku };
		if(o.is_preorder!==undefined && o.is_preorder!=='') out.is_preorder = ['true','1','yes','y'].includes(String(o.is_preorder).toLowerCase());
		if(o.preorder_limit!==undefined && o.preorder_limit!==''){ const n=Number(o.preorder_limit); if(Number.isInteger(n) && n>=0) out.preorder_limit=n; }
		if(o.preorder_message!==undefined && o.preorder_message!=='') out.preorder_message=o.preorder_message;
		rows.push(out);
	}
	return { rows, stats:{ totalRows: lines.length-1, skippedRows: skipped } };
}

async function getProductByHandle(env, handle){
	const q=`#graphql
		query ProductByHandle($handle: String!) {
			productByHandle(handle: $handle) {
				id title
				variants(first: 250) { edges { node { id sku } } }
			}
		}
	`;
	const data = await shopifyGraphQL(env, q, { handle });
	return data.productByHandle;
}

async function metafieldsSet(env, inputs){
	const m=`#graphql
		mutation Set($metafields: [MetafieldsSetInput!]!) {
			metafieldsSet(metafields: $metafields) { userErrors { field message code } }
		}
	`;
	const data = await shopifyGraphQL(env, m, { metafields: inputs });
	return data.metafieldsSet.userErrors || [];
}

async function listPreorderVariants(env, limit=1000){
	const ns = env.METAFIELD_NAMESPACE || 'preorder';
	const q=`#graphql
		query Products($after: String){
			products(first: 50, after: $after, sortKey: TITLE){
				edges{ cursor node{ id title handle variants(first: 100){ edges{ node{ id sku title inventoryQuantity metafield(namespace: "${ns}", key: "is_preorder"){ value } metafieldPreMsg: metafield(namespace: "${ns}", key: "preorder_message"){ value } metafieldPreLimit: metafield(namespace: "${ns}", key: "preorder_limit"){ value } } } } }
				pageInfo{ hasNextPage }
			}
		}
	`;
	const out=[]; let cursor=null; let hasNext=true;
	while(hasNext && out.length<limit){
		const data = await shopifyGraphQL(env, q, { after: cursor });
		const page = data.products; for(const edge of page.edges){ cursor=edge.cursor; const p=edge.node; const vars=(p.variants?.edges||[]).map(e=>e.node); for(const v of vars){ const isPre = v.metafield && String(v.metafield.value).toLowerCase()==='true'; if(!isPre) continue; out.push({ productId:p.id, productTitle:p.title, productHandle:p.handle, variantId:v.id, variantTitle:v.title, sku:v.sku, stockAvailable: typeof v.inventoryQuantity==='number'?v.inventoryQuantity:undefined, preorderMessage: v.metafieldPreMsg?.value||'', preorderLimit: v.metafieldPreLimit?.value||'' }); if(out.length>=limit) break; } if(out.length>=limit) break; } hasNext = page.pageInfo?.hasNextPage; }
	return out;
}

async function handleUpload(request, env){
	const form = await request.formData();
	const file = form.get('file'); if(!file) return json({ error:'CSV file is required' }, 400);
	const text = await file.text();
	const parsed = parseCsv(text); const rows = parsed.rows;
	const namespace = env.METAFIELD_NAMESPACE || 'preorder';
	const mongo = buildMongoClient(env);
	let uploadId=null;
	try { const up = await mongo.insertOne('uploads', { createdAt: new Date().toISOString(), filename: file.name || 'upload.csv', stats: { totalRows: rows.length, skippedRows: parsed.stats.skippedRows }, status: 'processing' }); uploadId = up.insertedId; } catch (_) {}

	const productCache = new Map();
	const limit = Number(env.CONCURRENCY || 5);
	let idx=0; let results={ totalRows: rows.length, skippedRows: parsed.stats.skippedRows, successCount:0, notFoundProduct:[], notFoundVariant:[], errors:[] };
	async function next(){
		const i=idx++; if(i>=rows.length) return;
		const row=rows[i];
		const handle=row.handle; const sku=row.sku;
		try{
			let product = productCache.has(handle)?productCache.get(handle):undefined;
			if(product===undefined){ product = await getProductByHandle(env, handle); productCache.set(handle, product || null); }
			if(!product){ results.notFoundProduct.push({ handle }); try{ await mongo.insertOne('upload_rows', { uploadId, handle, sku, status:'no_product', createdAt: new Date().toISOString() }); } catch(_){} return; }
			const variants=(product.variants?.edges||[]).map(e=>e.node);
			const variant = variants.find(v => (v.sku||'').trim()===sku.trim());
			if(!variant){ results.notFoundVariant.push({ handle, sku }); try{ await mongo.insertOne('upload_rows', { uploadId, handle, sku, status:'no_variant', createdAt: new Date().toISOString(), productId: product.id }); } catch(_){} return; }
			const isPreorder = (row.is_preorder===undefined? true : !!row.is_preorder);
			const metafields=[];
			metafields.push({ ownerId: variant.id, namespace, key:'is_preorder', type:'boolean', value: isPreorder?'true':'false' });
			if(row.preorder_limit!==undefined && row.preorder_limit!==null) metafields.push({ ownerId: variant.id, namespace, key:'preorder_limit', type:'number_integer', value: String(row.preorder_limit) });
			if(row.preorder_message!==undefined) metafields.push({ ownerId: variant.id, namespace, key:'preorder_message', type:'single_line_text_field', value: row.preorder_message });
			const errs = await metafieldsSet(env, metafields);
			if(errs && errs.length){ results.errors.push({ handle, sku, message: JSON.stringify(errs) }); try{ await mongo.insertOne('upload_rows', { uploadId, handle, sku, status:'error', error: errs, createdAt: new Date().toISOString(), variantId: variant.id }); } catch(_){} return; }
			results.successCount++;
			try{ await mongo.updateOne('variants', { variantId: variant.id }, { $set: { variantId: variant.id, productId: product.id, handle, sku, isPreorder, preorderLimit: (metafields.find(m=>m.key==='preorder_limit')||{}).value||null, preorderMessage: (metafields.find(m=>m.key==='preorder_message')||{}).value, updatedAt: new Date().toISOString() } }, true); } catch(_){ }
			try{ await mongo.insertOne('upload_rows', { uploadId, handle, sku, status:'updated', metafields, createdAt: new Date().toISOString(), variantId: variant.id }); } catch(_){ }
		}catch(e){ results.errors.push({ handle, sku, message: String(e && e.message ? e.message : e) }); try{ await mongo.insertOne('upload_rows', { uploadId, handle, sku, status:'exception', error: String(e && e.message ? e.message : e), createdAt: new Date().toISOString() }); } catch(_){} }
	}
	const workers = Array.from({length: Math.min(limit, rows.length)}, () => next());
	await Promise.all(workers);
	while(idx < rows.length){ await next(); }
	try{ if(uploadId) await mongo.updateOne('uploads', { _id: uploadId }, { $set: { status:'done', finishedAt: new Date().toISOString(), results } }, false); } catch(_){ }
	return json({ uploadId, ...results });
}

function json(obj, status=200){ return new Response(JSON.stringify(obj), { status, headers: { 'content-type':'application/json' } }); }

export default { async fetch(request, env) {
	const url = new URL(request.url);
	if (url.pathname === '/health') return json({ status:'ok' });
	if (url.pathname === '/' || url.pathname === '/index.html' || url.pathname === '/upload.html') {
		if (!env.ASSETS) return new Response('Assets binding missing', { status: 500 });
		const path = url.pathname === '/' ? '/index.html' : url.pathname;
		const asset = await env.ASSETS.fetch(new Request(new URL(path, 'http://assets')));
		return new Response(asset.body, asset);
	}
	if (url.pathname === '/api/preorder-products') {
		try { const variants = await listPreorderVariants(env, Math.min(Number(url.searchParams.get('limit')||'1000'), 2000)); return json({ count: variants.length, variants }); } catch(e) { return json({ error:'Failed to load preorder variants', details: String(e && e.message ? e.message : e) }, 500); }
	}
	if (url.pathname === '/upload' && request.method === 'POST') { try { return await handleUpload(request, env); } catch(e) { return json({ error:'Failed to process upload', details: String(e && e.message ? e.message : e) }, 500); } }
	if (url.pathname === '/webhooks/shopify/inventory' && request.method === 'POST') {
		const secret = env.SHOPIFY_WEBHOOK_SECRET; if (!secret) return new Response('Missing secret', { status: 500 });
		const hmac = request.headers.get('x-shopify-hmac-sha256'); if (!hmac) return new Response('Unauthorized', { status: 401 });
		const body = await request.arrayBuffer();
		const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(secret), { name:'HMAC', hash:'SHA-256' }, false, ['sign']);
		const sig = await crypto.subtle.sign('HMAC', key, body); const digest = btoa(String.fromCharCode(...new Uint8Array(sig)));
		if (digest !== hmac) return new Response('Unauthorized', { status: 401 });
		const mongo = buildMongoClient(env); const payload = JSON.parse(new TextDecoder().decode(body));
		await mongo.insertOne('inventory_events', { receivedAt: new Date().toISOString(), topic: request.headers.get('x-shopify-topic'), shopDomain: request.headers.get('x-shopify-shop-domain'), payload });
		return new Response('', { status: 200 });
	}
	return new Response('Not found', { status: 404 });
}}


