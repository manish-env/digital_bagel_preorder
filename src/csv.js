const { parse } = require('csv-parse/sync');

function toBoolean(input) {
	if (input === undefined || input === null) return undefined;
	const v = String(input).trim().toLowerCase();
	if (v === '') return undefined;
	if (['true', '1', 'yes', 'y'].includes(v)) return true;
	if (['false', '0', 'no', 'n'].includes(v)) return false;
	throw new Error(`Invalid boolean value: ${input}`);
}

function toInteger(input) {
	if (input === undefined || input === null) return undefined;
	const v = String(input).trim();
	if (v === '') return undefined;
	const n = Number(v);
	if (!Number.isInteger(n) || n < 0) throw new Error(`Invalid integer value: ${input}`);
	return n;
}

function normalizeHeader(name) {
	const base = String(name).replace(/^\uFEFF/, '').trim().toLowerCase();
	const key = base.replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
	if (['variant_sku', 'sku', 'variantid', 'variant_id_sku'].includes(key)) return 'sku';
	if (['handle', 'product_handle', 'handle_url'].includes(key)) return 'handle';
	if (['is_preorder', 'ispreorder', 'preorder', 'is_pre_order'].includes(key)) return 'is_preorder';
	if (['preorder_limit', 'pre_order_limit', 'limit'].includes(key)) return 'preorder_limit';
	if (['preorder_message', 'pre_order_message', 'message'].includes(key)) return 'preorder_message';
	return key;
}

function parseCsvFromBuffer(buffer) {
	const text = buffer.toString('utf8').replace(/^\uFEFF/, '');
	const records = parse(text, {
		columns: (header) => header.map(normalizeHeader),
		skip_empty_lines: true,
		trim: true,
	});

	let skippedRows = 0;
	const rows = [];
	for (const r of records) {
		const handle = r.handle === undefined || r.handle === null ? '' : String(r.handle).trim();
		const sku = r.sku === undefined || r.sku === null ? '' : String(r.sku).trim();
		if (!handle || !sku) { skippedRows += 1; continue; }

		const out = { handle, sku };
		if (r.is_preorder !== undefined) {
			try { out.is_preorder = toBoolean(r.is_preorder); } catch (_) { /* ignore invalid boolean */ }
		}
		if (r.preorder_limit !== undefined) {
			try { out.preorder_limit = toInteger(r.preorder_limit); } catch (_) { /* ignore invalid integer */ }
		}
		if (r.preorder_message !== undefined && r.preorder_message !== null && String(r.preorder_message).trim() !== '') {
			out.preorder_message = String(r.preorder_message);
		}
		rows.push(out);
	}

	return { rows, stats: { totalRows: records.length, skippedRows } };
}

module.exports = { parseCsvFromBuffer };


