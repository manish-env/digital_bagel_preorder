import { json } from '../utils/http.js';

export async function health(_request, _env) {
    return json({ status: 'ok' });
}


