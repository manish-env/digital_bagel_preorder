import { json } from '../utils/http.js';
import { listPreorderVariants } from '../services/shopifyService.js';

export async function getPreorderProducts(request, env) {
    try {
        const url = new URL(request.url);
        const limit = Math.min(Number(url.searchParams.get('limit') || '1000'), 2000);
        const variants = await listPreorderVariants(env, limit);
        return json({ count: variants.length, variants });
    } catch (error) {
        console.error('Error in getPreorderProducts:', error);
        return json({ 
            error: 'Failed to load preorder products', 
            details: String(error?.message || error) 
        }, 500);
    }
}


