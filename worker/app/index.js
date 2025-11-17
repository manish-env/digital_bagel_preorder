import { Router } from './router.js';
import { health } from './controllers/healthController.js';
import { getPreorderProducts } from './controllers/productsController.js';
import { uploadCsv, getUploadProgress } from './controllers/uploadController.js';
import { registerInventoryWebhook, initDb, uploadOrderedCsv, clearPreorderData } from './controllers/adminController.js';
import { shopifyInventoryWebhook } from './controllers/webhookController.js';
import { login, logout } from './controllers/authController.js';
import { updateVariantMetafields, deleteVariantPreorder } from './controllers/variantController.js';
import { json } from './utils/http.js';
import { requireAuth } from './utils/auth.js';

const router = new Router()
  .add('GET', '/health', health)
  .add('POST', '/api/login', login)
  .add('POST', '/api/logout', logout)
  .add('GET', '/api/preorder-products', getPreorderProducts)
  .add('POST', '/upload', uploadCsv)
  .add('POST', '/api/upload', uploadCsv)
  .add('POST', '/api/variant-metafields', updateVariantMetafields)
  .add('POST', '/api/variant-delete', deleteVariantPreorder)
  .add('POST', '/admin/upload-ordered', uploadOrderedCsv)
  .add('POST', '/admin/clear-preorder', clearPreorderData)
  .add('OPTIONS', '/upload', () => new Response('', { status: 204, headers: corsHeaders() }))
  .add('OPTIONS', '/api/upload', () => new Response('', { status: 204, headers: corsHeaders() }))
  .add('OPTIONS', '/api/upload/', () => new Response('', { status: 204, headers: corsHeaders() }))
  .add('OPTIONS', '/api/upload/:uploadId', () => new Response('', { status: 204, headers: corsHeaders() }))
  .add('OPTIONS', '/api/variant-metafields', () => new Response('', { status: 204, headers: corsHeaders() }))
  .add('OPTIONS', '/api/variant-delete', () => new Response('', { status: 204, headers: corsHeaders() }))
  .add('OPTIONS', '/admin/upload-ordered', () => new Response('', { status: 204, headers: corsHeaders() }))
  .add('OPTIONS', '/admin/clear-preorder', () => new Response('', { status: 204, headers: corsHeaders() }))
  .add('POST', '/admin/register-inventory-webhook', registerInventoryWebhook)
  .add('POST', '/admin/init-db', initDb)
  .add('POST', '/webhooks/shopify/inventory', shopifyInventoryWebhook);

function corsHeaders() {
  return {
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'POST, OPTIONS',
    'access-control-allow-headers': 'content-type',
  };
}

// Protected paths that require authentication
const PROTECTED_PATHS = [
  '/upload',
  '/api/upload',
  '/api/upload/',
  '/api/preorder-products',
  '/api/variant-metafields',
  '/api/variant-delete',
  '/admin',
  '/admin/clear-preorder',
  '/index.html',
  '/upload.html'
];

// Public paths that don't require authentication
const PUBLIC_PATHS = [
  '/health',
  '/api/login',
  '/api/logout',
  '/login.html',
  '/webhooks'
];

function isProtectedPath(pathname) {
  // Check if it's a public path first
  if (PUBLIC_PATHS.some(path => pathname === path || pathname.startsWith(path + '/'))) {
    return false;
  }
  // Check if it's a protected path
  return PROTECTED_PATHS.some(path => pathname === path || pathname.startsWith(path + '/'));
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const pathname = url.pathname;
    
    // Check authentication for protected paths
    if (isProtectedPath(pathname)) {
      const authResponse = requireAuth(request);
      if (authResponse) return authResponse;
    }
    
    // Handle upload progress requests
    if (request.method === 'GET' && pathname.startsWith('/api/upload/')) {
      try {
        const uploadId = pathname.split('/').pop();
        if (uploadId && uploadId !== 'upload') {
          const result = await getUploadProgress(request, env);
          return result;
        }
      } catch (e) {
        return json({ error: 'Failed to get upload progress', details: String(e.message) }, 500);
      }
    }

    try {
      const handled = await router.handle(request, env);
      if (handled) return handled;
    } catch (e) {
      return json(
        { error: 'Request failed', details: String(e?.message || e) },
        500
      );
    }

    if (request.method === 'GET') {
      if (!env.ASSETS)
        return new Response('Assets binding missing', { status: 500 });
      
      // Check auth for HTML pages
      if (pathname.endsWith('.html')) {
        const authResponse = requireAuth(request);
        if (authResponse) return authResponse;
      }
      
      return env.ASSETS.fetch(request);
    }

    return new Response('Not found', { status: 404 });
  },
};
