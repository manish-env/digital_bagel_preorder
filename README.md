# Shopify Preorder Worker (MVC)

This project is a Cloudflare Worker that manages Shopify preorder metafields and exposes a small dashboard and CSV upload.

## Structure

- `worker/app/index.js` – App entry; wires the router and static assets
- `worker/app/router.js` – Minimal method/path router
- `worker/app/controllers/` – Route handlers (health, products, upload, admin, webhooks)
- `worker/app/services/` – Shopify GraphQL service
- `worker/app/models/` – Mongo Data API client
- `worker/app/utils/` – HTTP helpers
- `public/` – Static UI (dashboard + CSV upload page)
- `worker/worker.js` – Worker entry that delegates to app
- `wrangler.toml` – Worker config and bindings

## Endpoints

- GET `/health`
- GET `/api/preorder-products?limit=1000`
- POST `/upload` (multipart form-data `file`)
- POST `/admin/register-inventory-webhook`
- POST `/webhooks/shopify/inventory`

## Env Vars (wrangler.toml [vars])

- `SHOPIFY_STORE_DOMAIN`
- `SHOPIFY_ADMIN_ACCESS_TOKEN`
- `SHOPIFY_API_VERSION` (default: 2024-10)
- `SHOPIFY_WEBHOOK_SECRET`
- `PUBLIC_BASE_URL` (HTTPS, for webhooks)
- `METAFIELD_NAMESPACE` (default: preorder)
- `CONCURRENCY` (default: 5)
- `MONGO` – MongoDB Atlas Data API config (JSON or `key=value;` list):
  - Keys: `url`, `key`, `dataSource`, `db` (optional)

## Develop

```bash
npm run dev
```

## Deploy

```bash
npm run deploy
```

## Notes

- No Node server or Node-only libs required; uses `fetch` and Workers runtime.
- CSV parsing is contained within the upload controller for simplicity; can be extracted to a util if needed.

