# Donation Queue Worker

Cloudflare Worker and Durable Object for receiving authenticated donation events and streaming them to a connected overlay.

## Setup

1. Install Node.js and run `npm install`.
2. For local development, copy `.dev.vars.example` to `.dev.vars` and replace the placeholder value.
3. For deployment, run `npx wrangler secret put SECRET` and enter your own secret.
4. Run `npm run deploy`.

The public `GET /` and `GET /users/{userId}/overlay` routes do not require the secret. Donation submissions require `Authorization: Bearer <your SECRET>`.

The Durable Object binding and first migration are defined in `wrangler.toml`. Do not commit `.dev.vars` or any secret value.
