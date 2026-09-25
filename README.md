# OpenSea NFT Momentum Tracker V8

Render-friendly version of the OpenSea global sales tracker.

## Alerts
- Sales: 5+ sales in rolling 1 minute; max one sales alert per collection per wall-clock minute.
- Volume: $1,000+ in any rolling 1–30 minute window; 1-hour cooldown per collection.
- Supply: polls OpenSea `total_supply` for recently active collections; supply increase alert has a 30-minute cooldown per collection.

## V8 reliability fixes
- Supply polling defaults to 30s / 40 hot collections instead of 15s / 150.
- Supply requests are serialized with a small gap to avoid request bursts.
- 429 responses respect `Retry-After` and back off.
- Supply runs cannot overlap if a previous run is still busy.
- First supply sweep runs 5s after startup instead of waiting for the first interval.
- Stream errors are logged through the SDK's error callback.
- Sale handlers are wrapped so one bad event cannot break the event callback.
- `/health` exposes uptime, last stream error, last stream event, and supply-loop status.
- Global `unhandledRejection` / `uncaughtException` logging added.

## Render
Use Node 22+.
Build: `npm install`
Start: `npm start`
Health path: `/health`

Keep an external UptimeRobot monitor on `/health` if you want an additional inbound request every 5 minutes. Render Free services can still be restarted by the platform and are not intended as production always-on workers.

## Environment
See `.env.example`.
