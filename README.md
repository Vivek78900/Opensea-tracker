# OpenSea Global NFT Momentum Tracker

Global event-driven tracker for every collection visible through OpenSea Stream.

## Alerts
- **Sales:** 5+ sales in a rolling 60-second window, per collection.
- **Volume:** $1,000+ in any rolling window up to 30 minutes, per collection.
- **Supply:** every mint-like transfer queues a `total_supply` verification; there is **no collection cap**. Verification uses OpenSea's batch collections endpoint.
- Volume alert cooldown: 1 hour per collection.
- Supply alert cooldown: 30 minutes per collection.
- Sales alerts are independent of those cooldowns.

## Why there is no polling of every collection
OpenSea Stream can subscribe globally with `*`, and streamed events do not consume REST rate limits. REST is used only when a mint-like transfer makes a supply verification necessary. The supply queue has no collection-count cap; batching and throttling protect the REST API.

## Render
- Build: `npm install`
- Start: `npm start`
- Node: 22+
- Health: `/health`
- Keep an external UptimeRobot monitor on `/health` if desired.

Never commit `.env` or API keys.
