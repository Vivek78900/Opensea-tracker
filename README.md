# OpenSea NFT Momentum Tracker — Render V4

## Alert logic

### 1) SALES
- **10 sales in the rolling last 1 minute** triggers a Telegram alert.
- This is **not** a 1–500 minute sales search.
- The bot checks every minute.
- If the collection keeps having 10+ sales in each later minute, it can alert again once per minute.

### 2) VOLUME
- Default threshold: **$1,000**.
- Window: **ANY rolling window from 1 to 30 minutes**.
- Calculated locally from OpenSea Stream sale events; it does not make 500 REST requests.

### 3) SUPPLY
- Recently active collections are checked against OpenSea `total_supply`.
- A change such as **400 → 401** triggers a supply alert.
- This is not a global REST poll of every collection because OpenSea REST is rate-limited.
- New collections are discovered automatically from global sale events.

## Render + 5-minute keep-alive

The app exposes:

`GET /health`

Render Free web services can spin down after 15 minutes with no inbound traffic. OpenSea WebSocket traffic can also keep a Render free service active when messages arrive, but a quiet market can still leave it idle. Render documents this behavior here: https://render.com/docs/free

### Use UptimeRobot Free

UptimeRobot's current Free plan supports **5-minute monitoring**. Create an HTTP(s) monitor for:

`https://YOUR-SERVICE.onrender.com/health`

Set the monitor interval to **5 minutes**.

Important: this ping must come from an **external** service. A timer inside this Node process cannot prevent Render from sleeping because it is the process itself that would be sleeping.

UptimeRobot: https://uptimerobot.com/

## Deploy

1. Push this project to GitHub.
2. Render → New → Web Service → connect the repository.
3. Build command: `npm install`
4. Start command: `npm start`
5. Plan: **Free**
6. Add the environment variables from `.env.example`.
7. Health check path: `/health`.
8. After the first deploy, copy the Render public URL into UptimeRobot as `/health`.

## Required environment variables

- `OPENSEA_API_KEY`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Optional additional OpenSea keys are supported for REST resilience. Keys from the same OpenSea account share the same rate-limit bucket, so they are **not** a quota bypass.

## Important Render limitation

Render Free has an ephemeral filesystem. In-memory sale/supply state is lost after a restart/spin-down/redeploy. The bot reconnects to OpenSea after restart, but it cannot reconstruct sales that happened while it was offline.
