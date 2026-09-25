import "dotenv/config";
import express from "express";
import { OpenSeaStreamClient } from "@opensea/sdk/stream";

const PORT = Number(process.env.PORT || 10000);
const SALES_PER_MINUTE = Number(process.env.SALES_PER_MINUTE || 5);
const VOLUME_THRESHOLD_USD = Number(process.env.VOLUME_THRESHOLD_USD || 1000);
const MAX_VOLUME_WINDOW_MINUTES = Number(process.env.MAX_VOLUME_WINDOW_MINUTES || 30);
const SUPPLY_POLL_SECONDS = Number(process.env.SUPPLY_POLL_SECONDS || 60);
const SUPPLY_ACTIVE_MINUTES = Number(process.env.SUPPLY_ACTIVE_MINUTES || 500);
const MAX_HOT_COLLECTIONS = Number(process.env.MAX_HOT_COLLECTIONS || 100);
const COLLECTION_CACHE_SECONDS = Number(process.env.COLLECTION_CACHE_SECONDS || 900);
const STATS_CACHE_SECONDS = Number(process.env.STATS_CACHE_SECONDS || 120);

const apiKeys = [
  process.env.OPENSEA_API_KEY,
  process.env.OPENSEA_API_KEY_2,
  process.env.OPENSEA_API_KEY_3,
  process.env.OPENSEA_API_KEY_4,
  process.env.OPENSEA_API_KEY_5
].filter(Boolean);

if (!apiKeys[0]) throw new Error("OPENSEA_API_KEY is required");
if (!process.env.TELEGRAM_BOT_TOKEN || !process.env.TELEGRAM_CHAT_ID) {
  throw new Error("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required");
}

const collections = new Map();
const paymentTokenCache = new Map();

function getCollection(slug) {
  if (!slug) return null;
  if (!collections.has(slug)) {
    collections.set(slug, {
      slug,
      sales: [],                 // {ts, usd, tokenId, tx}
      lastSaleAt: 0,
      chain: null,
      supply: null,
      supplyCheckedAt: 0,
      collectionCache: null,
      collectionCacheAt: 0,
      statsCache: null,
      statsCacheAt: 0,
      lastVolumeAlertAt: 0,
      lastSupplyAlertAt: 0,
      lastSalesMinuteKey: null
    });
  }
  return collections.get(slug);
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function getNested(obj, paths) {
  for (const path of paths) {
    let x = obj;
    for (const k of path) x = x?.[k];
    if (x !== undefined && x !== null) return x;
  }
  return null;
}

function extractSlug(event) {
  const p = event?.payload ?? {};
  const slug = getNested(event, [
    ["payload", "collection", "slug"],
    ["payload", "collection_slug"],
    ["payload", "collection", "collection_slug"],
    ["payload", "item", "collection", "slug"],
    ["payload", "item", "collection_slug"],
    ["collection", "slug"]
  ]);
  return slug ? String(slug) : null;
}

function extractChain(event) {
  const chain =
    event?.chain ??
    event?.payload?.chain?.name ??
    event?.payload?.chain ??
    event?.payload?.item?.chain?.name ??
    event?.payload?.item?.chain ??
    null;
  return chain ? String(chain).toLowerCase() : null;
}

function decimalUsdFromRaw(raw, decimals, usdPrice) {
  if (raw == null || usdPrice == null) return null;
  try {
    const rawStr = String(raw);
    if (!/^\d+$/.test(rawStr)) return null;
    const d = Number(decimals);
    const u = Number(usdPrice);
    if (!Number.isInteger(d) || d < 0 || d > 36 || !Number.isFinite(u) || u <= 0) return null;

    // Convert the integer token quantity using the payment token's decimals.
    // Never coerce the raw on-chain integer to Number first: that was the source
    // of the gigantic false USD volumes seen in V4.
    const whole = rawStr.length > d ? rawStr.slice(0, -d) : "0";
    const frac = d === 0 ? "" : rawStr.slice(-d).padStart(d, "0");
    const tokenAmount = Number(`${whole}.${frac || "0"}`);
    if (!Number.isFinite(tokenAmount)) return null;
    return tokenAmount * u;
  } catch {
    return null;
  }
}

function extractSale(event) {
  const p = event?.payload ?? {};
  const sale = p?.sale ?? p?.item_sale ?? p;
  const tsRaw =
    event?.event_timestamp ??
    sale?.event_timestamp ??
    event?.sent_at ??
    sale?.timestamp ??
    Date.now();
  const ts = typeof tsRaw === "number"
    ? (tsRaw < 2e12 ? tsRaw * 1000 : tsRaw)
    : Date.parse(tsRaw) || Date.now();

  const payment =
    sale?.payment_token ??
    sale?.paymentToken ??
    p?.payment_token ??
    p?.paymentToken ??
    sale?.payment ??
    p?.payment ??
    {};

  // Stream sale payloads expose the sale amount in token base units and the
  // payment token decimals. The previous code treated that base-unit integer
  // as a human token amount, producing fake $quadrillion volume alerts.
  const rawPrice =
    sale?.sale_price ??
    sale?.salePrice ??
    p?.sale_price ??
    p?.salePrice ??
    sale?.quantity ??
    payment?.quantity ??
    null;

  const decimals =
    payment?.decimals ??
    sale?.payment_token?.decimals ??
    p?.payment_token?.decimals ??
    null;

  const usdPrice =
    num(payment?.usd_price) ??
    num(payment?.usdPrice) ??
    num(sale?.payment_token?.usd_price) ??
    num(sale?.payment_token?.usdPrice) ??
    num(p?.payment_token?.usd_price) ??
    num(p?.payment_token?.usdPrice) ??
    null;

  const usd = decimalUsdFromRaw(rawPrice, decimals, usdPrice);

  const tokenId =
    getNested(event, [["payload","item","identifier"],["payload","item","token_id"],["payload","token_id"]]) ?? "";
  const tx =
    getNested(event, [["payload","transaction"],["payload","transaction","hash"],["payload","transaction_hash"],["payload","tx_hash"]]) ?? "";

  return {
    ts,
    usd,
    rawPrice: rawPrice == null ? null : String(rawPrice),
    tokenDecimals: decimals == null ? null : Number(decimals),
    tokenAddress: payment?.token_address ?? payment?.tokenAddress ?? null,
    tokenSymbol: payment?.symbol ?? null,
    tokenId: String(tokenId),
    tx: String(tx)
  };
}

async function resolveSaleUsd(sale, chain) {
  if (sale.usd != null && Number.isFinite(sale.usd)) return sale.usd;
  if (!chain || !sale.tokenAddress || sale.tokenDecimals == null || !sale.rawPrice) return null;

  const key = `${chain}:${String(sale.tokenAddress).toLowerCase()}`;
  let usdPrice = paymentTokenCache.get(key);
  const cached = usdPrice != null;

  if (!cached) {
    try {
      const data = await osFetch(`/api/v2/chain/${encodeURIComponent(chain)}/payment_token/${encodeURIComponent(sale.tokenAddress)}`);
      usdPrice = num(data?.usd_price) ?? num(data?.usdPrice) ?? num(data?.payment_token?.usd_price) ?? num(data?.paymentToken?.usdPrice);
      if (usdPrice != null && usdPrice > 0) paymentTokenCache.set(key, usdPrice);
    } catch (e) {
      console.warn(`[TOKEN] ${chain}/${sale.tokenAddress}: ${e.message}`);
      return null;
    }
  }

  return decimalUsdFromRaw(sale.rawPrice, sale.tokenDecimals, usdPrice);
}

function prune(c, now = Date.now()) {
  const cutoff = now - MAX_VOLUME_WINDOW_MINUTES * 60_000 - 60_000;
  while (c.sales.length && c.sales[0].ts < cutoff) c.sales.shift();
}

// Volume: find whether ANY rolling window of 1..MAX_VOLUME_WINDOW_MINUTES reaches threshold.
// This is computed from the streamed sale timestamps, not by making 500 API calls.
function volumeTrigger(c, now = Date.now()) {
  prune(c, now);
  if (!c.sales.length) return null;

  let left = 0;
  let sum = 0;
  for (let right = 0; right < c.sales.length; right++) {
    if (c.sales[right].usd != null) sum += c.sales[right].usd;
    while (left <= right && c.sales[right].ts - c.sales[left].ts > MAX_VOLUME_WINDOW_MINUTES * 60_000) {
      if (c.sales[left].usd != null) sum -= c.sales[left].usd;
      left++;
    }
    // The tightest window ending at this sale is the best one for a threshold.
    // If the full allowed window reaches threshold, then some <=500m rolling window does.
    if (sum >= VOLUME_THRESHOLD_USD) {
      const windowMinutes = Math.max(1, Math.ceil((c.sales[right].ts - c.sales[left].ts) / 60_000));
      return { volume: sum, windowMinutes };
    }
  }
  return null;
}

function salesLastMinute(c, now = Date.now()) {
  prune(c, now);
  const cutoff = now - 60_000;
  let i = c.sales.length - 1;
  let count = 0;
  while (i >= 0 && c.sales[i].ts >= cutoff) { count++; i--; }
  return count;
}

function fmtUsd(n) {
  if (!Number.isFinite(n)) return "$0";
  return `$${n.toLocaleString("en-US", { maximumFractionDigits: 2 })}`;
}

async function osFetch(path) {
  let lastErr;
  for (const key of apiKeys) {
    try {
      const r = await fetch(`https://api.opensea.io${path}`, {
        headers: { accept: "application/json", "x-api-key": key }
      });
      if (r.status === 429) {
        const retry = r.headers.get("retry-after") || "unknown";
        console.warn(`[OpenSea] 429, retry-after=${retry}s`);
        lastErr = new Error("429");
        continue;
      }
      if (!r.ok) throw new Error(`OpenSea ${r.status}: ${await r.text()}`);
      return await r.json();
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr || new Error("OpenSea request failed");
}

async function enrich(c) {
  const now = Date.now();

  if (!c.collectionCache || now - c.collectionCacheAt > COLLECTION_CACHE_SECONDS * 1000) {
    try {
      c.collectionCache = await osFetch(`/api/v2/collections/${encodeURIComponent(c.slug)}`);
      c.collectionCacheAt = now;
    } catch (e) { console.warn(`[ENRICH] collection ${c.slug}: ${e.message}`); }
  }

  if (!c.statsCache || now - c.statsCacheAt > STATS_CACHE_SECONDS * 1000) {
    try {
      c.statsCache = await osFetch(`/api/v2/collections/${encodeURIComponent(c.slug)}/stats`);
      c.statsCacheAt = now;
    } catch (e) { console.warn(`[ENRICH] stats ${c.slug}: ${e.message}`); }
  }

  return c;
}

async function telegram(text) {
  const url = `https://api.telegram.org/bot${process.env.TELEGRAM_BOT_TOKEN}/sendMessage`;
  const r = await fetch(url, {
    method: "POST",
    headers: {"content-type":"application/json"},
    body: JSON.stringify({
      chat_id: process.env.TELEGRAM_CHAT_ID,
      text,
      disable_web_page_preview: false
    })
  });
  if (!r.ok) console.error("[Telegram]", await r.text());
}

function details(c) {
  const col = c.collectionCache?.collection ?? c.collectionCache ?? {};
  const st = c.statsCache?.total ?? c.statsCache ?? {};
  const name = col.name || c.slug;
  const chain = c.chain || col.chain || col.primary_chain || "unknown";
  const supply = col.total_supply ?? c.supply ?? "unknown";
  const floor = st.floor_price ?? st.floor_price_usd ?? "unknown";
  const vol24 = st.volume ?? st.volume_24h ?? "unknown";
  const owners = col.owner_count ?? col.num_owners ?? "unknown";
  const url = `https://opensea.io/collection/${encodeURIComponent(c.slug)}`;
  return {name, chain, supply, floor, vol24, owners, url};
}

async function sendSalesAlert(c, count) {
  await enrich(c);
  const d = details(c);
  await telegram(
`馃毃 SALES ALERT

Collection: ${d.name}
Chain: ${d.chain}
Sales: ${count} in last 1 minute
Supply: ${d.supply}
Floor: ${d.floor}
24h Volume: ${d.vol24}
Owners: ${d.owners}

OpenSea: ${d.url}`
  );
}

async function sendVolumeAlert(c, hit) {
  await enrich(c);
  const d = details(c);
  await telegram(
`馃挵 VOLUME ALERT

Collection: ${d.name}
Chain: ${d.chain}
Volume: ${fmtUsd(hit.volume)}
Window: ${hit.windowMinutes} min (rolling, <= ${MAX_VOLUME_WINDOW_MINUTES} min)
Supply: ${d.supply}
Floor: ${d.floor}
24h Volume: ${d.vol24}
Owners: ${d.owners}

OpenSea: ${d.url}`
  );
}

async function sendSupplyAlert(c, oldSupply, newSupply) {
  await enrich(c);
  const d = details(c);
  await telegram(
`馃啎 SUPPLY INCREASE

Collection: ${d.name}
Chain: ${d.chain}
Supply: ${oldSupply} 鈫� ${newSupply}
Floor: ${d.floor}
24h Volume: ${d.vol24}
Owners: ${d.owners}

OpenSea: ${d.url}`
  );
}

async function handleSale(event) {
  const slug = extractSlug(event);
  if (!slug) return;

  const c = getCollection(slug);
  const eventChain = extractChain(event);
  if (eventChain) c.chain = eventChain;
  const sale = extractSale(event);
  sale.usd = await resolveSaleUsd(sale, c.chain);

  // Deduplicate a reconnect/duplicate delivery when tx + token are both present.
  const sig = `${sale.tx}|${sale.tokenId}|${sale.ts}`;
  if (c.sales.some(x => x.sig === sig)) return;
  sale.sig = sig;

  c.sales.push(sale);
  c.sales.sort((a,b) => a.ts - b.ts);
  c.lastSaleAt = sale.ts;
  prune(c, Date.now());

  // Evaluate immediately for fast volume crossing.
  const v = volumeTrigger(c);
  if (v && Date.now() - c.lastVolumeAlertAt > 60_000) {
    c.lastVolumeAlertAt = Date.now();
    await sendVolumeAlert(c, v).catch(e => console.error("[ALERT volume]", e.message));
  }

  console.log(`[SALE] ${slug} | ${sale.usd == null ? "USD=unknown" : "$" + sale.usd.toFixed(2)} | sales_1m=${salesLastMinute(c)}`);
}

async function salesMinuteLoop() {
  // Check every 5 seconds so a qualifying rolling 1-minute window is not missed at a wall-clock minute boundary.
  setInterval(async () => {
    const now = Date.now();
    for (const c of collections.values()) {
      const count = salesLastMinute(c, now);
      if (count >= SALES_PER_MINUTE) {
        // One alert per minute per collection. No 15-minute cooldown.
        const minuteKey = Math.floor(now / 60_000);
        if (c.lastSalesMinuteKey !== minuteKey) {
          c.lastSalesMinuteKey = minuteKey;
          await sendSalesAlert(c, count).catch(e => console.error("[ALERT sales]", e.message));
        }
      }
    }
  }, 5_000);
}

async function supplyLoop() {
  setInterval(async () => {
    const cutoff = Date.now() - SUPPLY_ACTIVE_MINUTES * 60_000;
    const hot = [...collections.values()]
      .filter(c => c.lastSaleAt >= cutoff)
      .sort((a,b) => b.lastSaleAt - a.lastSaleAt)
      .slice(0, MAX_HOT_COLLECTIONS);

    for (const c of hot) {
      try {
        const data = await osFetch(`/api/v2/collections/${encodeURIComponent(c.slug)}`);
        const col = data?.collection ?? data;
        const fresh = num(col?.total_supply);

        if (fresh != null) {
          if (c.supply != null && fresh > c.supply) {
            const old = c.supply;
            c.supply = fresh;
            c.collectionCache = data;
            c.collectionCacheAt = Date.now();
            if (Date.now() - c.lastSupplyAlertAt > 60_000) {
              c.lastSupplyAlertAt = Date.now();
              await sendSupplyAlert(c, old, fresh).catch(e => console.error("[ALERT supply]", e.message));
            }
          } else {
            c.supply = fresh;
          }
          c.supplyCheckedAt = Date.now();
        }
      } catch (e) {
        console.warn(`[SUPPLY] ${c.slug}: ${e.message}`);
      }
    }
  }, SUPPLY_POLL_SECONDS * 1000);
}

const app = express();
app.get("/health", (_, res) => res.json({
  ok: true,
  trackedCollections: collections.size,
  uptimeSeconds: Math.floor(process.uptime()),
  stream: "global-sales"
}));
app.get("/", (_, res) => res.send("OpenSea tracker is running."));
app.listen(PORT, "0.0.0.0", () => console.log(`[HTTP] listening on 0.0.0.0:${PORT}`));

const client = new OpenSeaStreamClient({ apiKey: apiKeys[0] });
client.onItemSold("*", handleSale);

console.log(`[START] Global OpenSea sales stream connected`);
console.log(`[RULE] ${SALES_PER_MINUTE} sales / rolling 1 minute`);
console.log(`[RULE] $${VOLUME_THRESHOLD_USD} in ANY rolling window 1..${MAX_VOLUME_WINDOW_MINUTES} minutes`);
console.log(`[RULE] Supply increase via REST for up to ${MAX_HOT_COLLECTIONS} recently active collections`);

salesMinuteLoop();
supplyLoop();
    
