import "dotenv/config";
import express from "express";
import { OpenSeaStreamClient } from "@opensea/sdk/stream";

const PORT = Number(process.env.PORT || 10000);
const SALES_PER_MINUTE = Number(process.env.SALES_PER_MINUTE || 5);
const VOLUME_THRESHOLD_USD = Number(process.env.VOLUME_THRESHOLD_USD || 1000);
const MAX_VOLUME_WINDOW_MINUTES = Number(process.env.MAX_VOLUME_WINDOW_MINUTES || 30);
const VOLUME_ALERT_COOLDOWN_MS = Number(process.env.VOLUME_ALERT_COOLDOWN_SECONDS || 3600) * 1000;
const SUPPLY_ALERT_COOLDOWN_MS = Number(process.env.SUPPLY_ALERT_COOLDOWN_SECONDS || 1800) * 1000;
const SUPPLY_BATCH_SIZE = Number(process.env.SUPPLY_BATCH_SIZE || 50);
const SUPPLY_BATCH_INTERVAL_MS = Number(process.env.SUPPLY_BATCH_INTERVAL_SECONDS || 10) * 1000;
const SUPPLY_RECHECK_DELAY_MS = Number(process.env.SUPPLY_RECHECK_DELAY_SECONDS || 20) * 1000;
const PRICE_CACHE_MS = Number(process.env.PRICE_CACHE_MINUTES || 10) * 60_000;
const COLLECTION_CACHE_MS = Number(process.env.COLLECTION_CACHE_MINUTES || 30) * 60_000;
const STATS_CACHE_MS = Number(process.env.STATS_CACHE_SECONDS || 120) * 1000;
const SALE_WINDOW_MS = 60_000;

const apiKeys = [
  process.env.OPENSEA_API_KEY,
  process.env.OPENSEA_API_KEY_2,
  process.env.OPENSEA_API_KEY_3,
  process.env.OPENSEA_API_KEY_4,
  process.env.OPENSEA_API_KEY_5,
].filter(Boolean);

if (!apiKeys[0]) throw new Error("OPENSEA_API_KEY is required");
if (!process.env.TELEGRAM_BOT_TOKEN || !process.env.TELEGRAM_CHAT_ID) {
  throw new Error("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required");
}

const ZERO = "0x0000000000000000000000000000000000000000";
const collections = new Map();
const paymentTokenCache = new Map();
const supplyQueue = new Map(); // slug -> { dueAt, reason, attempts }
const seenEvents = new Map();

let apiChain = Promise.resolve();
let lastApiRequestAt = 0;
let apiBackoffUntil = 0;
let lastStreamEventAt = 0;
let lastStreamErrorAt = 0;
let lastStreamError = null;
let lastSupplyBatchAt = 0;
let lastSupplyBatchSize = 0;
let supplyBatches = 0;
let supplyRequests = 0;
let supplyQueuePeak = 0;
let telegramErrors = 0;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function getCollection(slug) {
  if (!slug) return null;
  let c = collections.get(slug);
  if (!c) {
    c = {
      slug,
      chain: null,
      sales: [],
      lastSaleAt: 0,
      lastTransferAt: 0,
      supply: null,
      supplyCheckedAt: 0,
      lastVolumeAlertAt: 0,
      lastSupplyAlertAt: 0,
      lastSalesAlertMinute: -1,
      collectionCache: null,
      collectionCacheAt: 0,
      statsCache: null,
      statsCacheAt: 0,
      name: null,
      openSeaUrl: null,
    };
    collections.set(slug, c);
  }
  return c;
}

function first(obj, paths) {
  for (const path of paths) {
    let x = obj;
    for (const key of path) x = x?.[key];
    if (x !== undefined && x !== null) return x;
  }
  return null;
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function eventMs(event) {
  const raw = event?.event_timestamp ?? event?.payload?.event_timestamp ?? event?.sent_at ?? Date.now();
  if (typeof raw === "number") return raw < 2e12 ? raw * 1000 : raw;
  const parsed = Date.parse(raw);
  return Number.isFinite(parsed) ? parsed : Date.now();
}

function extractSlug(event) {
  const value = first(event, [
    ["payload", "collection", "slug"],
    ["payload", "collection_slug"],
    ["payload", "collection", "collection_slug"],
    ["payload", "item", "collection", "slug"],
    ["payload", "item", "collection_slug"],
    ["collection", "slug"],
  ]);
  return value ? String(value) : null;
}

function extractChain(event) {
  const value = first(event, [
    ["chain"],
    ["payload", "chain", "name"],
    ["payload", "chain"],
    ["payload", "item", "chain", "name"],
    ["payload", "item", "chain"],
  ]);
  return value ? String(value).toLowerCase() : null;
}

function extractTransferFrom(event) {
  const value = first(event, [
    ["payload", "from_address"],
    ["payload", "fromAddress"],
    ["payload", "from_account", "address"],
    ["payload", "from"],
    ["from_address"],
    ["fromAddress"],
  ]);
  return value ? String(value).toLowerCase() : null;
}

function extractTokenId(event) {
  const value = first(event, [
    ["payload", "item", "identifier"],
    ["payload", "item", "token_id"],
    ["payload", "item", "nft_id"],
    ["payload", "nft", "identifier"],
  ]);
  return value ? String(value) : "";
}

function extractTx(event) {
  const value = first(event, [
    ["payload", "transaction"],
    ["payload", "transaction", "hash"],
    ["payload", "transaction_hash"],
    ["payload", "tx_hash"],
  ]);
  return value ? String(value) : "";
}

function eventSignature(event, kind) {
  return `${kind}|${extractTx(event)}|${extractTokenId(event)}|${eventMs(event)}|${event?.version ?? ""}`;
}

function rememberEvent(sig) {
  const now = Date.now();
  if (seenEvents.has(sig)) return false;
  seenEvents.set(sig, now);
  if (seenEvents.size > 20_000) {
    const cutoff = now - 10 * 60_000;
    for (const [k, t] of seenEvents) if (t < cutoff) seenEvents.delete(k);
    while (seenEvents.size > 15_000) seenEvents.delete(seenEvents.keys().next().value);
  }
  return true;
}

function rawToNumber(raw, decimals) {
  if (raw == null || decimals == null) return null;
  const s = String(raw);
  if (!/^\d+$/.test(s)) return null;
  const d = Number(decimals);
  if (!Number.isInteger(d) || d < 0 || d > 36) return null;
  try {
    const whole = d === 0 ? s : (s.length > d ? s.slice(0, -d) : "0");
    const frac = d === 0 ? "" : s.slice(-d).padStart(d, "0");
    const n = Number(`${whole}.${frac || "0"}`);
    return Number.isFinite(n) ? n : null;
  } catch {
    return null;
  }
}

function extractSale(event) {
  const p = event?.payload ?? {};
  const sale = p?.sale ?? p;
  const payment = sale?.payment_token ?? sale?.paymentToken ?? p?.payment_token ?? p?.paymentToken ?? sale?.payment ?? {};
  const rawPrice = sale?.sale_price ?? sale?.salePrice ?? p?.sale_price ?? p?.salePrice ?? payment?.quantity ?? sale?.quantity ?? null;
  const decimals = payment?.decimals ?? sale?.payment_token?.decimals ?? p?.payment_token?.decimals ?? null;
  const usdPrice = num(payment?.usd_price) ?? num(payment?.usdPrice) ?? num(sale?.payment_token?.usd_price) ?? num(p?.payment_token?.usd_price);
  const tokenAddress = payment?.token_address ?? payment?.tokenAddress ?? null;
  return {
    ts: eventMs(event),
    rawPrice: rawPrice == null ? null : String(rawPrice),
    decimals: decimals == null ? null : Number(decimals),
    usdPrice,
    tokenAddress: tokenAddress ? String(tokenAddress) : null,
    usd: rawPrice != null && decimals != null && usdPrice != null ? rawToNumber(rawPrice, decimals) * usdPrice : null,
    tx: extractTx(event),
    tokenId: extractTokenId(event),
  };
}

async function queuedApi(fn, minGapMs = 750) {
  const run = apiChain.then(async () => {
    const now = Date.now();
    if (apiBackoffUntil > now) await sleep(apiBackoffUntil - now);
    const gap = Math.max(0, minGapMs - (Date.now() - lastApiRequestAt));
    if (gap) await sleep(gap);
    const result = await fn();
    lastApiRequestAt = Date.now();
    return result;
  });
  apiChain = run.catch(() => {});
  return run;
}

async function osFetch(path, options = {}, minGapMs = 750) {
  return queuedApi(async () => {
    let lastError;
    for (let attempt = 0; attempt < 4; attempt++) {
      const key = apiKeys[0];
      const response = await fetch(`https://api.opensea.io${path}`, {
        ...options,
        headers: {
          accept: "application/json",
          ...(options.body ? { "content-type": "application/json" } : {}),
          "x-api-key": key,
          ...(options.headers || {}),
        },
      });
      if (response.status === 429) {
        const retryAfter = num(response.headers.get("retry-after"));
        const reset = num(response.headers.get("x-ratelimit-reset"));
        const wait = retryAfter != null ? retryAfter * 1000 : reset != null ? Math.max(1000, reset * 1000 - Date.now()) : Math.min(30_000, 2000 * (attempt + 1));
        apiBackoffUntil = Date.now() + wait;
        console.warn(`[OpenSea] 429 ${path}; backing off ${Math.ceil(wait / 1000)}s`);
        lastError = new Error("OpenSea 429");
        await sleep(wait);
        continue;
      }
      if (!response.ok) throw new Error(`OpenSea ${response.status}: ${await response.text()}`);
      return response.json();
    }
    throw lastError || new Error("OpenSea request failed");
  }, minGapMs);
}

async function resolveUsd(sale, chain) {
  if (sale.usd != null && Number.isFinite(sale.usd)) return sale.usd;
  if (!chain || !sale.tokenAddress || sale.decimals == null || !sale.rawPrice) return null;
  const key = `${chain}:${sale.tokenAddress.toLowerCase()}`;
  let cached = paymentTokenCache.get(key);
  if (!cached || cached.expiresAt < Date.now()) {
    try {
      const data = await osFetch(`/api/v2/chain/${encodeURIComponent(chain)}/payment_token/${encodeURIComponent(sale.tokenAddress)}`);
      const price = num(data?.usd_price) ?? num(data?.usdPrice) ?? num(data?.payment_token?.usd_price);
      if (price != null && price > 0) {
        cached = { price, expiresAt: Date.now() + PRICE_CACHE_MS };
        paymentTokenCache.set(key, cached);
      }
    } catch (e) {
      console.warn(`[PRICE] ${key}: ${e.message}`);
      return null;
    }
  }
  const amount = rawToNumber(sale.rawPrice, sale.decimals);
  return amount != null && cached?.price ? amount * cached.price : null;
}

function prune(c, now = Date.now()) {
  const cutoff = now - MAX_VOLUME_WINDOW_MINUTES * 60_000 - 5_000;
  while (c.sales.length && c.sales[0].ts < cutoff) c.sales.shift();
}

function sales1m(c, now = Date.now()) {
  prune(c, now);
  const cutoff = now - SALE_WINDOW_MS;
  let count = 0;
  for (let i = c.sales.length - 1; i >= 0 && c.sales[i].ts >= cutoff; i--) count++;
  return count;
}

function volumeHit(c, now = Date.now()) {
  prune(c, now);
  let left = 0;
  let sum = 0;
  const maxMs = MAX_VOLUME_WINDOW_MINUTES * 60_000;
  for (let right = 0; right < c.sales.length; right++) {
    sum += c.sales[right].usd ?? 0;
    while (left < right && c.sales[right].ts - c.sales[left].ts > maxMs) {
      sum -= c.sales[left].usd ?? 0;
      left++;
    }
    if (sum >= VOLUME_THRESHOLD_USD) {
      const windowMinutes = Math.max(1, Math.ceil((c.sales[right].ts - c.sales[left].ts) / 60_000));
      return { volume: sum, windowMinutes };
    }
  }
  return null;
}

function fmtUsd(v) {
  return Number.isFinite(v) ? `$${v.toLocaleString("en-US", { maximumFractionDigits: 2 })}` : "$unknown";
}

async function enrich(c, needStats = true) {
  const now = Date.now();
  if (!c.collectionCache || now - c.collectionCacheAt > COLLECTION_CACHE_MS) {
    try {
      const data = await osFetch(`/api/v2/collections/${encodeURIComponent(c.slug)}`);
      c.collectionCache = data?.collection ?? data;
      c.collectionCacheAt = now;
      c.name = c.collectionCache?.name || c.name;
      c.openSeaUrl = c.collectionCache?.opensea_url || c.openSeaUrl;
      if (c.collectionCache?.contracts?.[0]?.chain) c.chain = c.collectionCache.contracts[0].chain;
    } catch (e) { console.warn(`[ENRICH] ${c.slug}: ${e.message}`); }
  }
  if (needStats && (!c.statsCache || now - c.statsCacheAt > STATS_CACHE_MS)) {
    try {
      const data = await osFetch(`/api/v2/collections/${encodeURIComponent(c.slug)}/stats`);
      c.statsCache = data?.total ?? data;
      c.statsCacheAt = now;
    } catch (e) { console.warn(`[STATS] ${c.slug}: ${e.message}`); }
  }
}

function detail(c) {
  const col = c.collectionCache ?? {};
  const st = c.statsCache ?? {};
  return {
    name: c.name || col.name || c.slug,
    chain: c.chain || col.contracts?.[0]?.chain || "unknown",
    supply: col.total_supply ?? c.supply ?? "unknown",
    floor: st.floor_price ?? "unknown",
    vol24: st.volume ?? st.volume_24h ?? "unknown",
    owners: col.owner_count ?? col.num_owners ?? "unknown",
    url: c.openSeaUrl || `https://opensea.io/collection/${encodeURIComponent(c.slug)}`,
  };
}

async function telegram(text) {
  try {
    const r = await fetch(`https://api.telegram.org/bot${process.env.TELEGRAM_BOT_TOKEN}/sendMessage`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ chat_id: process.env.TELEGRAM_CHAT_ID, text, disable_web_page_preview: false }),
    });
    if (!r.ok) { telegramErrors++; console.error("[Telegram]", await r.text()); }
  } catch (e) { telegramErrors++; console.error("[Telegram]", e.message); }
}

async function sendSalesAlert(c, count) {
  await enrich(c);
  const d = detail(c);
  await telegram(`馃毃 SALES ALERT\n\nCollection: ${d.name}\nChain: ${d.chain}\nSales: ${count} in last 1 minute\nSupply: ${d.supply}\nFloor: ${d.floor}\n24h Volume: ${d.vol24}\nOwners: ${d.owners}\n\nOpenSea: ${d.url}`);
}

async function sendVolumeAlert(c, hit) {
  await enrich(c);
  const d = detail(c);
  await telegram(`馃挵 VOLUME ALERT\n\nCollection: ${d.name}\nChain: ${d.chain}\nVolume: ${fmtUsd(hit.volume)}\nWindow: ${hit.windowMinutes} min (rolling, <= ${MAX_VOLUME_WINDOW_MINUTES} min)\nSupply: ${d.supply}\nFloor: ${d.floor}\n24h Volume: ${d.vol24}\nOwners: ${d.owners}\n\nOpenSea: ${d.url}`);
}

async function sendSupplyAlert(c, oldSupply, newSupply) {
  await enrich(c);
  const d = detail(c);
  await telegram(`馃啎 SUPPLY INCREASE\n\nCollection: ${d.name}\nChain: ${d.chain}\nSupply: ${oldSupply} 鈫� ${newSupply}\nFloor: ${d.floor}\n24h Volume: ${d.vol24}\nOwners: ${d.owners}\n\nOpenSea: ${d.url}`);
}

function queueSupply(slug, reason = "mint", delayMs = 0, attempts = 0) {
  const dueAt = Date.now() + delayMs;
  const existing = supplyQueue.get(slug);
  const nextAttempts = Math.max(existing?.attempts ?? 0, attempts);
  if (!existing || dueAt < existing.dueAt) supplyQueue.set(slug, { dueAt, reason, attempts: nextAttempts });
  supplyQueuePeak = Math.max(supplyQueuePeak, supplyQueue.size);
}

function isMintTransfer(event) {
  const from = extractTransferFrom(event);
  return from === ZERO;
}

async function handleSale(event) {
  const slug = extractSlug(event);
  if (!slug || !rememberEvent(eventSignature(event, "sale"))) return;
  const c = getCollection(slug);
  c.chain = extractChain(event) || c.chain;
  const sale = extractSale(event);
  sale.usd = await resolveUsd(sale, c.chain);
  c.sales.push(sale);
  c.sales.sort((a, b) => a.ts - b.ts);
  c.lastSaleAt = sale.ts;
  prune(c);

  const hit = volumeHit(c);
  if (hit && Date.now() - c.lastVolumeAlertAt >= VOLUME_ALERT_COOLDOWN_MS) {
    c.lastVolumeAlertAt = Date.now();
    await sendVolumeAlert(c, hit).catch(e => console.error("[ALERT volume]", e.message));
  }

  const count = sales1m(c);
  const minuteKey = Math.floor(Date.now() / 60_000);
  if (count >= SALES_PER_MINUTE && c.lastSalesAlertMinute !== minuteKey) {
    c.lastSalesAlertMinute = minuteKey;
    await sendSalesAlert(c, count).catch(e => console.error("[ALERT sales]", e.message));
  }
  console.log(`[SALE] ${slug} | ${sale.usd == null ? "USD=?" : fmtUsd(sale.usd)} | sales_1m=${count}`);
}

async function handleTransfer(event) {
  const slug = extractSlug(event);
  if (!slug || !rememberEvent(eventSignature(event, "transfer"))) return;
  const c = getCollection(slug);
  c.chain = extractChain(event) || c.chain;
  c.lastTransferAt = eventMs(event);
  if (isMintTransfer(event)) {
    // No arbitrary collection cap. Every collection that emits a mint transfer
    // is queued. The batch worker throttles REST requests, not discoveries.
    queueSupply(slug, "mint", SUPPLY_RECHECK_DELAY_MS);
    console.log(`[MINT] ${slug} | chain=${c.chain || "unknown"} | supply queued`);
  }
}

async function checkSupplyBatch(items) {
  if (!items.length) return;
  const slugs = items.map(x => x.slug);
  const body = JSON.stringify({ slugs });
  supplyRequests++;
  try {
    const data = await osFetch("/api/v2/collections/batch", { method: "POST", body }, 0);
    const returned = data?.collections ?? [];
    const bySlug = new Map(returned.map(x => [x.collection, x]));
    for (const item of items) {
      const c = getCollection(item.slug);
      const col = bySlug.get(item.slug);
      const fresh = num(col?.total_supply);
      if (fresh == null) {
        // Retry later; do not lose a mint just because OpenSea's index is late.
        if (item.attempts < 4) queueSupply(item.slug, item.reason, 15_000 * (item.attempts + 1), item.attempts);
        continue;
      }
      c.chain = col?.contracts?.[0]?.chain || c.chain;
      c.collectionCache = col;
      c.collectionCacheAt = Date.now();
      c.name = col?.name || c.name;
      c.openSeaUrl = col?.opensea_url || c.openSeaUrl;
      const old = c.supply;
      c.supply = fresh;
      c.supplyCheckedAt = Date.now();
      if (old != null && fresh > old && Date.now() - c.lastSupplyAlertAt >= SUPPLY_ALERT_COOLDOWN_MS) {
        c.lastSupplyAlertAt = Date.now();
        await sendSupplyAlert(c, old, fresh).catch(e => console.error("[ALERT supply]", e.message));
        console.log(`[SUPPLY ALERT] ${c.slug} | ${old} -> ${fresh}`);
      } else if (old != null && fresh > old) {
        console.log(`[SUPPLY] ${c.slug} increased ${old} -> ${fresh}; 30m cooldown active`);
      }
      // If a mint arrived but OpenSea had not indexed it yet, one delayed recheck
      // is enough to catch the eventual total_supply change without continuous polling.
      if (item.reason === "mint" && old === fresh && item.attempts < 2) {
        queueSupply(item.slug, "mint-recheck", 30_000, item.attempts);
      }
    }
  } catch (e) {
    console.warn(`[SUPPLY BATCH] ${e.message}`);
    for (const item of items) {
      if (item.attempts < 3) queueSupply(item.slug, item.reason, 30_000 * (item.attempts + 1), item.attempts);
    }
  }
}

async function supplyWorker() {
  while (true) {
    const now = Date.now();
    const due = [...supplyQueue.entries()]
      .filter(([, item]) => item.dueAt <= now)
      .sort((a, b) => a[1].dueAt - b[1].dueAt)
      .slice(0, SUPPLY_BATCH_SIZE);

    if (due.length) {
      for (const [slug] of due) supplyQueue.delete(slug);
      const items = due.map(([slug, item]) => ({ slug, ...item, attempts: item.attempts + 1 }));
      lastSupplyBatchAt = Date.now();
      lastSupplyBatchSize = items.length;
      supplyBatches++;
      await checkSupplyBatch(items);
    }
    await sleep(SUPPLY_BATCH_INTERVAL_MS);
  }
}

function salesSweep() {
  setInterval(async () => {
    const now = Date.now();
    for (const c of collections.values()) {
      const count = sales1m(c, now);
      const minuteKey = Math.floor(now / 60_000);
      if (count >= SALES_PER_MINUTE && c.lastSalesAlertMinute !== minuteKey) {
        c.lastSalesAlertMinute = minuteKey;
        await sendSalesAlert(c, count).catch(e => console.error("[ALERT sales sweep]", e.message));
      }
    }
  }, 5_000);
}

const app = express();
app.get("/", (_, res) => res.send("OpenSea global NFT tracker is running."));
app.get("/health", (_, res) => {
  const now = Date.now();
  res.json({
    ok: true,
    uptimeSeconds: Math.floor(process.uptime()),
    trackedCollections: collections.size,
    supplyQueue: supplyQueue.size,
    supplyQueuePeak,
    stream: {
      lastEventSecondsAgo: lastStreamEventAt ? Math.floor((now - lastStreamEventAt) / 1000) : null,
      lastError: lastStreamError,
      lastErrorSecondsAgo: lastStreamErrorAt ? Math.floor((now - lastStreamErrorAt) / 1000) : null,
    },
    supply: {
      batches: supplyBatches,
      requests: supplyRequests,
      lastBatchSecondsAgo: lastSupplyBatchAt ? Math.floor((now - lastSupplyBatchAt) / 1000) : null,
      lastBatchSize: lastSupplyBatchSize,
      batchSize: SUPPLY_BATCH_SIZE,
      intervalSeconds: SUPPLY_BATCH_INTERVAL_MS / 1000,
    },
    api: { backoffSeconds: Math.max(0, Math.ceil((apiBackoffUntil - now) / 1000)) },
    telegramErrors,
  });
});
app.listen(PORT, "0.0.0.0", () => console.log(`[HTTP] listening on 0.0.0.0:${PORT}`));

const stream = new OpenSeaStreamClient({
  apiKey: apiKeys[0],
  onError: (err) => {
    lastStreamErrorAt = Date.now();
    lastStreamError = err?.message || String(err);
    console.error("[STREAM ERROR]", err);
  },
});

stream.onItemSold("*", async (event) => {
  lastStreamEventAt = Date.now();
  try { await handleSale(event); } catch (e) { console.error("[SALE HANDLER]", e); }
});

stream.onItemTransferred("*", async (event) => {
  lastStreamEventAt = Date.now();
  try { await handleTransfer(event); } catch (e) { console.error("[TRANSFER HANDLER]", e); }
});

process.on("unhandledRejection", (e) => console.error("[UNHANDLED REJECTION]", e));
process.on("uncaughtException", (e) => console.error("[UNCAUGHT EXCEPTION]", e));

console.log("[START] GLOBAL OpenSea tracker");
console.log(`[RULE] ${SALES_PER_MINUTE}+ sales / rolling 1 minute 鈥� ALL collections`);
console.log(`[RULE] $${VOLUME_THRESHOLD_USD}+ volume / rolling 1..${MAX_VOLUME_WINDOW_MINUTES} minutes 鈥� ALL collections`);
console.log(`[RULE] Volume cooldown: ${VOLUME_ALERT_COOLDOWN_MS / 1000}s per collection`);
console.log(`[RULE] Supply: every mint transfer queues verification 鈥� NO collection cap`);
console.log(`[RULE] Supply batch: ${SUPPLY_BATCH_SIZE} collections / ${SUPPLY_BATCH_INTERVAL_MS / 1000}s`);
console.log(`[RULE] Supply cooldown: ${SUPPLY_ALERT_COOLDOWN_MS / 1000}s per collection`);

salesSweep();
supplyWorker();
