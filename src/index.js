// wardrift-sync — Wardrive Go auto-backup -> Wardrift, fully serverless.
//
// Flow: the phone POSTs its backup .zip here on close (Sync to a URL). We stash
// it in R2 and return fast. A cron drains it: unzip -> read wardrivego.db (pure
// JS, no WASM) -> forward new observations to Wardrift /v1/ingest/signals, with
// the same batching / adaptive-split / dedup the local tool proved out.

import { unzipSync } from "fflate";
import { SqliteFile } from "./sqlite.js";

const WARDRIFT_BASE = "https://wardrift.net";
const BATCH = 250;
const MIN_SPLIT = 8;
const MAX_PER_RUN = 800;       // cap work per run so we never blow the CPU/time budget
const NET_RETRIES = 5;
const MAC_RE = /^([0-9a-f]{2}:){5}[0-9a-f]{2}$/;

// ------------------------------------------------------------------ helpers
const enc = new TextEncoder();
async function sha256hex(str) {
  const d = await crypto.subtle.digest("SHA-256", enc.encode(str));
  return [...new Uint8Array(d)].map((b) => b.toString(16).padStart(2, "0")).join("");
}
function uuidish(hex) {
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
}
const iso = (ms) => {
  const n = Number(ms);
  return Number.isFinite(n) && n > 0 ? new Date(n).toISOString().replace(/\.\d{3}Z$/, "Z") : null;
};
function kindOf(t) {
  t = (t || "").toUpperCase();
  if (t === "WIFI") return "wifi";
  if (t === "BLE" || t === "BT") return "bluetooth";
  if (["GSM", "LTE", "CDMA", "WCDMA", "NR", "UMTS", "CELL"].includes(t)) return "cellular";
  return "other";
}

// ------------------------------------------------------------------ D1 state
async function getState(env, k, def = null) {
  const r = await env.DB.prepare("SELECT v FROM state WHERE k=?").bind(k).first();
  return r ? r.v : def;
}
async function setState(env, k, v) {
  await env.DB.prepare("INSERT INTO state (k,v) VALUES (?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v")
    .bind(k, String(v)).run();
}
async function nextSeq(env) {
  const cur = Number(await getState(env, "sequence_no", "0"));
  const seq = Math.max(cur + 1, Math.floor(Date.now() / 1000));
  await setState(env, "sequence_no", seq);
  return seq;
}

// ------------------------------------------------------------------ Wardrift
async function sendChunk(env, chunk, keyFp) {
  const digest = await sha256hex(JSON.stringify(chunk));
  const idem = uuidish(await sha256hex(`w2w-wardrift:${keyFp}:${digest}`));
  let seq = await nextSeq(env);
  let body = {}, r = null;
  for (let attempt = 0; attempt < 2; attempt++) {
    const envelope = {
      idempotency_key: idem, sequence_no: seq,
      device_ts: new Date().toISOString().replace(/\.\d{3}Z$/, "Z"),
      signature: "", items: chunk,
    };
    r = await fetchRetry(`${WARDRIFT_BASE}/v1/ingest/signals`, {
      method: "POST", headers: { "X-API-Key": env.WARDRIFT_API_KEY, "Content-Type": "application/json" },
      body: JSON.stringify(envelope),
    });
    try { body = await r.json(); } catch { body = { raw: await r.text() }; }
    const msg = `${body.error || ""} ${body.message || ""}`.toLowerCase();
    if (attempt === 0 && !r.ok && (msg.includes("replay") || msg.includes("sequence"))) {
      seq = await nextSeq(env);
      continue;
    }
    break;
  }
  const ok = r.ok && (body.accepted || body.duplicate);
  return { ok, body };
}

async function fetchRetry(url, opts) {
  let last;
  for (let i = 0; i < NET_RETRIES; i++) {
    try { return await fetch(url, opts); }
    catch (e) { last = e; await sleep(Math.min(2 ** i, 20) * 1000); }
  }
  throw last;
}
const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

// push items with adaptive split; returns {sent, dropped, awarded_exp}
async function pushSignals(env, items, keyFp) {
  const totals = { sent: 0, dropped: 0, duplicates: 0, awarded_exp: 0 };
  const queue = [];
  for (let i = 0; i < items.length; i += BATCH) queue.push(items.slice(i, i + BATCH));
  const synced = [];
  while (queue.length) {
    const chunk = queue.shift();
    const { ok, body } = await sendChunk(env, chunk, keyFp);
    if (ok) {
      totals.sent += Number(body.item_count || chunk.length);
      totals.duplicates += body.duplicate ? 1 : 0;
      totals.awarded_exp += Number(body.awarded_exp || 0);
      for (const it of chunk) synced.push(it.bssid);
    } else if (chunk.length > MIN_SPLIT) {
      const mid = chunk.length >> 1;
      queue.unshift(chunk.slice(mid));
      queue.unshift(chunk.slice(0, mid));
    } else {
      totals.dropped += chunk.length; // impossible_speed etc. — unrecoverable
    }
    await sleep(200);
  }
  // record everything we managed to place (so we never resend it)
  if (synced.length) {
    const now = Date.now();
    for (let i = 0; i < synced.length; i += 50) {
      const slice = synced.slice(i, i + 50);
      const sql = "INSERT OR IGNORE INTO synced (bssid,sent_at) VALUES " +
        slice.map(() => "(?,?)").join(",");
      await env.DB.prepare(sql).bind(...slice.flatMap((b) => [b, now])).run();
    }
  }
  return totals;
}

// ------------------------------------------------------------------ processing
async function unzipEntry(zipBytes, name) {
  const files = unzipSync(zipBytes, { filter: (f) => f.name === name });
  return files[name] || null;
}

async function process(env) {
  const version = await getState(env, "zip_version");
  if (!version) return { skipped: "no backup yet" };

  // cheap gate: only re-scan the DB when the backup actually changed, unless a
  // backlog is still draining
  const processedVer = await getState(env, "processed_version");
  const backlog = (await getState(env, "backlog", "0")) === "1";
  if (version === processedVer && !backlog) return { skipped: "unchanged" };

  const buf = await env.BACKUPS.get("latest.zip", { type: "arrayBuffer" });
  if (!buf) return { skipped: "no backup blob" };
  const zipBytes = new Uint8Array(buf);
  const dbBytes = await unzipEntry(zipBytes, "wardrivego.db");
  if (!dbBytes) return { error: "no wardrivego.db in backup" };

  const db = new SqliteFile(dbBytes);
  const seen = new Set();
  const { results } = await env.DB.prepare("SELECT bssid FROM synced").all();
  for (const row of results) seen.add(row.bssid);

  const items = [];
  let more = false;
  for (const r of db.rows("observations")) {
    if (r.lat == null || r.lng == null) continue;
    const mac = String(r.key || "").toLowerCase();
    if (!MAC_RE.test(mac) || seen.has(mac)) continue;
    if (items.length >= MAX_PER_RUN) { more = true; break; }
    const ts = iso(r.first_seen) || new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
    const item = { ts, lat: r.lat, lon: r.lng, kind: kindOf(r.type), bssid: mac,
                   source_ts: ts, metadata: { source: "wardrivego-backup" } };
    if (r.best_rssi != null) item.rssi = Number(r.best_rssi);
    if (r.name) item.ssid = r.name;
    items.push(item);
  }

  if (!items.length) {
    await setState(env, "processed_version", version);
    await setState(env, "backlog", "0");
    return { done: true, sent: 0, remaining: 0 };
  }

  const keyFp = (await sha256hex(env.WARDRIFT_API_KEY)).slice(0, 16);
  const totals = await pushSignals(env, items, keyFp);
  await setState(env, "backlog", more ? "1" : "0");
  if (!more) await setState(env, "processed_version", version);
  await setState(env, "last_run", JSON.stringify({ at: Date.now(), ...totals, remaining: more }));
  return { ...totals, remaining: more };
}

// ------------------------------------------------------------------ handlers
export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/") {
      const last = await getState(env, "last_run");
      const cnt = await env.DB.prepare("SELECT COUNT(*) n FROM synced").first();
      return html(last ? JSON.parse(last) : null, cnt?.n || 0);
    }

    if (request.method === "GET" && url.pathname === "/run") {
      if (env.SYNC_TOKEN && url.searchParams.get("token") !== env.SYNC_TOKEN)
        return json({ error: "unauthorized" }, 401);
      return json(await process(env));
    }

    if (request.method === "POST") {
      if (env.SYNC_TOKEN) {
        const auth = request.headers.get("Authorization") || "";
        if (auth !== `Bearer ${env.SYNC_TOKEN}`)
          return json({ error: "unauthorized" }, 401);
      }
      const buf = await request.arrayBuffer();
      if (!buf.byteLength) return json({ error: "empty body" }, 400);
      await env.BACKUPS.put("latest.zip", buf, {
        metadata: {
          label: request.headers.get("X-Wardrive-Label") || "",
          networks: request.headers.get("X-Wardrive-Networks") || "",
          device: request.headers.get("X-Wardrive-Device") || "",
        },
      });
      await setState(env, "zip_version", `${Date.now()}-${buf.byteLength}`);
      await setState(env, "backlog", "1"); // force the next cron to scan it
      return json({ accepted: true, bytes: buf.byteLength }, 202);
    }
    return json({ error: "method not allowed" }, 405);
  },

  async scheduled(_event, env, ctx) {
    ctx.waitUntil(process(env).then((r) => console.log("cron:", JSON.stringify(r))));
  },
};

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), { status, headers: { "Content-Type": "application/json" } });
}
function html(last, synced) {
  const body = `<!doctype html><meta charset=utf8><title>wardrift-sync</title>
<style>body{background:#0a0f0a;color:#8f8;font-family:ui-monospace,monospace;max-width:640px;margin:40px auto;padding:0 16px}
h1{color:#4f4}.k{color:#6a6}.v{color:#dfd}code{color:#9f9}</style>
<h1>wardrift-sync</h1>
<p><span class=k>status:</span> <span class=v>operational</span></p>
<p><span class=k>bssids synced:</span> <span class=v>${synced}</span></p>
<p><span class=k>last run:</span> <span class=v>${last ? new Date(last.at).toISOString() : "never"}</span></p>
${last ? `<p><span class=k>last run sent:</span> <span class=v>${last.sent}</span> · dropped ${last.dropped} · +${last.awarded_exp} exp${last.remaining ? " · backlog draining" : ""}</p>` : ""}
<p class=k>POST your Wardrive Go backup .zip here (Sync to a URL).</p>`;
  return new Response(body, { headers: { "Content-Type": "text/html;charset=utf-8" } });
}
