# wardrift-sync

Serverless bridge that forwards your **[Wardrive Go](https://play.google.com/store/apps/details?id=com.rocketgod.wardrive)** captures to the **[Wardrift](https://wardrift.net)** leaderboard — fully automatic, zero-tap, no PC.

Wardrive Go uploads your drives to WiGLE and WDGWars, but has **no Wardrift target**. Its "Sync to a URL" feature POSTs the full backup `.zip` (the app's SQLite DB) to any endpoint on app close. This Cloudflare Worker is that endpoint: it unzips the backup, reads the observations, and forwards the new ones to Wardrift's ingest API — at full fidelity (real BSSID/SSID/RSSI and the true GPS trail), so Wardrift awards proper EXP instead of flagging everything `non_fixed`.

```
Wardrive Go  ──POST .zip──►  wardrift-sync (Worker)  ──►  Wardrift /v1/ingest/signals
 (phone, on close)            unzip → read DB → dedup → push
```

## Why it's interesting

- **Pure-JS SQLite reader** (`src/sqlite.js`, ~230 lines, no WASM) — reads the backup's `wardrivego.db` b-tree (interior/leaf pages, overflow pages, record serial types) so it runs in the Workers runtime with zero native deps. Tested byte-exact against `sqlite3`.
- **`fflate`** streams a single entry out of the zip (only decompresses `wardrivego.db`, skips the rest).
- **Idempotent, resumable delta sync** — D1 tracks every forwarded BSSID; each run only pushes what's new. A per-run cap keeps every invocation inside the CPU/time budget; a cron drains any backlog.
- **Adaptive batching** — Wardrift rebuilds the drive route and rejects trails with impossible jumps; a rejected 250-item batch is halved and retried, isolating one bad stretch instead of sinking the whole upload.

## Layout

| file | what |
|---|---|
| `src/index.js` | fetch (ingest + `/run` + status) & scheduled (cron) handlers, Wardrift push |
| `src/sqlite.js` | dependency-free read-only SQLite file reader |
| `schema.sql` | D1: `synced` (dedup) + `state` (kv) |
| `wrangler.jsonc` | bindings: KV `BACKUPS`, D1 `DB`, cron |

## Deploy

```bash
npm install
wrangler kv namespace create BACKUPS      # paste id into wrangler.jsonc
wrangler d1 create wardrift-sync          # paste id into wrangler.jsonc
wrangler d1 execute wardrift-sync --remote --file=schema.sql
wrangler secret put SYNC_TOKEN            # bearer the app must send (choose one)
wrangler secret put WARDRIFT_API_KEY      # your wardrift.net character API key (wdk_...)
wrangler deploy
```

Then in Wardrive Go → **Settings → Updates & maintenance → Backup / Restore data → Sync to a URL**: set the endpoint to your Worker URL, the auth token to your `SYNC_TOKEN`, and enable it (plus *Auto-backup on close*).

## Endpoints

- `POST /` — receives the backup `.zip` (checked against `SYNC_TOKEN`), stores it, returns `202`.
- `GET /run?token=…` — process the stored backup now (manual drain / debugging).
- `GET /` — tiny status page (bssids synced, last run).
- cron `*/10 * * * *` — drains any pending backlog.

## Notes

- Cellular rows are skipped (no MAC → Wardrift rejects with `invalid_mac`).
- Wardrift hashes bssid/ssid server-side; the wire carries raw values over TLS.
- Wardrift's ingest protocol was reverse-engineered from `PierreGode/Ragnar` + live probing.

Not affiliated with Wardrive Go or Wardrift. For your own data only.
