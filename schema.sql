-- dedup: every observation BSSID we've already forwarded to Wardrift
CREATE TABLE IF NOT EXISTS synced (
  bssid   TEXT PRIMARY KEY,
  sent_at INTEGER NOT NULL
);

-- key/value state: wardrift sequence_no, last processed backup, run log
CREATE TABLE IF NOT EXISTS state (
  k TEXT PRIMARY KEY,
  v TEXT
);
