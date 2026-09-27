// Minimal read-only SQLite file reader — enough to stream rows out of one table.
// No WASM, no deps: runs anywhere (Cloudflare Workers included). Handles rowid
// table b-trees (interior + leaf) and overflow pages. Not a general SQLite engine.

function u8(buf) { return buf instanceof Uint8Array ? buf : new Uint8Array(buf); }

function readVarint(bytes, off) {
  let result = 0n;
  for (let i = 0; i < 9; i++) {
    const b = bytes[off + i];
    if (i === 8) { result = (result << 8n) | BigInt(b); return [result, 9]; }
    result = (result << 7n) | BigInt(b & 0x7f);
    if ((b & 0x80) === 0) return [result, i + 1];
  }
  return [result, 9];
}

function be(bytes, off, len) {
  let v = 0n;
  for (let i = 0; i < len; i++) v = (v << 8n) | BigInt(bytes[off + i]);
  return v;
}

// signed big-endian integer of `len` bytes
function beSigned(bytes, off, len) {
  let v = be(bytes, off, len);
  const bits = BigInt(len * 8);
  const signBit = 1n << (bits - 1n);
  if (v & signBit) v -= (1n << bits);
  return v;
}

function serialSize(t) {
  if (t <= 4) return t;
  if (t === 5) return 6;
  if (t === 6 || t === 7) return 8;
  if (t === 8 || t === 9) return 0;
  return (t - (t % 2 === 0 ? 12 : 13)) / 2;
}

export class SqliteFile {
  constructor(buf) {
    this.b = u8(buf);
    if (String.fromCharCode(...this.b.slice(0, 15)) !== "SQLite format 3")
      throw new Error("not a sqlite file");
    let ps = Number(be(this.b, 16, 2));
    this.pageSize = ps === 1 ? 65536 : ps;
    this.reserved = this.b[20];
    this.usable = this.pageSize - this.reserved;
  }

  page(n) { // 1-indexed
    const start = (n - 1) * this.pageSize;
    return this.b.subarray(start, start + this.pageSize);
  }

  // assemble a cell payload, following overflow pages if needed
  _payload(page, pOff, P) {
    const U = this.usable;
    const X = U - 35; // table leaf max local
    if (P <= X) return page.subarray(pOff, pOff + P);
    const M = Math.floor(((U - 12) * 32) / 255) - 23;
    let K = M + ((P - M) % (U - 4));
    const local = K <= X ? K : M;
    const out = new Uint8Array(P);
    out.set(page.subarray(pOff, pOff + local), 0);
    let filled = local;
    let next = Number(be(page, pOff + local, 4));
    while (next !== 0 && filled < P) {
      const op = this.page(next);
      next = Number(be(op, 0, 4));
      const take = Math.min(U - 4, P - filled);
      out.set(op.subarray(4, 4 + take), filled);
      filled += take;
    }
    return out;
  }

  _decodeRecord(rec) {
    const [hdrLen, n0] = readVarint(rec, 0);
    let ho = n0;
    const types = [];
    const headerEnd = Number(hdrLen);
    while (ho < headerEnd) {
      const [t, adv] = readVarint(rec, ho);
      types.push(Number(t));
      ho += adv;
    }
    let bo = headerEnd;
    const vals = [];
    for (const t of types) {
      if (t === 0) { vals.push(null); }
      else if (t >= 1 && t <= 6) { vals.push(beSigned(rec, bo, serialSize(t))); }
      else if (t === 7) {
        const dv = new DataView(rec.buffer, rec.byteOffset + bo, 8);
        vals.push(dv.getFloat64(0, false));
      }
      else if (t === 8) vals.push(0n);
      else if (t === 9) vals.push(1n);
      else if (t >= 12 && t % 2 === 0) { vals.push(rec.subarray(bo, bo + serialSize(t))); }
      else { vals.push(new TextDecoder().decode(rec.subarray(bo, bo + serialSize(t)))); }
      bo += serialSize(t);
    }
    return vals;
  }

  // walk a table b-tree, yielding decoded value arrays for each row
  * _walk(pageNo) {
    const page = this.page(pageNo);
    const hdrOff = pageNo === 1 ? 100 : 0;
    const type = page[hdrOff];
    const nCells = Number(be(page, hdrOff + 3, 2));
    const cellPtrBase = hdrOff + (type === 0x05 ? 12 : 8);
    if (type === 0x05) { // interior table
      for (let i = 0; i < nCells; i++) {
        const cp = Number(be(page, cellPtrBase + i * 2, 2));
        const child = Number(be(page, cp, 4));
        yield* this._walk(child);
      }
      const right = Number(be(page, hdrOff + 8, 4));
      yield* this._walk(right);
    } else if (type === 0x0d) { // leaf table
      for (let i = 0; i < nCells; i++) {
        const cp = Number(be(page, cellPtrBase + i * 2, 2));
        let o = cp;
        const [P, a1] = readVarint(page, o); o += a1;
        const [, a2] = readVarint(page, o); o += a2; // rowid (unused)
        const rec = this._payload(page, o, Number(P));
        yield this._decodeRecord(rec);
      }
    }
  }

  // sqlite_master lives on page 1; find a table's root page + CREATE sql
  _findTable(name) {
    for (const row of this._walk(1)) {
      // columns: type, name, tbl_name, rootpage, sql
      if (row[0] === "table" && row[1] === name)
        return { root: Number(row[3]), sql: row[4] };
    }
    return null;
  }

  // parse "CREATE TABLE x (colA TYPE, colB TYPE, ...)" -> ordered column names
  static _columns(sql) {
    const inner = sql.slice(sql.indexOf("(") + 1, sql.lastIndexOf(")"));
    const cols = [];
    let depth = 0, cur = "";
    for (const ch of inner) {
      if (ch === "(") depth++;
      if (ch === ")") depth--;
      if (ch === "," && depth === 0) { cols.push(cur.trim()); cur = ""; }
      else cur += ch;
    }
    if (cur.trim()) cols.push(cur.trim());
    return cols
      .map((c) => c.trim())
      .filter((c) => !/^(PRIMARY|UNIQUE|CHECK|FOREIGN|CONSTRAINT)\b/i.test(c))
      .map((c) => c.replace(/^[`"\[]?([A-Za-z0-9_]+)[`"\]]?.*/, "$1"));
  }

  // yield {col: value} objects for a table
  * rows(tableName) {
    const t = this._findTable(tableName);
    if (!t) throw new Error(`table ${tableName} not found`);
    const cols = SqliteFile._columns(t.sql);
    for (const vals of this._walk(t.root)) {
      const o = {};
      for (let i = 0; i < cols.length; i++) o[cols[i]] = vals[i] === undefined ? null : vals[i];
      yield o;
    }
  }
}
