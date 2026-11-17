"""
Keeps candle history files up to date.
Reads config.json, then for every symbol and timeframe:
  - first run: downloads the last `backfill_days` days
  - later runs: downloads only the candles after the last saved one
Files are written to data/<SYMBOL>_<SECONDS>.csv  (time,open,high,low,close; time = UTC epoch seconds)
Uses Deriv's public market-data WebSocket. No account, token or password is needed.
"""
import csv, json, os, sys, time
from datetime import datetime, timezone

import websocket  # pip install websocket-client

ENDPOINTS = [e for e in [
    os.environ.get("DERIV_WS"),
    "wss://api.derivws.com/trading/v1/options/ws/public",
    "wss://ws.derivws.com/websockets/v3?app_id=1089",
] if e]
DATA = "data"
PAGE = 5000


class Feed:
    def __init__(self):
        self.ws = None
        self.rid = 0

    def connect(self):
        last = None
        for url in ENDPOINTS:
            try:
                self.ws = websocket.create_connection(url, timeout=30)
                print("connected:", url.split("?")[0])
                return
            except Exception as e:
                last = e
                print("could not connect to", url.split("?")[0], "-", e)
        raise RuntimeError("no Deriv endpoint reachable: %s" % last)

    def candles(self, symbol, gran, **kw):
        for attempt in range(3):
            try:
                if self.ws is None:
                    self.connect()
                self.rid += 1
                msg = {"ticks_history": symbol, "style": "candles", "granularity": gran,
                       "count": PAGE, "adjust_start_time": 1, "req_id": self.rid}
                msg.update(kw)
                self.ws.send(json.dumps(msg))
                while True:
                    d = json.loads(self.ws.recv())
                    if d.get("req_id") == self.rid or "candles" in d or "error" in d:
                        break
                if d.get("error"):
                    raise RuntimeError(d["error"].get("message", "error"))
                return [(int(c["epoch"]), float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"]))
                        for c in d.get("candles", [])]
            except Exception as e:
                print("  retry %d for %s: %s" % (attempt + 1, symbol, e))
                try:
                    if self.ws: self.ws.close()
                except Exception:
                    pass
                self.ws = None
                time.sleep(2 * (attempt + 1))
        raise RuntimeError("failed to fetch %s" % symbol)


def read(path):
    rows = {}
    if os.path.exists(path):
        with open(path, newline="") as f:
            for r in csv.reader(f):
                try:
                    rows[int(r[0])] = (int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]))
                except (ValueError, IndexError):
                    continue
    return rows


def write(path, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["time", "open", "high", "low", "close"])
        for t in sorted(rows):
            w.writerow(rows[t])


def update(feed, symbol, gran, backfill_days, max_candles):
    path = os.path.join(DATA, "%s_%d.csv" % (symbol, gran))
    rows = read(path)
    now = int(time.time())
    closed = lambda c: c[0] + gran <= now
    added = 0
    if rows:
        start = max(rows) + gran
        for _ in range(40):
            if now < start + gran:
                break
            got = [c for c in feed.candles(symbol, gran, start=start, end="latest") if closed(c)]
            new = [c for c in got if c[0] not in rows]
            for c in new:
                rows[c[0]] = c
            added += len(new)
            if not new:
                break
            start = max(rows) + gran
    else:
        target = now - backfill_days * 86400
        end = "latest"
        for _ in range(60):
            got = feed.candles(symbol, gran, end=end)
            got = [c for c in got if closed(c)]
            new = [c for c in got if c[0] not in rows]
            for c in new:
                rows[c[0]] = c
            added += len(new)
            if not new or min(rows) <= target:
                break
            end = min(rows) - 1
    if len(rows) > max_candles:
        for t in sorted(rows)[:len(rows) - max_candles]:
            del rows[t]
    if added:
        write(path, rows)
    return len(rows), added, (min(rows) if rows else None), (max(rows) if rows else None)


def main():
    cfg = json.load(open("config.json"))
    os.makedirs(DATA, exist_ok=True)
    feed = Feed()
    manifest = {"updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"), "series": {}}
    ok = 0
    for sym in cfg["symbols"]:
        for gran in cfg["timeframes"]:
            try:
                n, added, first, last = update(feed, sym, gran, cfg.get("backfill_days", 90), cfg.get("max_candles", 120000))
                print("%s %ss: %d candles (+%d)" % (sym, gran, n, added))
                if n:
                    manifest["series"]["%s_%d" % (sym, gran)] = {"first": first, "last": last, "count": n}
                ok += 1
            except Exception as e:
                print("%s %ss FAILED: %s" % (sym, gran, e))
    old = {}
    try:
        old = json.load(open(os.path.join(DATA, "manifest.json")))
    except Exception:
        pass
    if old.get("series") != manifest["series"]:
        json.dump(manifest, open(os.path.join(DATA, "manifest.json"), "w"), indent=1)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
