import csv
import json
import urllib.request
from pathlib import Path

DHAN_SCRIP_MASTER_DETAILED = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

OUT = Path("data/universe_nse_eq.json")
OUT.parent.mkdir(parents=True, exist_ok=True)

def build():
    # Download CSV (stream) and filter NSE / E / EQ
    with urllib.request.urlopen(DHAN_SCRIP_MASTER_DETAILED) as resp:
        text = resp.read().decode("utf-8", errors="ignore").splitlines()

    reader = csv.DictReader(text)

    universe = []
    seen = set()

    for r in reader:
        exch = (r.get("EXCH_ID") or "").strip()
        seg = (r.get("SEGMENT") or "").strip()
        series = (r.get("SERIES") or "").strip()
        sid = (r.get("SECURITY_ID") or "").strip()
        symbol = (r.get("SYMBOL_NAME") or "").strip()
        name = (r.get("DISPLAY_NAME") or "").strip()

        # Core filters: NSE + Equity segment + EQ series
        if exch != "NSE":
            continue
        if seg != "E":
            continue
        if series != "EQ":
            continue
        if not sid or not symbol:
            continue

        key = (sid, symbol)
        if key in seen:
            continue
        seen.add(key)

        universe.append({
            "security_id": int(float(sid)),
            "symbol": symbol,
            "display_name": name or symbol,
            "series": series
        })

    OUT.write_text(json.dumps({
        "source": DHAN_SCRIP_MASTER_DETAILED,
        "filters": {"EXCH_ID": "NSE", "SEGMENT": "E", "SERIES": "EQ"},
        "count": len(universe),
        "universe": universe
    }, indent=2), encoding="utf-8")

    print(f"✅ Wrote {len(universe)} symbols to {OUT}")

if __name__ == "__main__":
    build()
