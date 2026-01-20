# main.py
from fastapi import FastAPI, Query
import requests, csv, io, os
from datetime import datetime

app = FastAPI()

# Environment vars (set in Render later)
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")

# Dhan CSV URL
DHAN_CSV_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


def fetch_dhan_csv():
    """Load Dhan master CSV and return list of dicts."""
    resp = requests.get(DHAN_CSV_URL)
    resp.raise_for_status()
    data = list(csv.DictReader(io.StringIO(resp.text)))
    return data


def normalize(s):
    return (s or "").strip().upper()


@app.get("/scan")
def scan(symbol: str = Query(..., description="Symbol like RELIANCE or TCS")):
    """Fetch BTST/equity quote."""
    try:
        rows = fetch_dhan_csv()

        # Match equity instruments
        cash_rows = [
            r for r in rows
            if normalize(r.get("SEGMENT")) != "D"
            and not normalize(r.get("INSTRUMENT")).startswith(("OPT", "FUT"))
        ]

        def is_match(r):
            return (
                normalize(r.get("UNDERLYING_SYMBOL")) == symbol
                or normalize(r.get("SYMBOL_NAME")) == symbol
                or normalize(r.get("DISPLAY_NAME")).startswith(symbol)
            )

        equity = next((r for r in cash_rows if is_match(r)), None)
        if not equity:
            return {"status": "error", "reason": f"Symbol {symbol} not found"}

        exch_id = normalize(equity.get("EXCH_ID"))
        quote_key = "NSE_EQ" if exch_id == "NSE" else "BSE_EQ"
        security_id = int(equity["SECURITY_ID"])

        # Fetch live quote
        url = "https://api.dhan.co/v2/marketfeed/quote"
        payload = {quote_key: [security_id]}
        headers = {
            "access-token": DHAN_ACCESS_TOKEN,
            "client-id": DHAN_CLIENT_ID,
            "Content-Type": "application/json",
        }
        q = requests.post(url, json=payload, headers=headers)
        if q.status_code != 200:
            return {"status": "error", "reason": "Dhan API quote failed", "details": q.text}

        return {
            "status": "success",
            "mode": "EQUITY_QUOTE",
            "symbol": symbol,
            "exchange": exch_id,
            "security_id": security_id,
            "display_name": equity["DISPLAY_NAME"],
            "quote": q.json(),
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        return {"status": "error", "reason": str(e)}


@app.get("/optionchain")
def option_chain(symbol: str = Query(..., description="Symbol name"), expiry: str | None = None):
    """Fetch options contracts from Dhan master CSV."""
    try:
        rows = fetch_dhan_csv()

        option_rows = [
            r for r in rows
            if normalize(r.get("SEGMENT")) == "D"
            and normalize(r.get("INSTRUMENT")) in ("OPTSTK", "OPTIDX")
            and normalize(r.get("UNDERLYING_SYMBOL")) == symbol
        ]

        if not option_rows:
            return {"status": "error", "reason": f"No options found for {symbol}"}

        # Sort by expiry date
        option_rows.sort(key=lambda r: r.get("SM_EXPIRY_DATE") or "2100-01-01")

        expiry_to_use = expiry or option_rows[0]["SM_EXPIRY_DATE"]
        filtered = [r for r in option_rows if r.get("SM_EXPIRY_DATE") == expiry_to_use]

        contracts = [
            {
                "display_name": r["DISPLAY_NAME"],
                "expiry": r["SM_EXPIRY_DATE"],
                "strike": r.get("STRIKE_PRICE"),
                "option_type": r.get("OPTION_TYPE"),
                "lot_size": r.get("LOT_SIZE"),
            }
            for r in filtered
        ][:40]

        return {
            "status": "success",
            "mode": "OPTIONS",
            "symbol": symbol,
            "expiry": expiry_to_use,
            "contracts": contracts,
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        return {"status": "error", "reason": str(e)}
