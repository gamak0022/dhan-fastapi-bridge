# main.py
from fastapi import FastAPI, Query
import requests, csv, io
from datetime import datetime
from dhan_auth import DhanAuth

app = FastAPI(title="Dhan Market Data Bridge", version="1.0")

auth = DhanAuth()
DHAN_CSV_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"


def fetch_csv():
    """Download Dhan scrip master CSV."""
    resp = requests.get(DHAN_CSV_URL)
    resp.raise_for_status()
    return list(csv.DictReader(io.StringIO(resp.text)))


def norm(x): 
    return (x or "").strip().upper()


@app.get("/")
def home():
    """Root message for easy testing."""
    return {
        "status": "ok",
        "message": "Dhan FastAPI Bridge is running 🚀",
        "endpoints": {
            "health": "/health",
            "quote": "/scan?symbol=RELIANCE",
            "optionchain": "/optionchain?symbol=TCS",
        },
    }


@app.get("/health")
def health():
    """Simple health check."""
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.get("/scan")
def scan(symbol: str = Query(..., description="Symbol like RELIANCE or TCS")):
    """Fetch live equity quote from Dhan."""
    try:
        rows = fetch_csv()
        cash_rows = [r for r in rows if norm(r["SEGMENT"]) != "D"]

        equity = next(
            (
                r for r in cash_rows
                if norm(r["SYMBOL_NAME"]) == norm(symbol)
                or norm(r["UNDERLYING_SYMBOL"]) == norm(symbol)
                or norm(r["DISPLAY_NAME"]).startswith(norm(symbol))
            ),
            None,
        )
        if not equity:
            return {"status": "error", "reason": f"Symbol {symbol} not found"}

        exch = "NSE_EQ" if norm(equity["EXCH_ID"]) == "NSE" else "BSE_EQ"
        sec_id = int(equity["SECURITY_ID"])
        token = auth.get_token()

        quote = requests.post(
            f"{auth.base_url}/v2/marketfeed/quote",
            json={exch: [sec_id]},
            headers={
                "access-token": token,
                "client-id": auth.client_id,
                "Content-Type": "application/json",
            },
        )

        return {
            "status": "success",
            "symbol": symbol,
            "exchange": exch,
            "security_id": sec_id,
            "display_name": equity["DISPLAY_NAME"],
            "quote": quote.json(),
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        return {"status": "error", "reason": str(e)}


@app.get("/optionchain")
def option_chain(symbol: str = Query(...), expiry: str | None = None):
    """Fetch simplified option chain from Dhan CSV."""
    try:
        rows = fetch_csv()
        options = [
            r for r in rows
            if norm(r["SEGMENT"]) == "D"
            and norm(r["INSTRUMENT"]) in ("OPTSTK", "OPTIDX")
            and norm(r["UNDERLYING_SYMBOL"]) == norm(symbol)
        ]
        if not options:
            return {"status": "error", "reason": f"No option data found for {symbol}"}

        options.sort(key=lambda x: x.get("SM_EXPIRY_DATE") or "2100-01-01")
        expiry_date = expiry or options[0]["SM_EXPIRY_DATE"]

        filtered = [r for r in options if r["SM_EXPIRY_DATE"] == expiry_date]
        contracts = [
            {
                "display_name": r["DISPLAY_NAME"],
                "strike": r["STRIKE_PRICE"],
                "option_type": r["OPTION_TYPE"],
                "lot_size": r["LOT_SIZE"],
                "expiry": r["SM_EXPIRY_DATE"],
            }
            for r in filtered
        ][:40]

        return {
            "status": "success",
            "symbol": symbol,
            "expiry": expiry_date,
            "contracts": contracts,
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        return {"status": "error", "reason": str(e)}
