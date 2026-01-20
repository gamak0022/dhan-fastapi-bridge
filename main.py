# =========================================================
# 🧠 Dhan FastAPI Bridge — v3.4.0 (Multi Segment BTST + Options)
# =========================================================

from fastapi import FastAPI, Query, HTTPException
from datetime import datetime, timedelta
import requests
import os
import csv
from io import StringIO

# =========================================================
# 🔧 Config
# =========================================================
app = FastAPI(
    title="Dhan FastAPI Bridge",
    version="3.4.0",
    description="Smart multi-segment bridge with BTST scanning (Large, Mid, Small caps), option chain, and real-time IST timestamps."
)

DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")

DHAN_BASE = "https://api.dhan.co/v2"
MASTER_CSV = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

# =========================================================
# 🕒 IST Helper
# =========================================================
def ist_now():
    """Return current Indian Standard Time as a string."""
    return (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %I:%M:%S %p")

# =========================================================
# 🏠 Root
# =========================================================
@app.get("/")
def home():
    return {
        "status": "ok",
        "version": "3.4.0",
        "message": "Dhan FastAPI Bridge — Multi Segment BTST + Options 🚀",
        "endpoints": {
            "health": "/health",
            "scan_one": "/scan?symbol=RELIANCE",
            "scan_all": "/scan/all?limit=50",
            "scan_largecap": "/scan/largecap?limit=50",
            "scan_midcap": "/scan/midcap?limit=50",
            "scan_smallcap": "/scan/smallcap?limit=50",
            "scan_custom": "/scan/custom?symbols=RELIANCE,TCS,INFY",
            "optionchain": "/optionchain?symbol=TCS",
            "order": "/order/place"
        },
        "timestamp": ist_now()
    }

# =========================================================
# 🩺 Health
# =========================================================
@app.get("/health")
def health_check():
    return {"status": "ok", "time": ist_now()}

# =========================================================
# 🧩 Core Scanner Utility
# =========================================================
def run_btst_scan(symbols, limit=50):
    results = []
    for eq in symbols:
        try:
            symbol = eq["SYMBOL_NAME"].strip()
            exch = eq["EXCH_ID"].upper()
            security_id = int(eq["SECURITY_ID"])
            quote_key = "NSE_EQ" if exch == "NSE" else "BSE_EQ"

            payload = {quote_key: [security_id]}
            res = requests.post(
                f"{DHAN_BASE}/marketfeed/quote",
                json=payload,
                headers={
                    "access-token": DHAN_ACCESS_TOKEN,
                    "client-id": DHAN_CLIENT_ID,
                    "Content-Type": "application/json",
                },
                timeout=5,
            )

            if res.status_code != 200:
                continue

            q = res.json().get("data", {}).get(quote_key, {}).get(str(security_id), {})
            if not q:
                continue

            ohlc = q.get("ohlc", {})
            last_price = q.get("last_price", 0)
            close = ohlc.get("close", 0) or 1
            last_trade_time = q.get("last_trade_time", "N/A")

            if last_price > close * 1.01:
                bias = "BULLISH"
                confidence = 80
            elif last_price < close * 0.99:
                bias = "BEARISH"
                confidence = 80
            else:
                bias = "NEUTRAL"
                confidence = 65

            results.append({
                "symbol": symbol,
                "exchange": exch,
                "bias": bias,
                "confidence": confidence,
                "last_price": last_price,
                "last_trade_time": last_trade_time
            })
        except Exception:
            continue

    results = sorted(results, key=lambda x: x["confidence"], reverse=True)
    return results[:limit]

# =========================================================
# 📊 Single Stock Quote
# =========================================================
@app.get("/scan")
def get_quote(symbol: str = Query(...)):
    """
    Fetch single stock live quote.
    """
    try:
        csv_response = requests.get(MASTER_CSV, timeout=10)
        csv_data = list(csv.DictReader(StringIO(csv_response.text)))

        symbol_upper = symbol.upper()
        equity = next(
            (
                r for r in csv_data
                if symbol_upper in r["SYMBOL_NAME"].upper()
                or symbol_upper in r["DISPLAY_NAME"].upper()
                or symbol_upper in r["UNDERLYING_SYMBOL"].upper()
            ),
            None
        )
        if not equity:
            raise HTTPException(status_code=404, detail=f"Symbol '{symbol}' not found in Dhan master CSV")

        exch = equity["EXCH_ID"].upper()
        security_id = int(equity["SECURITY_ID"])
        quote_key = "NSE_EQ" if exch == "NSE" else "BSE_EQ"

        payload = {quote_key: [security_id]}
        res = requests.post(
            f"{DHAN_BASE}/marketfeed/quote",
            json=payload,
            headers={
                "access-token": DHAN_ACCESS_TOKEN,
                "client-id": DHAN_CLIENT_ID,
                "Content-Type": "application/json",
            },
            timeout=6,
        )

        data = res.json()
        q = data.get("data", {}).get(quote_key, {}).get(str(security_id), {})
        last_trade_time = q.get("last_trade_time", "N/A")

        return {
            "status": "success",
            "symbol": equity["SYMBOL_NAME"],
            "exchange": exch,
            "security_id": security_id,
            "quote": data,
            "last_trade_time": last_trade_time,
            "timestamp": ist_now()
        }

    except Exception as e:
        return {"status": "error", "reason": str(e), "timestamp": ist_now()}

# =========================================================
# 🏦 Largecap Scan (Top NSE 100)
# =========================================================
@app.get("/scan/largecap")
def scan_largecap(limit: int = 50):
    csv_response = requests.get(MASTER_CSV, timeout=15)
    csv_data = list(csv.DictReader(StringIO(csv_response.text)))
    largecaps = [r for r in csv_data if r["EXCH_ID"].upper() == "NSE"][:100]
    results = run_btst_scan(largecaps, limit)
    return {"status": "success", "segment": "LARGECAP", "timestamp": ist_now(), "top_results": results}

# =========================================================
# 📊 Midcap Scan (NSE 200–500)
# =========================================================
@app.get("/scan/midcap")
def scan_midcap(limit: int = 50):
    csv_response = requests.get(MASTER_CSV, timeout=15)
    csv_data = list(csv.DictReader(StringIO(csv_response.text)))
    midcaps = [r for r in csv_data if "MID" in r["DISPLAY_NAME"].upper() or "500" in r["SYMBOL_NAME"]]  # pseudo filter
    midcaps = midcaps[:150]
    results = run_btst_scan(midcaps, limit)
    return {"status": "success", "segment": "MIDCAP", "timestamp": ist_now(), "top_results": results}

# =========================================================
# 🚀 Smallcap Scan
# =========================================================
@app.get("/scan/smallcap")
def scan_smallcap(limit: int = 50):
    csv_response = requests.get(MASTER_CSV, timeout=15)
    csv_data = list(csv.DictReader(StringIO(csv_response.text)))
    smallcaps = [r for r in csv_data if "SMALL" in r["DISPLAY_NAME"].upper() or "EQ" in r["INSTRUMENT"].upper()]
    smallcaps = smallcaps[:150]
    results = run_btst_scan(smallcaps, limit)
    return {"status": "success", "segment": "SMALLCAP", "timestamp": ist_now(), "top_results": results}

# =========================================================
# 🎯 Custom Symbol Scan
# =========================================================
@app.get("/scan/custom")
def scan_custom(symbols: str = Query(...), limit: int = 50):
    symbol_list = [s.strip().upper() for s in symbols.split(",")]
    csv_response = requests.get(MASTER_CSV, timeout=15)
    csv_data = list(csv.DictReader(StringIO(csv_response.text)))
    matches = [r for r in csv_data if r["SYMBOL_NAME"].upper() in symbol_list]
    results = run_btst_scan(matches, limit)
    return {"status": "success", "segment": "CUSTOM", "symbols": symbol_list, "timestamp": ist_now(), "top_results": results}

# =========================================================
# ⚙️ Option Chain
# =========================================================
@app.get("/optionchain")
def get_optionchain(symbol: str = Query(...), expiry: str = Query(None)):
    csv_response = requests.get(MASTER_CSV, timeout=10)
    csv_data = csv.DictReader(StringIO(csv_response.text))
    contracts = [
        {
            "display_name": r["DISPLAY_NAME"],
            "strike": float(r["STRIKE_PRICE"]) if r["STRIKE_PRICE"] else None,
            "option_type": r["OPTION_TYPE"],
            "lot_size": int(r["LOT_SIZE"]) if r["LOT_SIZE"] else None,
            "expiry": r["SM_EXPIRY_DATE"],
            "security_id": int(r["SECURITY_ID"]),
        }
        for r in csv_data
        if symbol.upper() in r["UNDERLYING_SYMBOL"].upper()
        and "OPT" in r["INSTRUMENT"].upper()
        and (not expiry or r["SM_EXPIRY_DATE"] == expiry)
    ]
    return {
        "status": "success" if contracts else "error",
        "symbol": symbol.upper(),
        "contracts_count": len(contracts),
        "contracts": contracts[:50],
        "timestamp": ist_now(),
    }

# =========================================================
# 💰 Simulated Order
# =========================================================
@app.post("/order/place")
def place_order(symbol: str = Query(...), qty: int = Query(1), side: str = Query(...)):
    return {
        "status": "success",
        "symbol": symbol.upper(),
        "qty": qty,
        "side": side.upper(),
        "message": f"Simulated {side.upper()} order for {qty} shares of {symbol.upper()}",
        "timestamp": ist_now()
    }

# =========================================================
# ✅ End of File
# =========================================================
