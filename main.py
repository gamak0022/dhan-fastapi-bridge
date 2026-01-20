# =========================================================
# 🧠 Dhan FastAPI Bridge — Full Market BTST Scanner Version
# =========================================================

from fastapi import FastAPI, Query, HTTPException
from datetime import datetime
import requests
import os
import csv
from io import StringIO

# =========================================================
# 🔧 Config
# =========================================================
app = FastAPI(title="Dhan FastAPI Bridge", version="3.0.0")

DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")

DHAN_BASE = "https://api.dhan.co/v2"
MASTER_CSV = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

# =========================================================
# 🏠 Root
# =========================================================
@app.get("/")
def home():
    return {
        "status": "ok",
        "message": "Dhan FastAPI Bridge (Full Market BTST Scanner) 🚀",
        "endpoints": {
            "health": "/health",
            "scan_one": "/scan?symbol=RELIANCE",
            "scan_all": "/scan/all?limit=50",
            "optionchain": "/optionchain?symbol=TCS",
            "order": "/order/place"
        }
    }

# =========================================================
# 🩺 Health
# =========================================================
@app.get("/health")
def health_check():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}

# =========================================================
# 📊 Single Stock Scan
# =========================================================
@app.get("/scan")
def get_quote(symbol: str = Query(...)):
    """
    Fetch live market quote for a given symbol.
    """
    try:
        # Load CSV to get security ID
        csv_response = requests.get(MASTER_CSV)
        csv_data = csv.DictReader(StringIO(csv_response.text))
        equity = next((r for r in csv_data if r["SYMBOL_NAME"].upper() == symbol.upper()), None)
        if not equity:
            raise HTTPException(status_code=404, detail="Symbol not found")

        security_id = int(equity["SECURITY_ID"])
        exch = equity["EXCH_ID"].upper()
        quote_key = "NSE_EQ" if exch == "NSE" else "BSE_EQ"

        # Fetch live quote
        payload = {quote_key: [security_id]}
        res = requests.post(
            f"{DHAN_BASE}/marketfeed/quote",
            json=payload,
            headers={
                "access-token": DHAN_ACCESS_TOKEN,
                "client-id": DHAN_CLIENT_ID,
                "Content-Type": "application/json",
            },
        )

        if res.status_code != 200:
            raise HTTPException(status_code=500, detail="Dhan quote fetch failed")

        return {
            "status": "success",
            "symbol": symbol.upper(),
            "exchange": exch,
            "security_id": security_id,
            "quote": res.json(),
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        return {"status": "error", "reason": str(e)}

# =========================================================
# ⚙️ Option Chain
# =========================================================
@app.get("/optionchain")
def get_optionchain(symbol: str = Query(...), expiry: str = Query(None)):
    """
    Fetch available option contracts for given symbol.
    """
    try:
        csv_response = requests.get(MASTER_CSV)
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
            if r["UNDERLYING_SYMBOL"].upper() == symbol.upper()
            and "OPT" in r["INSTRUMENT"].upper()
            and (not expiry or r["SM_EXPIRY_DATE"] == expiry)
        ]

        if not contracts:
            raise HTTPException(status_code=404, detail=f"No option data for {symbol}")

        return {
            "status": "success",
            "symbol": symbol.upper(),
            "expiry": expiry or contracts[0]["expiry"],
            "contracts_count": len(contracts),
            "contracts": contracts[:50],
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        return {"status": "error", "reason": str(e)}

# =========================================================
# 📈 Bulk Market BTST Scanner
# =========================================================
@app.get("/scan/all")
def scan_all(limit: int = Query(50, description="Top results to return")):
    """
    Scans the entire Dhan equity universe (NSE/BSE cash)
    and returns top BTST opportunities with bias/confidence.
    """
    try:
        csv_response = requests.get(MASTER_CSV)
        csv_data = csv.DictReader(StringIO(csv_response.text))

        # Filter only equity instruments
        equities = [
            r for r in csv_data
            if r["SEGMENT"].upper() != "D"
            and "OPT" not in r["INSTRUMENT"].upper()
            and "FUT" not in r["INSTRUMENT"].upper()
            and r["EXCH_ID"].upper() in ["NSE", "BSE"]
        ]

        results = []
        for eq in equities:
            symbol = eq["SYMBOL_NAME"]
            exch = eq["EXCH_ID"].upper()
            security_id = int(eq["SECURITY_ID"])
            quote_key = "NSE_EQ" if exch == "NSE" else "BSE_EQ"

            # Fetch quote
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
            close = ohlc.get("close", 0)

            bias = "NEUTRAL"
            confidence = 60

            # Simple technical logic
            if last_price > close * 1.01:
                bias = "BULLISH"
                confidence = 75
            elif last_price < close * 0.99:
                bias = "BEARISH"
                confidence = 75

            results.append({
                "symbol": symbol,
                "exchange": exch,
                "bias": bias,
                "confidence": confidence,
                "last_price": last_price,
            })

        # Sort & return top results
        results = sorted(results, key=lambda x: x["confidence"], reverse=True)
        return {
            "status": "success",
            "timestamp": datetime.utcnow().isoformat(),
            "symbols_scanned": len(results),
            "top_results": results[:limit]
        }

    except Exception as e:
        return {"status": "error", "reason": str(e)}

# =========================================================
# 💰 Simulated Order
# =========================================================
@app.post("/order/place")
def place_order(symbol: str = Query(...), qty: int = Query(1), side: str = Query(...)):
    return {
        "status": "success",
        "message": f"Simulated {side.upper()} order placed for {qty} shares of {symbol.upper()}",
        "timestamp": datetime.utcnow().isoformat(),
    }

# =========================================================
# ✅ End of File
# =========================================================
