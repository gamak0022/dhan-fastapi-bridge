# =========================================================
# 🧠 Dhan FastAPI Bridge — Final Production Version
# =========================================================

from fastapi import FastAPI, Query, HTTPException
from datetime import datetime
import requests
import os

# =========================================================
# 🔧 Configuration
# =========================================================
app = FastAPI(title="Dhan FastAPI Bridge", version="2.0.0")

# Environment variables (configured in Vercel)
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")

DHAN_BASE = "https://api.dhan.co/v2"

# =========================================================
# 🏠 Root Endpoint
# =========================================================
@app.get("/")
def home():
    return {
        "status": "ok",
        "message": "Dhan FastAPI Bridge is running 🚀",
        "endpoints": {
            "health": "/health",
            "quote": "/scan?symbol=RELIANCE",
            "optionchain": "/optionchain?symbol=TCS",
            "order": "/order/place"
        }
    }

# =========================================================
# 🩺 Health Check
# =========================================================
@app.get("/health")
def health_check():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}

# =========================================================
# 📊 Scan / Quote Endpoint
# =========================================================
@app.get("/scan")
def get_quote(symbol: str = Query(..., description="Stock symbol, e.g. RELIANCE")):
    """
    Fetch live market quote for a given symbol.
    """
    try:
        # Load Dhan master CSV to resolve security_id
        csv_url = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
        csv_data = requests.get(csv_url).text

        # Find security_id for symbol
        lines = csv_data.split("\n")
        header = lines[0].split(",")
        idx_symbol = header.index("SYMBOL_NAME")
        idx_security = header.index("SECURITY_ID")

        security_id = None
        for line in lines[1:]:
            parts = line.split(",")
            if len(parts) > idx_symbol and parts[idx_symbol].upper() == symbol.upper():
                security_id = int(parts[idx_security])
                break

        if not security_id:
            raise HTTPException(status_code=404, detail="Symbol not found")

        # Query Dhan quote endpoint
        payload = {"NSE_EQ": [security_id]}
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
            "security_id": security_id,
            "quote": res.json(),
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        return {"status": "error", "reason": str(e)}

# =========================================================
# ⚙️ Option Chain Endpoint
# =========================================================
@app.get("/optionchain")
def get_optionchain(symbol: str = Query(...), expiry: str = Query(None)):
    """
    Fetch available option contracts for given symbol.
    """
    try:
        csv_url = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
        data = requests.get(csv_url).text.splitlines()
        header = data[0].split(",")
        idx_underlying = header.index("UNDERLYING_SYMBOL")
        idx_instr = header.index("INSTRUMENT")
        idx_expiry = header.index("SM_EXPIRY_DATE")
        idx_strike = header.index("STRIKE_PRICE")
        idx_type = header.index("OPTION_TYPE")
        idx_disp = header.index("DISPLAY_NAME")
        idx_sec = header.index("SECURITY_ID")
        idx_lot = header.index("LOT_SIZE")

        contracts = []
        for line in data[1:]:
            parts = line.split(",")
            if len(parts) < idx_instr:
                continue
            if parts[idx_underlying].upper() == symbol.upper() and "OPT" in parts[idx_instr]:
                if expiry and parts[idx_expiry] != expiry:
                    continue
                contracts.append({
                    "display_name": parts[idx_disp],
                    "strike": float(parts[idx_strike]) if parts[idx_strike] else None,
                    "option_type": parts[idx_type],
                    "lot_size": int(parts[idx_lot]) if parts[idx_lot] else None,
                    "expiry": parts[idx_expiry],
                    "security_id": int(parts[idx_sec]),
                })

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
# 💰 Place Order (Optional)
# =========================================================
@app.post("/order/place")
def place_order(
    symbol: str = Query(...),
    qty: int = Query(1),
    side: str = Query(..., regex="^(BUY|SELL)$"),
):
    """
    Simulated order placement.
    In production, connect to Dhan order API.
    """
    return {
        "status": "success",
        "message": f"Simulated {side} order placed for {symbol}",
        "quantity": qty,
        "timestamp": datetime.utcnow().isoformat(),
    }

# =========================================================
# ✅ End of File
# =========================================================
