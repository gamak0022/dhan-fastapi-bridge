# =========================================================
# 🧠 Dhan FastAPI Bridge — BTST + Options (Vercel Optimized)
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
app = FastAPI(
    title="Dhan FastAPI Bridge",
    version="3.2.0",
    description="Vercel-optimized Dhan market data bridge — BTST scanner, option chain, and trade simulation."
)

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
        "message": "Dhan FastAPI Bridge (Full Market BTST + Options) 🚀",
        "version": "3.2.0",
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
def get_quote(symbol: str = Query(..., description="Stock symbol, e.g. RELIANCE, TCS, HDFCBANK")):
    """
    Fetch live market quote for a given symbol with flexible matching.
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

        if res.status_code != 200:
            raise HTTPException(status_code=502, detail="Failed to fetch quote from Dhan")

        return {
            "status": "success",
            "symbol": equity["SYMBOL_NAME"],
            "exchange": exch,
            "security_id": security_id,
            "quote": res.json(),
            "timestamp": datetime.utcnow().isoformat(),
        }

    except HTTPException as e:
        raise e
    except Exception as e:
        return {"status": "error", "reason": str(e)}

# =========================================================
# ⚙️ Option Chain
# =========================================================
@app.get("/optionchain")
def get_optionchain(symbol: str = Query(...), expiry: str = Query(None)):
    """
    Fetch available option contracts for a given symbol.
    """
    try:
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

    except HTTPException as e:
        raise e
    except Exception as e:
        return {"status": "error", "reason": str(e)}

# =========================================================
# ⚡ Optimized BTST Scanner (Safe for Vercel Runtime)
# =========================================================
@app.get("/scan/all")
def scan_all(
    limit: int = Query(50, description="Top results to return"),
    sample_size: int = Query(150, description="Number of equities to scan (default: 150)")
):
    """
    Fast BTST Scanner — scans a limited subset of equities (default 150) for Vercel runtime safety.
    """
    try:
        csv_response = requests.get(MASTER_CSV, timeout=15)
        csv_data = list(csv.DictReader(StringIO(csv_response.text)))

        equities = [
            r for r in csv_data
            if r["SEGMENT"].upper() != "D"
            and "OPT" not in r["INSTRUMENT"].upper()
            and "FUT" not in r["INSTRUMENT"].upper()
            and r["EXCH_ID"].upper() == "NSE"
        ][:sample_size]

        results = []
        for eq in equities:
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
                })
            except Exception:
                continue

        results = sorted(results, key=lambda x: x["confidence"], reverse=True)

        return {
            "status": "success",
            "timestamp": datetime.utcnow().isoformat(),
            "symbols_scanned": len(results),
            "top_results": results[:limit],
            "note": "Fast scan mode active — limited to top 150 equities for Vercel runtime."
        }

    except Exception as e:
        return {"status": "error", "reason": str(e)}

# =========================================================
# 💰 Simulated Order
# =========================================================
@app.post("/order/place")
def place_order(
    symbol: str = Query(...),
    qty: int = Query(1),
    side: str = Query(..., regex="^(BUY|SELL|buy|sell)$")
):
    """
    Simulates a buy/sell order for GPT testing and analysis workflows.
    """
    return {
        "status": "success",
        "symbol": symbol.upper(),
        "qty": qty,
        "side": side.upper(),
        "message": f"Simulated {side.upper()} order for {qty} shares of {symbol.upper()}",
        "timestamp": datetime.utcnow().isoformat(),
    }

# =========================================================
# ✅ End of File
# =========================================================
