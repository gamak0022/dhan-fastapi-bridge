# =========================================================
# 🧠 Dhan FastAPI Bridge v4.0.0 — BTST + Options + News + Sentiment
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
    version="4.0.0",
    description="Full-featured market intelligence bridge with BTST scans, option chains, fuzzy symbol matching, live IST timestamps, and news sentiment."
)

DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
MARKETAUX_API_KEY = os.getenv("MARKETAUX_API_KEY")  # <-- Add in Vercel env vars

DHAN_BASE = "https://api.dhan.co/v2"
MASTER_CSV = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

# =========================================================
# 🕒 IST Helper
# =========================================================
def ist_now():
    """Return current Indian Standard Time (UTC+5:30)"""
    return (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %I:%M:%S %p")

# =========================================================
# 🏠 Root
# =========================================================
@app.get("/")
def home():
    return {
        "status": "ok",
        "version": "4.0.0",
        "message": "Dhan FastAPI Bridge — BTST + Options + News + Sentiment 🚀",
        "endpoints": {
            "health": "/health",
            "scan": "/scan?symbol=RELIANCE",
            "scan/all": "/scan/all?limit=50",
            "optionchain": "/optionchain?symbol=TCS",
            "news": "/news?symbol=RELIANCE"
        },
        "timestamp": ist_now()
    }

# =========================================================
# 🩺 Health Check
# =========================================================
@app.get("/health")
def health_check():
    return {"status": "ok", "time": ist_now()}

# =========================================================
# 📊 Smart Symbol Resolver
# =========================================================
def resolve_symbol(symbol: str):
    """Fuzzy symbol resolver that matches SYMBOL_NAME, DISPLAY_NAME, or UNDERLYING_SYMBOL."""
    csv_response = requests.get(MASTER_CSV, timeout=10)
    csv_data = list(csv.DictReader(StringIO(csv_response.text)))
    symbol_upper = symbol.upper().replace(" ", "")
    candidates = []

    for r in csv_data:
        combo = " ".join([
            r.get("SYMBOL_NAME", ""),
            r.get("DISPLAY_NAME", ""),
            r.get("UNDERLYING_SYMBOL", "")
        ]).upper().replace(" ", "")
        if symbol_upper in combo:
            candidates.append(r)

    if not candidates:
        raise HTTPException(status_code=404, detail=f"Symbol '{symbol}' not found in Dhan master CSV.")

    return next((r for r in candidates if r["EXCH_ID"].upper() == "NSE"), candidates[0])

# =========================================================
# 📰 News + Sentiment
# =========================================================
@app.get("/news")
def get_news(symbol: str = Query(..., description="Stock symbol for sentiment analysis")):
    """
    Fetch top 5 market headlines for a symbol using MarketAux API.
    """
    try:
        url = f"https://api.marketaux.com/v1/news/all?symbols={symbol}&language=en&filter_entities=true&api_token={MARKETAUX_API_KEY}"
        res = requests.get(url, timeout=10)

        if res.status_code != 200:
            raise HTTPException(status_code=502, detail="MarketAux API fetch failed.")

        articles = res.json().get("data", [])[:5]
        return {
            "status": "success",
            "symbol": symbol.upper(),
            "articles": [
                {
                    "title": a.get("title"),
                    "summary": a.get("description"),
                    "sentiment": a.get("sentiment"),
                    "published_at": a.get("published_at")
                }
                for a in articles
            ],
            "timestamp": ist_now()
        }

    except Exception as e:
        return {"status": "error", "reason": str(e), "timestamp": ist_now()}

# =========================================================
# 📈 Single Stock Scan (Improved)
# =========================================================
@app.get("/scan")
def get_quote(symbol: str = Query(...)):
    """Fetch live market quote and sentiment for a symbol."""
    try:
        equity = resolve_symbol(symbol)
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

        # --- Fetch News Sentiment ---
        news_data = get_news(symbol)
        sentiment_summary = [
            f"{a['title']} ({a['sentiment']})" for a in news_data.get("articles", [])
        ]

        return {
            "status": "success",
            "symbol": equity["SYMBOL_NAME"],
            "exchange": exch,
            "security_id": security_id,
            "last_trade_time": last_trade_time,
            "timestamp": ist_now(),
            "quote": q,
            "news_sentiment": sentiment_summary
        }

    except Exception as e:
        return {"status": "error", "reason": str(e), "timestamp": ist_now()}

# =========================================================
# ⚡ BTST Market Scan
# =========================================================
@app.get("/scan/all")
def scan_all(limit: int = 50):
    """Fast BTST scan with sentiment layer."""
    try:
        csv_response = requests.get(MASTER_CSV, timeout=15)
        csv_data = list(csv.DictReader(StringIO(csv_response.text)))
        equities = [
            r for r in csv_data
            if r["SEGMENT"].upper() != "D"
            and "OPT" not in r["INSTRUMENT"].upper()
            and "FUT" not in r["INSTRUMENT"].upper()
            and r["EXCH_ID"].upper() == "NSE"
        ][:100]

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
                q = res.json().get("data", {}).get(quote_key, {}).get(str(security_id), {})
                if not q:
                    continue

                ohlc = q.get("ohlc", {})
                last_price = q.get("last_price", 0)
                close = ohlc.get("close", 0) or 1
                last_trade_time = q.get("last_trade_time", "N/A")

                if last_price > close * 1.01:
                    bias, confidence = "BULLISH", 80
                elif last_price < close * 0.99:
                    bias, confidence = "BEARISH", 80
                else:
                    bias, confidence = "NEUTRAL", 65

                results.append({
                    "symbol": symbol,
                    "bias": bias,
                    "confidence": confidence,
                    "last_price": last_price,
                    "last_trade_time": last_trade_time
                })
            except Exception:
                continue

        results = sorted(results, key=lambda x: x["confidence"], reverse=True)
        return {
            "status": "success",
            "timestamp": ist_now(),
            "symbols_scanned": len(results),
            "top_results": results[:limit]
        }

    except Exception as e:
        return {"status": "error", "reason": str(e), "timestamp": ist_now()}

# =========================================================
# ⚙️ Option Chain
# =========================================================
@app.get("/optionchain")
def get_optionchain(symbol: str = Query(...), expiry: str = Query(None)):
    """Fetch available option contracts."""
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
            "timestamp": ist_now()
        }

    except Exception as e:
        return {"status": "error", "reason": str(e), "timestamp": ist_now()}

# =========================================================
# ✅ End of File
# =========================================================
