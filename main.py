# =========================================================
# 🧠 Dhan FastAPI Bridge v5.0.1 — BTST + Options + Momentum + News + Sentiment
# =========================================================

from fastapi import FastAPI, Query, HTTPException
from datetime import datetime, timedelta
import requests
import os
import csv
from io import StringIO

# =========================================================
# 🔧 CONFIGURATION
# =========================================================
app = FastAPI(
    title="Dhan FastAPI Bridge",
    version="5.0.1",
    description="Unified Dhan market data bridge for BTST scans, option chains, fuzzy symbol resolution, momentum breakouts, and news sentiment."
)

DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
MARKETAUX_API_KEY = os.getenv("MARKETAUX_API_KEY")

DHAN_BASE = "https://api.dhan.co/v2"
MASTER_CSV = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

# =========================================================
# 🕒 UTIL: INDIAN STANDARD TIME
# =========================================================
def ist_now():
    """Return current Indian Standard Time (UTC+5:30)."""
    return (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %I:%M:%S %p IST")

# =========================================================
# 🏠 ROOT ENDPOINT
# =========================================================
@app.get("/")
def home():
    return {
        "status": "ok",
        "version": "5.0.1",
        "message": "Dhan FastAPI Bridge — BTST + Options + Momentum + News + Sentiment 🚀",
        "endpoints": {
            "health": "/health",
            "scan": "/scan?symbol=RELIANCE",
            "scan_all": "/scan/all?limit=50",
            "optionchain": "/optionchain?symbol=TCS",
            "option_momentum": "/option/momentum?symbol=RELIANCE",
            "news": "/news?symbol=RELIANCE"
        },
        "timestamp": ist_now()
    }

# =========================================================
# 🩺 HEALTH CHECK
# =========================================================
@app.get("/health")
def health_check():
    return {"status": "ok", "time": ist_now()}

# =========================================================
# 📊 SMART SYMBOL RESOLVER
# =========================================================
def resolve_symbol(symbol: str):
    csv_response = requests.get(MASTER_CSV, timeout=10)
    csv_data = list(csv.DictReader(StringIO(csv_response.text)))
    symbol_upper = symbol.upper().replace(" ", "")
    candidates = []

    for r in csv_data:
        combined = " ".join([
            r.get("SYMBOL_NAME", ""),
            r.get("DISPLAY_NAME", ""),
            r.get("UNDERLYING_SYMBOL", "")
        ]).upper().replace(" ", "")
        if symbol_upper in combined:
            candidates.append(r)

    if not candidates:
        raise HTTPException(status_code=404, detail=f"Symbol '{symbol}' not found in Dhan master CSV.")

    return next((r for r in candidates if r["EXCH_ID"].upper() == "NSE"), candidates[0])

# =========================================================
# 📰 NEWS + SENTIMENT (MARKETAUX)
# =========================================================
@app.get("/news")
def get_news(symbol: str = Query(..., description="Stock symbol for sentiment analysis")):
    """Fetch top 5 market headlines for a given symbol."""
    try:
        if not MARKETAUX_API_KEY:
            return {"status": "error", "reason": "Missing MarketAux API key", "timestamp": ist_now()}

        url = f"https://api.marketaux.com/v1/news/all?symbols={symbol}&language=en&filter_entities=true&api_token={MARKETAUX_API_KEY}"
        res = requests.get(url, timeout=10)

        if res.status_code != 200:
            raise HTTPException(status_code=502, detail="MarketAux API fetch failed")

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
# 📈 SINGLE STOCK SCAN + SENTIMENT
# =========================================================
@app.get("/scan")
def get_quote(symbol: str = Query(...)):
    """Fetch live market quote and latest sentiment."""
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

        q = res.json().get("data", {}).get(quote_key, {}).get(str(security_id), {})
        last_trade_time = q.get("last_trade_time", "N/A")

        # Fetch sentiment
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
# ⚡ EQUITY-ONLY BTST MARKET SCANNER
# =========================================================
@app.get("/scan/all")
def scan_all(limit: int = 50):
    """Scan NSE equities (only EQUITY/EQ/STOCK) for BTST opportunities."""
    try:
        csv_response = requests.get(MASTER_CSV, timeout=15)
        csv_data = list(csv.DictReader(StringIO(csv_response.text)))

        # ✅ Filter only equity instruments
        equities = [
            r for r in csv_data
            if r["EXCH_ID"].upper() == "NSE"
            and any(word in r["INSTRUMENT"].upper() for word in ["EQUITY", "EQ", "STOCK"])
        ][:150]

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
                if not q or not q.get("last_price"):
                    continue

                ohlc = q.get("ohlc", {})
                last_price = q.get("last_price", 0)
                close = ohlc.get("close", 0) or 1
                last_trade_time = q.get("last_trade_time", "N/A")

                if last_price > close * 1.015:
                    bias, confidence = "BULLISH", 85
                elif last_price < close * 0.985:
                    bias, confidence = "BEARISH", 82
                else:
                    bias, confidence = "NEUTRAL", 65

                results.append({
                    "symbol": symbol,
                    "bias": bias,
                    "confidence": confidence,
                    "last_price": round(last_price, 2),
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
# ⚙️ OPTION CHAIN (SAFE STRIKE CONVERSION)
# =========================================================
@app.get("/optionchain")
def get_optionchain(symbol: str = Query(...), expiry: str = Query(None)):
    """Fetch available option contracts with float-safe strike parsing."""
    try:
        csv_response = requests.get(MASTER_CSV, timeout=10)
        csv_data = csv.DictReader(StringIO(csv_response.text))
        contracts = []

        for r in csv_data:
            if (
                symbol.upper() in r["UNDERLYING_SYMBOL"].upper()
                and "OPT" in r["INSTRUMENT"].upper()
                and (not expiry or r["SM_EXPIRY_DATE"] == expiry)
            ):
                strike_raw = r.get("STRIKE_PRICE", "").strip()
                strike = None
                if strike_raw:
                    try:
                        strike_val = float(strike_raw)
                        strike = int(strike_val) if strike_val.is_integer() else strike_val
                    except ValueError:
                        strike = None

                contracts.append({
                    "display_name": r["DISPLAY_NAME"],
                    "strike": strike,
                    "option_type": r["OPTION_TYPE"],
                    "lot_size": int(float(r["LOT_SIZE"])) if r["LOT_SIZE"] else None,
                    "expiry": r["SM_EXPIRY_DATE"],
                    "security_id": int(float(r["SECURITY_ID"])),
                })

        if not contracts:
            raise HTTPException(status_code=404, detail=f"No option data found for {symbol}")

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
# 💥 OPTION MOMENTUM ANALYSIS (CE & PE)
# =========================================================
@app.get("/option/momentum")
def analyze_option_momentum(symbol: str = Query(...), expiry: str = Query(None)):
    """Detects CE momentum breakouts and PE reversal/hedge opportunities."""
    try:
        csv_response = requests.get(MASTER_CSV)
        csv_data = list(csv.DictReader(StringIO(csv_response.text)))

        options = [
            r for r in csv_data
            if r["UNDERLYING_SYMBOL"].upper() == symbol.upper()
            and "OPT" in r["INSTRUMENT"].upper()
            and (not expiry or r["SM_EXPIRY_DATE"] == expiry)
        ]

        ce_list, pe_list = [], []
        for opt in options[:40]:
            strike = float(opt["STRIKE_PRICE"] or 0)
            opt_type = opt["OPTION_TYPE"]
            exch = opt["EXCH_ID"].upper()
            sec_id = int(opt["SECURITY_ID"])
            quote_key = "NSE_D" if exch == "NSE" else "BSE_D"

            payload = {quote_key: [sec_id]}
            res = requests.post(
                f"{DHAN_BASE}/marketfeed/quote",
                json=payload,
                headers={
                    "access-token": DHAN_ACCESS_TOKEN,
                    "client-id": DHAN_CLIENT_ID,
                    "Content-Type": "application/json",
                },
                timeout=5
            )
            if res.status_code != 200:
                continue

            q = res.json().get("data", {}).get(quote_key, {}).get(str(sec_id), {})
            oi = q.get("oi", 0)
            ltp = q.get("last_price", 0)
            ohlc = q.get("ohlc", {})
            change = ltp - ohlc.get("close", 0)

            record = {"strike": strike, "ltp": ltp, "oi": oi, "change": change}

            if opt_type == "CE":
                ce_list.append(record)
            else:
                pe_list.append(record)

        ce_momentum = [x for x in ce_list if x["change"] > 0 and x["oi"] > 0]
        pe_opportunities = [x for x in pe_list if x["change"] > 0 and x["oi"] > 0]

        ce_momentum.sort(key=lambda x: (x["change"], x["oi"]), reverse=True)
        pe_opportunities.sort(key=lambda x: (x["change"], x["oi"]), reverse=True)

        return {
            "status": "success",
            "symbol": symbol.upper(),
            "expiry": expiry or "nearest",
            "momentum_breakouts": ce_momentum[:3],
            "pe_opportunities": pe_opportunities[:3],
            "timestamp": ist_now()
        }

    except Exception as e:
        return {"status": "error", "reason": str(e), "timestamp": ist_now()}
