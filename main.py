# =========================================================
# 🧠 Dhan FastAPI Bridge v5.2.2 — BTST + Options + Momentum + News + Sentiment
# =========================================================

from fastapi import FastAPI, Query, HTTPException
from datetime import datetime, timedelta, date
import requests
import os
import csv
import time
from io import StringIO
from typing import Dict, List, Any, Optional

# =========================================================
# 🔧 CONFIGURATION
# =========================================================
app = FastAPI(
    title="Dhan FastAPI Bridge",
    version="5.2.2",
    description="Dhan market data bridge: BTST scan (NSE EQ universe), option chain, option momentum, news sentiment."
)

DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
MARKETAUX_API_KEY = os.getenv("MARKETAUX_API_KEY")

DHAN_BASE = "https://api.dhan.co/v2"
MASTER_CSV = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

# Master CSV cache (serverless-safe: works while instance warm)
MASTER_CACHE_TTL = 6 * 60 * 60  # 6 hours

_MASTER_CACHE: Dict[str, Any] = {
    "fetched_at": 0.0,
    "rows": None,             # Optional[List[Dict[str,str]]]
    "nse_eq_universe": None,  # Optional[List[Dict[str,Any]]]
}

# Scan response cache to prevent repeated 429 (short TTL)
SCAN_CACHE_TTL = 25  # seconds
_SCAN_CACHE: Dict[str, Dict[str, Any]] = {}  # key -> {"t": float, "resp": dict}

SESSION = requests.Session()

# =========================================================
# 🕒 UTIL: INDIAN STANDARD TIME
# =========================================================
def ist_now_str() -> str:
    return (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %I:%M:%S %p IST")

def ist_today() -> date:
    return (datetime.utcnow() + timedelta(hours=5, minutes=30)).date()

def parse_last_trade_date(last_trade_time: str) -> Optional[date]:
    # Typically: "29/01/2026 15:49:31"
    if not last_trade_time or last_trade_time == "N/A":
        return None
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M"):
        try:
            return datetime.strptime(last_trade_time, fmt).date()
        except Exception:
            continue
    return None

def require_dhan_creds():
    if not DHAN_ACCESS_TOKEN or not DHAN_CLIENT_ID:
        raise HTTPException(
            status_code=500,
            detail="Missing DHAN_ACCESS_TOKEN / DHAN_CLIENT_ID in environment."
        )

def _norm(s: str) -> str:
    return "".join(ch for ch in (s or "").upper() if ch.isalnum())

# =========================================================
# 🧾 MASTER CSV LOADER (CACHED)
# =========================================================
def load_master_rows(force: bool = False) -> List[Dict[str, str]]:
    now = time.time()
    if (
        not force
        and _MASTER_CACHE["rows"] is not None
        and (now - _MASTER_CACHE["fetched_at"] < MASTER_CACHE_TTL)
    ):
        return _MASTER_CACHE["rows"]

    res = SESSION.get(MASTER_CSV, timeout=25)
    if res.status_code != 200:
        raise HTTPException(status_code=502, detail="Failed to fetch Dhan master CSV")

    rows = list(csv.DictReader(StringIO(res.text)))
    _MASTER_CACHE["rows"] = rows
    _MASTER_CACHE["fetched_at"] = now
    _MASTER_CACHE["nse_eq_universe"] = None  # reset derived cache
    return rows

def build_nse_eq_universe(force_refresh: bool = False) -> List[Dict[str, Any]]:
    """
    ✅ True equity universe:
      EXCH_ID = NSE
      SEGMENT = E
      SERIES  = EQ
    Returns: [{security_id, symbol_name, display_name}]
    """
    if force_refresh:
        load_master_rows(force=True)

    if _MASTER_CACHE["nse_eq_universe"] is not None:
        return _MASTER_CACHE["nse_eq_universe"]

    rows = load_master_rows()
    universe: List[Dict[str, Any]] = []
    seen = set()

    for r in rows:
        exch = (r.get("EXCH_ID") or "").strip().upper()
        seg = (r.get("SEGMENT") or "").strip().upper()
        series = (r.get("SERIES") or "").strip().upper()

        if exch != "NSE":
            continue
        if seg != "E":
            continue
        if series != "EQ":
            continue

        sid_raw = (r.get("SECURITY_ID") or "").strip()
        sym = (r.get("SYMBOL_NAME") or "").strip()
        disp = (r.get("DISPLAY_NAME") or "").strip()

        if not sid_raw or not sym:
            continue

        try:
            security_id = int(float(sid_raw))
        except Exception:
            continue

        key = (security_id, sym)
        if key in seen:
            continue
        seen.add(key)

        universe.append({
            "security_id": security_id,
            "symbol_name": sym,
            "display_name": disp or sym,
        })

    _MASTER_CACHE["nse_eq_universe"] = universe
    return universe

# =========================================================
# 📊 SMART SYMBOL RESOLVER (CACHED MASTER)
# =========================================================
def resolve_symbol(symbol: str) -> Dict[str, str]:
    """
    Resolve by matching SYMBOL_NAME / DISPLAY_NAME / UNDERLYING_SYMBOL.
    Prefers NSE if multiple matches.
    """
    rows = load_master_rows()
    s = _norm(symbol)

    exact = []
    for r in rows:
        sym = _norm(r.get("SYMBOL_NAME", ""))
        disp = _norm(r.get("DISPLAY_NAME", ""))
        und = _norm(r.get("UNDERLYING_SYMBOL", ""))
        if s == sym or s == disp or s == und:
            exact.append(r)

    if exact:
        for r in exact:
            if (r.get("EXCH_ID") or "").upper() == "NSE":
                return r
        return exact[0]

    candidates = []
    for r in rows:
        combined = _norm(" ".join([
            r.get("SYMBOL_NAME", ""),
            r.get("DISPLAY_NAME", ""),
            r.get("UNDERLYING_SYMBOL", "")
        ]))
        if s and s in combined:
            candidates.append(r)

    if not candidates:
        raise HTTPException(status_code=404, detail=f"Symbol '{symbol}' not found in Dhan master CSV.")

    for r in candidates:
        if (r.get("EXCH_ID") or "").upper() == "NSE":
            return r
    return candidates[0]

# =========================================================
# 🏠 ROOT ENDPOINT
# =========================================================
@app.get("/")
def home():
    return {
        "status": "ok",
        "version": app.version,
        "message": "Dhan FastAPI Bridge — BTST + Options + Momentum + News + Sentiment 🚀",
        "endpoints": {
            "health": "/health",
            "universe": "/universe",
            "scan": "/scan?symbol=RELIANCE",
            "scan_all": "/scan/all?limit=30&offset=0",
            "optionchain": "/optionchain?symbol=TCS",
            "option_momentum": "/option/momentum?symbol=RELIANCE",
            "news": "/news?symbol=RELIANCE"
        },
        "timestamp": ist_now_str()
    }

# =========================================================
# 🩺 HEALTH CHECK
# =========================================================
@app.get("/health")
def health_check():
    return {"status": "ok", "time": ist_now_str()}

# =========================================================
# ✅ UNIVERSE DEBUG (VERIFY ALL NSE EQ)
# =========================================================
@app.get("/universe")
def universe_debug(
    q: str = Query(None, description="Search symbol/display"),
    sample: int = Query(20, ge=1, le=100),
    refresh: bool = Query(False, description="Force refresh master CSV cache")
):
    u = build_nse_eq_universe(force_refresh=refresh)
    if q:
        nq = _norm(q)
        u = [x for x in u if nq in _norm(x["symbol_name"]) or nq in _norm(x["display_name"])]

    return {
        "status": "success",
        "source": MASTER_CSV,
        "filters": {"EXCH_ID": "NSE", "SEGMENT": "E", "SERIES": "EQ"},
        "count": len(u),
        "sample": u[:sample],
        "timestamp": ist_now_str(),
    }

# =========================================================
# 📰 NEWS + SENTIMENT (MARKETAUX)
# =========================================================
@app.get("/news")
def get_news(symbol: str = Query(..., description="Stock symbol for sentiment analysis")):
    try:
        if not MARKETAUX_API_KEY:
            return {"status": "error", "reason": "Missing MarketAux API key", "timestamp": ist_now_str()}

        url = (
            "https://api.marketaux.com/v1/news/all"
            f"?symbols={symbol}&language=en&filter_entities=true&api_token={MARKETAUX_API_KEY}"
        )
        res = SESSION.get(url, timeout=10)
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
            "timestamp": ist_now_str()
        }
    except Exception as e:
        return {"status": "error", "reason": str(e), "timestamp": ist_now_str()}

# =========================================================
# 🔌 DHAN QUOTE (BATCHED)
# =========================================================
def dhan_quote_batch(quote_key: str, security_ids: List[int]) -> Dict[str, Any]:
    require_dhan_creds()

    res = SESSION.post(
        f"{DHAN_BASE}/marketfeed/quote",
        json={quote_key: security_ids},
        headers={
            "access-token": DHAN_ACCESS_TOKEN,
            "client-id": DHAN_CLIENT_ID,
            "Content-Type": "application/json",
        },
        timeout=8,
    )

    # ✅ Graceful rate-limit message
    if res.status_code == 429:
        raise HTTPException(
            status_code=429,
            detail="Dhan rate limit (429). Retry after ~20–30 seconds or reduce max_symbols/batch_size."
        )

    if res.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Dhan quote API failed ({res.status_code})")

    data = res.json().get("data", {})
    return data.get(quote_key, {})

# =========================================================
# 📈 SINGLE STOCK SCAN + SENTIMENT
# =========================================================
@app.get("/scan")
def get_quote(symbol: str = Query(...)):
    try:
        equity = resolve_symbol(symbol)
        exch = (equity.get("EXCH_ID") or "").upper()
        security_id = int(float(equity["SECURITY_ID"]))
        quote_key = "NSE_EQ" if exch == "NSE" else "BSE_EQ"

        qmap = dhan_quote_batch(quote_key, [security_id])
        q = qmap.get(str(security_id), {}) or {}
        last_trade_time = q.get("last_trade_time", "N/A")

        news_data = get_news(symbol)
        sentiment_summary = [
            f"{a.get('title')} ({a.get('sentiment')})"
            for a in news_data.get("articles", [])
        ]

        return {
            "status": "success",
            "symbol": equity.get("SYMBOL_NAME", symbol),
            "exchange": exch,
            "security_id": security_id,
            "last_trade_time": last_trade_time,
            "timestamp": ist_now_str(),
            "quote": q,
            "news_sentiment": sentiment_summary
        }

    except HTTPException:
        raise
    except Exception as e:
        return {"status": "error", "reason": str(e), "timestamp": ist_now_str()}

# =========================================================
# ⚡ BTST SCAN — NSE EQ UNIVERSE (PAGED + BATCHED + SAFE DEFAULTS)
# =========================================================
@app.get("/scan/all")
def scan_all(
    limit: int = Query(30, ge=1, le=200),
    offset: int = Query(0, ge=0, description="Universe offset for paging"),
    # ✅ SAFE DEFAULTS (so /scan/all works without params)
    max_symbols: int = Query(80, ge=50, le=600, description="How many symbols to scan in this call"),
    batch_size: int = Query(60, ge=50, le=200, description="Quote batch size per Dhan request"),
    only_today: bool = Query(True, description="Skip stocks not traded today"),
):
    """
    Scans NSE equities (strict NSE/E/EQ universe) in pages.
    Use offset to scan next pages: 0, 80, 160, 240, ...
    """
    cache_key = f"{offset}:{max_symbols}:{batch_size}:{only_today}"
    now = time.time()
    cached = _SCAN_CACHE.get(cache_key)

    # ✅ Return cached results to avoid repeated 429 on refresh
    if cached and (now - cached["t"] < SCAN_CACHE_TTL):
        resp = cached["resp"]
        resp["top_results"] = resp.get("top_results", [])[:limit]
        return resp

    try:
        universe = build_nse_eq_universe()
        universe_count = len(universe)
        page = universe[offset: offset + max_symbols]

        if not page:
            resp = {
                "status": "success",
                "timestamp": ist_now_str(),
                "universe_count": universe_count,
                "offset": offset,
                "max_symbols": max_symbols,
                "symbols_scanned": 0,
                "top_results": [],
                "note": "No symbols in this page (offset beyond universe)."
            }
            _SCAN_CACHE[cache_key] = {"t": now, "resp": resp}
            return resp

        security_ids = [x["security_id"] for x in page]
        quote_key = "NSE_EQ"
        today = ist_today()

        qmaps: Dict[str, Any] = {}

        # Batched quote fetch with throttle to reduce 429
        for i in range(0, len(security_ids), batch_size):
            chunk = security_ids[i:i + batch_size]
            qmaps.update(dhan_quote_batch(quote_key, chunk))
            if i + batch_size < len(security_ids):
                time.sleep(0.25)  # ✅ stronger throttle

        results = []
        skipped_no_quote = 0
        skipped_stale = 0

        for item in page:
            sid = item["security_id"]
            sym = item["symbol_name"]

            q = qmaps.get(str(sid), {}) or {}
            last_price = q.get("last_price")
            if not last_price:
                skipped_no_quote += 1
                continue

            last_trade_time = q.get("last_trade_time", "N/A")
            if only_today:
                d = parse_last_trade_date(last_trade_time)
                if d is None or d != today:
                    skipped_stale += 1
                    continue

            ohlc = q.get("ohlc", {}) or {}
            prev_close = ohlc.get("close") or float(last_price) or 1.0

            # BTST heuristic
            if float(last_price) > float(prev_close) * 1.015:
                bias, confidence = "BULLISH", 85
            elif float(last_price) < float(prev_close) * 0.985:
                bias, confidence = "BEARISH", 82
            else:
                bias, confidence = "NEUTRAL", 65

            pct = 0.0
            try:
                pct = ((float(last_price) - float(prev_close)) / float(prev_close)) * 100.0
            except Exception:
                pct = 0.0

            results.append({
                "symbol": sym,
                "bias": bias,
                "confidence": confidence,
                "last_price": round(float(last_price), 2),
                "pct_vs_prev_close": round(float(pct), 2),
                "last_trade_time": last_trade_time
            })

        results.sort(key=lambda x: (x["confidence"], x["pct_vs_prev_close"]), reverse=True)

        next_offset = offset + max_symbols
        has_more = next_offset < universe_count

        resp = {
            "status": "success",
            "timestamp": ist_now_str(),
            "source": MASTER_CSV,
            "filters": {"EXCH_ID": "NSE", "SEGMENT": "E", "SERIES": "EQ"},
            "universe_count": universe_count,
            "offset": offset,
            "max_symbols": max_symbols,
            "batch_size": batch_size,
            "only_today": only_today,
            "symbols_in_page": len(page),
            "symbols_scanned": len(results),
            "skipped_no_quote": skipped_no_quote,
            "skipped_stale": skipped_stale,
            "top_results": results[:limit],
            "paging": {"has_more": has_more, "next_offset": next_offset if has_more else None}
        }

        _SCAN_CACHE[cache_key] = {"t": now, "resp": resp}
        return resp

    except HTTPException as he:
        err_resp = {"status": "error", "reason": he.detail, "timestamp": ist_now_str()}
        _SCAN_CACHE[cache_key] = {"t": now, "resp": err_resp}
        raise
    except Exception as e:
        return {"status": "error", "reason": str(e), "timestamp": ist_now_str()}

# =========================================================
# ⚙️ OPTION CHAIN (MASTER CSV CACHE)
# =========================================================
@app.get("/optionchain")
def get_optionchain(symbol: str = Query(...), expiry: str = Query(None)):
    try:
        rows = load_master_rows()
        contracts = []

        for r in rows:
            if (
                (r.get("UNDERLYING_SYMBOL") or "").upper() == symbol.upper()
                and "OPT" in (r.get("INSTRUMENT") or "").upper()
                and (not expiry or (r.get("SM_EXPIRY_DATE") == expiry))
            ):
                strike_raw = (r.get("STRIKE_PRICE") or "").strip()
                strike = None
                if strike_raw:
                    try:
                        strike_val = float(strike_raw)
                        strike = int(strike_val) if strike_val.is_integer() else strike_val
                    except Exception:
                        strike = None

                sec_raw = (r.get("SECURITY_ID") or "").strip()
                try:
                    sec_id = int(float(sec_raw))
                except Exception:
                    continue

                lot_raw = (r.get("LOT_SIZE") or "").strip()
                lot_size = None
                try:
                    lot_size = int(float(lot_raw)) if lot_raw else None
                except Exception:
                    lot_size = None

                contracts.append({
                    "display_name": r.get("DISPLAY_NAME"),
                    "strike": strike,
                    "option_type": r.get("OPTION_TYPE"),
                    "lot_size": lot_size,
                    "expiry": r.get("SM_EXPIRY_DATE"),
                    "security_id": sec_id,
                })

        if not contracts:
            raise HTTPException(status_code=404, detail=f"No option data found for {symbol}")

        return {
            "status": "success",
            "symbol": symbol.upper(),
            "expiry": expiry or contracts[0].get("expiry"),
            "contracts_count": len(contracts),
            "contracts": contracts[:50],
            "timestamp": ist_now_str()
        }

    except HTTPException:
        raise
    except Exception as e:
        return {"status": "error", "reason": str(e), "timestamp": ist_now_str()}

# =========================================================
# 💥 OPTION MOMENTUM ANALYSIS (CE & PE)
# =========================================================
@app.get("/option/momentum")
def analyze_option_momentum(symbol: str = Query(...), expiry: str = Query(None)):
    try:
        require_dhan_creds()
        rows = load_master_rows()

        options = [
            r for r in rows
            if (r.get("UNDERLYING_SYMBOL") or "").upper() == symbol.upper()
            and "OPT" in (r.get("INSTRUMENT") or "").upper()
            and (not expiry or r.get("SM_EXPIRY_DATE") == expiry)
        ]

        records_meta: Dict[int, Dict[str, Any]] = {}
        sec_ids: List[int] = []

        for opt in options[:120]:
            opt_type = (opt.get("OPTION_TYPE") or "").upper()
            if opt_type not in ("CE", "PE"):
                continue

            sec_raw = (opt.get("SECURITY_ID") or "").strip()
            try:
                sec_id = int(float(sec_raw))
            except Exception:
                continue

            strike_raw = (opt.get("STRIKE_PRICE") or "").strip()
            try:
                strike = float(strike_raw) if strike_raw else 0.0
            except Exception:
                strike = 0.0

            records_meta[sec_id] = {"strike": strike, "option_type": opt_type}
            sec_ids.append(sec_id)

        quote_key = "NSE_D"

        quotes: Dict[str, Any] = {}
        for i in range(0, len(sec_ids), 200):
            chunk = sec_ids[i:i + 200]
            quotes.update(dhan_quote_batch(quote_key, chunk))
            if i + 200 < len(sec_ids):
                time.sleep(0.25)

        ce_list, pe_list = [], []

        for sec_id_str, q in quotes.items():
            try:
                sid = int(sec_id_str)
            except Exception:
                continue

            meta = records_meta.get(sid)
            if not meta:
                continue

            qq = q or {}
            strike = meta.get("strike", 0.0)
            opt_type = meta.get("option_type", "NA")

            oi = qq.get("oi", 0) or 0
            ltp = qq.get("last_price", 0) or 0
            ohlc = qq.get("ohlc", {}) or {}
            prev_close = ohlc.get("close", 0) or 0
            change = float(ltp) - float(prev_close)

            record = {"strike": strike, "ltp": ltp, "oi": oi, "change": round(change, 2)}

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
            "timestamp": ist_now_str()
        }

    except HTTPException:
        raise
    except Exception as e:
        return {"status": "error", "reason": str(e), "timestamp": ist_now_str()}
