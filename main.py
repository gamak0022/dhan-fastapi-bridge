# =========================================================
# 🧠 Dhan FastAPI Bridge v5.3.0 — BTST + Options + Momentum + News + Sentiment
# ✅ NSE Equity Universe (NSE / E / EQ)
# ✅ Uses % vs DAY OPEN (works morning + close)
# ✅ Safe defaults to avoid 429
# ✅ Samples across whole universe by default (no paging needed for GPT)
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
    version="5.3.0",
    description="BTST scan (NSE EQ universe), option chain, option momentum, news sentiment."
)

DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
MARKETAUX_API_KEY = os.getenv("MARKETAUX_API_KEY")

DHAN_BASE = "https://api.dhan.co/v2"
MASTER_CSV = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

SESSION = requests.Session()

# Master CSV cache (warm instance only)
MASTER_CACHE_TTL = 6 * 60 * 60  # 6 hours
_MASTER_CACHE: Dict[str, Any] = {"fetched_at": 0.0, "rows": None, "nse_eq_universe": None}

# Short scan cache to avoid repeated 429 on refresh
SCAN_CACHE_TTL = 25  # seconds
_SCAN_CACHE: Dict[str, Dict[str, Any]] = {}  # key -> {"t": float, "resp": dict}

# =========================================================
# 🕒 UTIL
# =========================================================
def ist_now_str() -> str:
    return (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %I:%M:%S %p IST")

def ist_today() -> date:
    return (datetime.utcnow() + timedelta(hours=5, minutes=30)).date()

def parse_last_trade_date(last_trade_time: str) -> Optional[date]:
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
        raise HTTPException(status_code=500, detail="Missing DHAN_ACCESS_TOKEN / DHAN_CLIENT_ID in env.")

def _norm(s: str) -> str:
    return "".join(ch for ch in (s or "").upper() if ch.isalnum())

# =========================================================
# 🧾 MASTER CSV (CACHED)
# =========================================================
def load_master_rows(force: bool = False) -> List[Dict[str, str]]:
    now = time.time()
    if (not force and _MASTER_CACHE["rows"] is not None and (now - _MASTER_CACHE["fetched_at"] < MASTER_CACHE_TTL)):
        return _MASTER_CACHE["rows"]

    res = SESSION.get(MASTER_CSV, timeout=25)
    if res.status_code != 200:
        raise HTTPException(status_code=502, detail="Failed to fetch Dhan master CSV")

    rows = list(csv.DictReader(StringIO(res.text)))
    _MASTER_CACHE["rows"] = rows
    _MASTER_CACHE["fetched_at"] = now
    _MASTER_CACHE["nse_eq_universe"] = None
    return rows

def build_nse_eq_universe(force_refresh: bool = False) -> List[Dict[str, Any]]:
    """
    ✅ Strict universe:
      EXCH_ID=NSE, SEGMENT=E, SERIES=EQ
    Plus extra filtering to remove ETFs/MFs etc.
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

        if exch != "NSE" or seg != "E" or series != "EQ":
            continue

        sid_raw = (r.get("SECURITY_ID") or "").strip()
        sym = (r.get("SYMBOL_NAME") or "").strip()
        disp = (r.get("DISPLAY_NAME") or "").strip()
        instr = (r.get("INSTRUMENT") or "").strip().upper()

        if not sid_raw or not sym:
            continue

        # Extra exclusion: remove ETFs/MFs/bonds/etc if they sneak in
        name_blob = f"{sym} {disp} {instr}".upper()
        if any(bad in name_blob for bad in ["ETF", "MUTUAL", "MF", "BOND", "GSEC", "GOVT", "SDL", "NCD", "DEBENTURE"]):
            continue

        try:
            security_id = int(float(sid_raw))
        except Exception:
            continue

        key = (security_id, sym)
        if key in seen:
            continue
        seen.add(key)

        universe.append({"security_id": security_id, "symbol_name": sym, "display_name": disp or sym})

    _MASTER_CACHE["nse_eq_universe"] = universe
    return universe

# =========================================================
# 📊 SYMBOL RESOLVER
# =========================================================
def resolve_symbol(symbol: str) -> Dict[str, str]:
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
        combined = _norm(" ".join([r.get("SYMBOL_NAME", ""), r.get("DISPLAY_NAME", ""), r.get("UNDERLYING_SYMBOL", "")]))
        if s and s in combined:
            candidates.append(r)

    if not candidates:
        raise HTTPException(status_code=404, detail=f"Symbol '{symbol}' not found in Dhan master CSV.")

    for r in candidates:
        if (r.get("EXCH_ID") or "").upper() == "NSE":
            return r
    return candidates[0]

# =========================================================
# 🏠 ROOT + HEALTH
# =========================================================
@app.get("/")
def home():
    return {
        "status": "ok",
        "version": app.version,
        "message": "Dhan FastAPI Bridge — BTST scan + Options + Momentum + News",
        "endpoints": {
            "health": "/health",
            "universe": "/universe",
            "scan": "/scan?symbol=HINDUSTAN%20COPPER",
            "scan_all": "/scan/all?limit=30",
            "optionchain": "/optionchain?symbol=TCS",
            "option_momentum": "/option/momentum?symbol=RELIANCE",
            "news": "/news?symbol=RELIANCE"
        },
        "timestamp": ist_now_str()
    }

@app.get("/health")
def health_check():
    return {"status": "ok", "time": ist_now_str()}

# =========================================================
# ✅ UNIVERSE DEBUG
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
# 📰 NEWS (MARKETAUX)
# =========================================================
@app.get("/news")
def get_news(symbol: str = Query(...)):
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
# 🔌 DHAN QUOTE (BATCH)
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

    if res.status_code == 429:
        raise HTTPException(status_code=429, detail="Dhan rate limit (429). Retry after ~20–30 seconds.")
    if res.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Dhan quote API failed ({res.status_code})")

    data = res.json().get("data", {})
    return data.get(quote_key, {})

# =========================================================
# 📈 SINGLE STOCK SCAN
# =========================================================
@app.get("/scan")
def scan_single(symbol: str = Query(...)):
    try:
        equity = resolve_symbol(symbol)
        exch = (equity.get("EXCH_ID") or "").upper()
        security_id = int(float(equity["SECURITY_ID"]))
        quote_key = "NSE_EQ" if exch == "NSE" else "BSE_EQ"

        qmap = dhan_quote_batch(quote_key, [security_id])
        q = qmap.get(str(security_id), {}) or {}

        news_data = get_news(symbol)
        sentiment_summary = [f"{a.get('title')} ({a.get('sentiment')})" for a in news_data.get("articles", [])]

        return {
            "status": "success",
            "symbol": equity.get("SYMBOL_NAME", symbol),
            "exchange": exch,
            "security_id": security_id,
            "last_trade_time": q.get("last_trade_time", "N/A"),
            "timestamp": ist_now_str(),
            "quote": q,
            "news_sentiment": sentiment_summary
        }

    except HTTPException:
        raise
    except Exception as e:
        return {"status": "error", "reason": str(e), "timestamp": ist_now_str()}

# =========================================================
# ⚡ BTST SCAN (works morning + close)
# ✅ Default = sample across whole universe (so GPT doesn’t need paging)
# =========================================================
@app.get("/scan/all")
def scan_all(
    limit: int = Query(30, ge=1, le=200),
    # SAFE DEFAULTS (avoid 429, also tool can’t pass these)
    max_symbols: int = Query(50, ge=20, le=200, description="How many symbols to scan per request"),
    batch_size: int = Query(50, ge=20, le=100, description="Quote batch size per Dhan request"),
    only_today: bool = Query(True),
    # IMPORTANT: covers the whole universe without paging
    spread: bool = Query(True, description="If true, sample across the entire universe (recommended)."),
    spread_shift: int = Query(0, ge=0, le=5000, description="Shift start index for spread sampling"),
    mode: str = Query("btst", description="btst | morning"),
):
    """
    - mode=morning: focuses on intraday momentum vs OPEN.
    - mode=btst: adds close-near-high filter (still based on OPEN).
    """
    cache_key = f"{limit}:{max_symbols}:{batch_size}:{only_today}:{spread}:{spread_shift}:{mode}"
    now = time.time()
    cached = _SCAN_CACHE.get(cache_key)
    if cached and (now - cached["t"] < SCAN_CACHE_TTL):
        resp = cached["resp"]
        resp["top_results"] = resp.get("top_results", [])[:limit]
        return resp

    try:
        universe = build_nse_eq_universe()
        universe_count = len(universe)
        today = ist_today()

        # Choose which symbols to scan
        if spread:
            # stride sample across entire universe so we don’t always scan the same first 50
            stride = max(1, universe_count // max_symbols)
            indices = []
            start = spread_shift % max(1, universe_count)
            i = start
            while len(indices) < max_symbols and i < universe_count:
                indices.append(i)
                i += stride
            page = [universe[idx] for idx in indices if idx < universe_count]
        else:
            # sequential first N (not recommended unless you page manually)
            page = universe[:max_symbols]

        security_ids = [x["security_id"] for x in page]
        quote_key = "NSE_EQ"

        # Batch fetch with small throttle (reduces 429)
        qmaps: Dict[str, Any] = {}
        for i in range(0, len(security_ids), batch_size):
            chunk = security_ids[i:i + batch_size]
            qmaps.update(dhan_quote_batch(quote_key, chunk))
            if i + batch_size < len(security_ids):
                time.sleep(0.25)

        results = []
        skipped_no_quote = 0
        skipped_stale = 0

        for item in page:
            sid = item["security_id"]
            sym = item["symbol_name"]

            q = qmaps.get(str(sid), {}) or {}
            ltp = q.get("last_price")
            if not ltp:
                skipped_no_quote += 1
                continue

            ltt = q.get("last_trade_time", "N/A")
            if only_today:
                d = parse_last_trade_date(ltt)
                if d is None or d != today:
                    skipped_stale += 1
                    continue

            ohlc = q.get("ohlc", {}) or {}
            day_open = ohlc.get("open") or 0
            day_high = ohlc.get("high") or 0
            day_low = ohlc.get("low") or 0
            vol = q.get("volume", 0) or 0

            if not day_open:
                # if open missing, skip because morning logic depends on it
                skipped_no_quote += 1
                continue

            pct_vs_open = ((float(ltp) - float(day_open)) / float(day_open)) * 100.0

            # range position: 0 = at low, 1 = at high
            range_pos = 0.5
            if day_high and day_low and day_high != day_low:
                range_pos = (float(ltp) - float(day_low)) / (float(day_high) - float(day_low))
                range_pos = max(0.0, min(1.0, range_pos))

            # Scoring heuristic
            bullish = pct_vs_open >= 1.2
            bearish = pct_vs_open <= -1.2

            # For BTST, we prefer close nearer to high (range_pos)
            if mode.lower() == "btst":
                if bullish and range_pos >= 0.70:
                    bias, confidence = "BULLISH", 85
                elif bearish and range_pos <= 0.30:
                    bias, confidence = "BEARISH", 82
                else:
                    bias, confidence = "NEUTRAL", 65
            else:
                # Morning mode: momentum vs open only (lighter)
                if bullish:
                    bias, confidence = "BULLISH", 80
                elif bearish:
                    bias, confidence = "BEARISH", 78
                else:
                    bias, confidence = "NEUTRAL", 65

            results.append({
                "symbol": sym,
                "bias": bias,
                "confidence": confidence,
                "last_price": round(float(ltp), 2),
                "pct_vs_open": round(float(pct_vs_open), 2),
                "range_pos": round(float(range_pos), 2),
                "volume": int(vol),
                "last_trade_time": ltt
            })

        # Sort: confidence then pct_vs_open
        results.sort(key=lambda x: (x["confidence"], x["pct_vs_open"]), reverse=True)

        resp = {
            "status": "success",
            "timestamp": ist_now_str(),
            "source": MASTER_CSV,
            "filters": {"EXCH_ID": "NSE", "SEGMENT": "E", "SERIES": "EQ"},
            "universe_count": universe_count,
            "spread": spread,
            "spread_shift": spread_shift,
            "max_symbols": max_symbols,
            "batch_size": batch_size,
            "mode": mode,
            "only_today": only_today,
            "symbols_scanned": len(results),
            "skipped_no_quote": skipped_no_quote,
            "skipped_stale": skipped_stale,
            "top_results": results[:limit]
        }

        _SCAN_CACHE[cache_key] = {"t": now, "resp": resp}
        return resp

    except HTTPException as he:
        err = {"status": "error", "reason": he.detail, "timestamp": ist_now_str()}
        _SCAN_CACHE[cache_key] = {"t": now, "resp": err}
        raise
    except Exception as e:
        return {"status": "error", "reason": str(e), "timestamp": ist_now_str()}

# =========================================================
# ⚙️ OPTION CHAIN (LIST CONTRACTS)
# =========================================================
@app.get("/optionchain")
def optionchain(symbol: str = Query(...), expiry: str = Query(None)):
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
# 💥 OPTION MOMENTUM (SUBSET)
# =========================================================
@app.get("/option/momentum")
def option_momentum(symbol: str = Query(...), expiry: str = Query(None)):
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

            rec = {"strike": strike, "ltp": ltp, "oi": oi, "change": round(change, 2)}
            if opt_type == "CE":
                ce_list.append(rec)
            else:
                pe_list.append(rec)

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
