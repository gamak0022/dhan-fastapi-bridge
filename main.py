# main.py
from fastapi import FastAPI, Query
import requests, csv, io, os, time
from datetime import datetime, timedelta

app = FastAPI()

# Load credentials
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
DHAN_REFRESH_TOKEN = os.getenv("DHAN_REFRESH_TOKEN")
DHAN_BASE_URL = os.getenv("DHAN_BASE_URL", "https://api.dhan.co")

# In-memory store for access token and expiry
token_cache = {
    "access_token": DHAN_ACCESS_TOKEN,
    "expires_at": time.time() + 3600  # assume valid for 1 hour initially
}


def refresh_token_if_needed():
    """Refresh the Dhan access token automatically if expired."""
    if time.time() < token_cache["expires_at"]:
        return token_cache["access_token"]

    if not DHAN_REFRESH_TOKEN:
        print("⚠️ No refresh token found — using old token.")
        return token_cache["access_token"]

    try:
        print("🔁 Refreshing Dhan token...")
        url = f"{DHAN_BASE_URL}/token/refresh"
        resp = requests.post(url, json={
            "client_id": DHAN_CLIENT_ID,
            "refresh_token": DHAN_REFRESH_TOKEN
        })

        if resp.status_code != 200:
            print("❌ Refresh failed:", resp.text)
            return token_cache["access_token"]

        data = resp.json()
        token_cache["access_token"] = data["access_token"]
        token_cache["expires_at"] = time.time() + int(data.get("expires_in", 3600))
        print("✅ Token refreshed successfully.")
        return token_cache["access_token"]

    except Exception as e:
        print("❌ Token refresh error:", e)
        return token_cache["access_token"]


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.get("/scan")
def scan(symbol: str = Query(...)):
    """Fetch stock quote."""
    access_token = refresh_token_if_needed()

    try:
        # Fetch Dhan CSV
        csv_data = requests.get("https://images.dhan.co/api-data/api-scrip-master-detailed.csv")
        rows = list(csv.DictReader(io.StringIO(csv_data.text)))

        # Find equity row
        symbol = symbol.upper()
        equity = next(
            (r for r in rows if r.get("SYMBOL_NAME", "").upper() == symbol),
            None
        )
        if not equity:
            return {"status": "error", "reason": f"{symbol} not found"}

        exch = "NSE_EQ" if equity["EXCH_ID"] == "NSE" else "BSE_EQ"
        sec_id = int(equity["SECURITY_ID"])

        # Fetch quote
        quote = requests.post(
            f"{DHAN_BASE_URL}/v2/marketfeed/quote",
            json={exch: [sec_id]},
            headers={
                "access-token": access_token,
                "client-id": DHAN_CLIENT_ID,
                "Content-Type": "application/json",
            },
        )

        return quote.json()

    except Exception as e:
        return {"status": "error", "reason": str(e)}
