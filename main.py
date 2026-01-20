from fastapi import FastAPI
from dhan_auth import DhanAuth
from dhan_trade import place_order, order_status, cancel_order
import requests, os, time
from datetime import datetime

app = FastAPI(title="Dhan Trading Bridge", version="1.0")
auth = DhanAuth()

@app.get("/")
def home():
    return {
        "status": "ok",
        "message": "Dhan Trading Bridge running 🚀",
        "endpoints": {
            "health": "/health",
            "quote": "/scan?symbol=RELIANCE",
            "order": "/order/place"
        }
    }

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}

@app.get("/token/status")
def token_status():
    return {
        "has_token": bool(auth.access_token),
        "valid_till": auth.expires_at,
        "utc_now": time.time()
    }

@app.get("/scan")
def scan(symbol: str):
    """Fetch live quote"""
    token = auth.get_token()
    headers = {"access-token": token, "client-id": auth.client_id}
    body = {"NSE_EQ": [symbol]}
    r = requests.post(f"{auth.base_url}/v2/marketfeed/quote", headers=headers, json=body)
    if r.status_code == 401:
        auth._login_for_new_token()
        headers["access-token"] = auth.access_token
        r = requests.post(f"{auth.base_url}/v2/marketfeed/quote", headers=headers, json=body)
    return r.json()

@app.post("/order/place")
def order(symbol: str, qty: int, side: str, price: float = None):
    return place_order(symbol, qty, side, price)

@app.get("/order/status")
def order_check(order_id: str):
    return order_status(order_id)

@app.delete("/order/cancel")
def order_cancel(order_id: str):
    return cancel_order(order_id)
