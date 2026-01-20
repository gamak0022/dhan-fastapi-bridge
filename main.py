from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from dhan_auth import DhanAuth
from dhan_trade import place_order, order_status, cancel_order
import os, requests
from datetime import datetime

app = FastAPI(title="Dhan FastAPI Trading Bridge", version="2.0")
auth = DhanAuth()

# Optional protection: uncomment if you want API key
# API_KEY = os.getenv("GPT_API_KEY")
# @app.middleware("http")
# async def verify_key(request: Request, call_next):
#     if request.url.path not in ["/", "/health"]:
#         if request.headers.get("x-api-key") != API_KEY:
#             return JSONResponse(status_code=401, content={"error": "Unauthorized"})
#     return await call_next(request)

@app.get("/")
def home():
    return {
        "status": "ok",
        "message": "Dhan Trading Bridge live 🚀",
        "endpoints": {
            "health": "/health",
            "quote": "/scan?symbol=RELIANCE",
            "optionchain": "/optionchain?symbol=TCS",
            "order": "/order/place"
        }
    }

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}

@app.get("/token/status")
def token_status():
    return {
        "access_token_present": bool(auth.access_token),
        "valid_till_utc": auth.expires_at,
        "utc_now": time.time()
    }

@app.get("/scan")
def scan(symbol: str):
    token = auth.get_token()
    r = requests.post(
        f"{auth.base_url}/v2/marketfeed/quote",
        headers={"access-token": token, "client-id": auth.client_id},
        json={"NSE_EQ": [symbol]}
    )
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
