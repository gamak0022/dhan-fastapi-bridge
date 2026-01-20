from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse
from dhan_auth import DhanAuth
from dhan_trade import place_order, order_status, cancel_order
import requests, csv, io, os
from datetime import datetime

app = FastAPI(title="Dhan Trading Bridge", version="2.0")
auth = DhanAuth()

API_KEY = os.getenv("GPT_API_KEY")

@app.middleware("http")
async def verify_key(request: Request, call_next):
    if request.url.path not in ["/", "/health"]:
        if request.headers.get("x-api-key") != API_KEY:
            return JSONResponse(status_code=401, content={"error": "Unauthorized"})
    return await call_next(request)

@app.get("/")
def home():
    return {
        "status": "ok",
        "message": "Dhan Trading Bridge live 🚀",
        "endpoints": {
            "health": "/health",
            "quote": "/scan?symbol=RELIANCE",
            "optionchain": "/optionchain?symbol=NIFTY",
            "order": "/order/place"
        }
    }

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}

@app.post("/order/place")
async def order(symbol: str, qty: int, side: str, price: float = None):
    return place_order(symbol, qty, side, price)

@app.get("/order/status")
def order_check(order_id: str):
    return order_status(order_id)

@app.delete("/order/cancel")
def order_cancel(order_id: str):
    return cancel_order(order_id)
