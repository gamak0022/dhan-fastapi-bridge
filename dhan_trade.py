import os, requests
from dhan_auth import DhanAuth

auth = DhanAuth()

# simple position size limit
MAX_RISK_PER_TRADE = float(os.getenv("MAX_RISK_PER_TRADE", 0.02))  # 2% default

def place_order(symbol, qty, side, price=None, order_type="MARKET", stop_loss=None):
    token = auth.get_token()
    headers = {
        "access-token": token,
        "client-id": auth.client_id,
        "Content-Type": "application/json",
    }

    payload = {
        "transaction_type": "BUY" if side.upper()=="BUY" else "SELL",
        "exchange_segment": "NSE_EQ",
        "product_type": "INTRADAY",
        "security_id": symbol,
        "quantity": qty,
        "order_type": order_type,
        "price": price or 0,
        "after_market_order": False,
        "amo_time": None
    }

    # risk check
    if not _check_risk(qty, price):
        return {"status": "error", "reason": "Risk limit exceeded."}

    r = requests.post(f"{auth.base_url}/orders", headers=headers, json=payload)
    return r.json()

def _check_risk(qty, price):
    try:
        capital = float(os.getenv("CAPITAL", 100000))
        est_value = qty * (price or 1000)
        return est_value <= capital * MAX_RISK_PER_TRADE
    except:
        return False

def order_status(order_id):
    token = auth.get_token()
    r = requests.get(
        f"{auth.base_url}/orders/{order_id}",
        headers={"access-token": token, "client-id": auth.client_id}
    )
    return r.json()

def cancel_order(order_id):
    token = auth.get_token()
    r = requests.delete(
        f"{auth.base_url}/orders/{order_id}",
        headers={"access-token": token, "client-id": auth.client_id}
    )
    return r.json()
