import os, requests
from dhan_auth import DhanAuth

auth = DhanAuth()
CAPITAL = float(os.getenv("CAPITAL", 100000))
MAX_RISK = float(os.getenv("MAX_RISK_PER_TRADE", 0.02))  # 2%

def _risk_ok(qty, price):
    est_value = qty * (price or 1000)
    return est_value <= CAPITAL * MAX_RISK

def place_order(symbol, qty, side, price=None, order_type="MARKET"):
    token = auth.get_token()
    headers = {
        "access-token": token,
        "client-id": auth.client_id,
        "Content-Type": "application/json"
    }

    payload = {
        "transaction_type": side.upper(),
        "exchange_segment": "NSE_EQ",
        "product_type": "INTRADAY",
        "security_id": symbol,
        "quantity": qty,
        "order_type": order_type,
        "price": price or 0,
        "after_market_order": False
    }

    if not _risk_ok(qty, price):
        return {"status": "error", "reason": "Risk limit exceeded."}

    r = requests.post(f"{auth.base_url}/orders", headers=headers, json=payload)
    if r.status_code == 401:
        # token expired: re-login once
        auth._login_for_new_token()
        headers["access-token"] = auth.access_token
        r = requests.post(f"{auth.base_url}/orders", headers=headers, json=payload)
    try:
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"status": "error", "reason": str(e)}

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
