# =========================================================
# ⚙️ Option Chain (Safe Strike Conversion)
# =========================================================
@app.get("/optionchain")
def get_optionchain(symbol: str = Query(...), expiry: str = Query(None)):
    """
    Fetch available option contracts for a given underlying symbol.
    Auto-handles float/integer strike formats safely.
    """
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
                if strike_raw:
                    try:
                        # Convert safely: float → int if whole number
                        strike_val = float(strike_raw)
                        strike = int(strike_val) if strike_val.is_integer() else strike_val
                    except ValueError:
                        strike = None
                else:
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
