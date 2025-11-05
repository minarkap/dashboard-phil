from __future__ import annotations

from datetime import date
import requests
import pandas as pd
import streamlit as st


FALLBACK_FX_RATES: dict[str, float] = {
    "USD": 0.93,
    "GBP": 1.18,
    "MXN": 0.05,
    "COP": 0.00023,
    "CLP": 0.001,
    "CRC": 0.00188,
    "ARS": 0.001,
    "PEN": 0.25,
    "UYU": 0.023,
}


@st.cache_data(ttl=86400)
def get_fx_timeseries(start_date: date, end_date: date, base_currency: str) -> dict[date, float]:
    base_currency = (base_currency or "EUR").upper()
    if base_currency == "EUR":
        return {}

    start_str = start_date.isoformat()
    end_str = end_date.isoformat()

    try:
        url = "https://api.exchangerate.host/timeseries"
        params = {"start_date": start_str, "end_date": end_str, "base": base_currency, "symbols": "EUR"}
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        rates = data.get("rates", {})
        return {date.fromisoformat(d_str): float(r.get("EUR", 0.0)) for d_str, r in rates.items()}
    except Exception as e:
        st.warning(f"FX timeseries API failed for {base_currency} ({start_str} to {end_str}): {e}. Using fallback rate.")
        fallback_rate = FALLBACK_FX_RATES.get(base_currency, 0.0)
        all_days = pd.date_range(start=start_date, end=end_date, freq="D")
        return {d.date(): fallback_rate for d in all_days}



