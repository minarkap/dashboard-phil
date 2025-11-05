from __future__ import annotations

import os
from datetime import datetime
from typing import Dict, Iterable, Optional

import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
import base64


load_dotenv()

HOTMART_CLIENT_ID = os.getenv("HOTMART_CLIENT_ID")
HOTMART_CLIENT_SECRET = os.getenv("HOTMART_CLIENT_SECRET")
HOTMART_ACCESS_TOKEN = os.getenv("HOTMART_ACCESS_TOKEN")  # opcional si ya lo tienes

# Probar primero dominio canónico que suele redirigir a la región adecuada,
# y como fallback el host regional más común.
BASE_URLS = [
    "https://api.hotmart.com",
    "https://api-sec-vlc.hotmart.com",
]


def _headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(5))
def get_token() -> str:
    # 1) Token directo desde entorno (limpio por si viene con comillas/espacios)
    env_token = (os.getenv("HOTMART_ACCESS_TOKEN") or HOTMART_ACCESS_TOKEN or "").strip().strip('"').strip("'")
    if env_token:
        return env_token

    # 2) Client credentials
    if not HOTMART_CLIENT_ID or not HOTMART_CLIENT_SECRET:
        raise RuntimeError("Configura HOTMART_CLIENT_ID y HOTMART_CLIENT_SECRET o HOTMART_ACCESS_TOKEN")

    basic = base64.b64encode(f"{HOTMART_CLIENT_ID}:{HOTMART_CLIENT_SECRET}".encode()).decode()
    headers_basic = {
        "Authorization": f"Basic {basic}",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }
    headers_plain = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }

    last_err: Exception | None = None
    for base in BASE_URLS:
        for headers in (headers_basic, headers_plain):
            try:
                resp = requests.post(
                    f"{base}/security/oauth/token",
                    data={
                        "grant_type": "client_credentials",
                        "client_id": HOTMART_CLIENT_ID,
                        "client_secret": HOTMART_CLIENT_SECRET,
                    },
                    headers=headers,
                    timeout=30,
                )
                resp.raise_for_status()
                try:
                    data = resp.json() or {}
                except Exception:
                    data = {}
                token = (data or {}).get("access_token")
                if token:
                    return token
            except Exception as e:
                last_err = e
    raise RuntimeError(f"Hotmart: no se pudo obtener access_token ({last_err})")


@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(5))
def list_transactions(updated_after: Optional[datetime] = None) -> Iterable[Dict]:
    token = get_token()
    path_candidates = [
        "/payments/api/v1/transactions",
        "/payments/api/v1/sales",
    ]
    page = 1
    while True:
        params = {"page": page, "rows": 100}
        if updated_after:
            params["start_date"] = int(updated_after.timestamp() * 1000)

        response_ok = None
        for base in BASE_URLS:
            for path in path_candidates:
                url = f"{base}{path}"
                resp = requests.get(url, headers=_headers(token), params=params, timeout=60, allow_redirects=True)
                if resp.status_code in (200, 206):
                    response_ok = resp
                    break
                # 404: probar siguiente path/base
                if resp.status_code in (401, 403):
                    # Autorización inválida
                    resp.raise_for_status()
            if response_ok:
                break

        if not response_ok:
            raise requests.HTTPError("No se encontró un endpoint válido para Hotmart (404 en todas las rutas)")

        # Parseo robusto: algunos proxies devuelven HTML o cuerpo vacío con 200
        data = {}
        try:
            if "application/json" in (response_ok.headers.get("Content-Type") or "").lower():
                data = response_ok.json() or {}
            else:
                data = response_ok.json() or {}
        except Exception:
            data = {}
        items = data.get("items") or data.get("list") or []
        for it in items:
            yield it
        total_pages = (data.get("total_pages") or data.get("totalPages") or 1)
        if page >= int(total_pages):
            break
        page += 1


