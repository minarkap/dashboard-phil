from __future__ import annotations

import os
from typing import Dict, Any, Iterable, Optional
from datetime import datetime, timedelta, date

import requests
from dotenv import load_dotenv


class KajabiClient:
    """Cliente OAuth para la Public API de Kajabi (v1).
    Requiere en .env: KAJABI_BASE_URL (o KAJABI_API_URL), KAJABI_CLIENT_ID, KAJABI_CLIENT_SECRET.
    """

    def __init__(self) -> None:
        load_dotenv()
        base_env = (
            os.getenv("KAJABI_BASE_URL")
            or os.getenv("KAJABI_API_URL")
            or "https://api.kajabi.com"
        )
        self.base_url = base_env.rstrip("/")
        self.client_id = os.getenv("KAJABI_CLIENT_ID") or ""
        self.client_secret = os.getenv("KAJABI_CLIENT_SECRET") or ""
        if not self.client_id or not self.client_secret:
            raise RuntimeError("Faltan KAJABI_CLIENT_ID/KAJABI_CLIENT_SECRET en .env")
        self._token: Optional[str] = None
        self._token_exp: Optional[datetime] = None
        try:
            self.page_size = int(os.getenv("KAJABI_PAGE_SIZE") or 500)
        except Exception:
            self.page_size = 500

    def _ensure_token(self) -> str:
        if self._token and self._token_exp and datetime.utcnow() < self._token_exp:
            return self._token
        resp = requests.post(
            f"{self.base_url}/v1/oauth/token",
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=30,
        )
        resp.raise_for_status()
        js = resp.json()
        self._token = js.get("access_token")
        exp = int(js.get("expires_in") or 3600)
        self._token_exp = datetime.utcnow() + timedelta(seconds=max(60, exp - 60))
        return self._token or ""

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._ensure_token()}",
            "Accept": "application/vnd.api+json",
        }

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        resp = requests.get(
            f"{self.base_url}{path}", params=params or {}, headers=self._headers(), timeout=60
        )
        resp.raise_for_status()
        return resp.json()

    def iter_contacts(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
        max_pages: Optional[int] = None,
    ) -> Iterable[Dict[str, Any]]:
        """Itera contactos (leads) desde Kajabi.
        Nota: La API pública puede no exponer todos los campos UTM; mapeamos lo disponible.
        """
        page = 1
        while True:
            params: Dict[str, Any] = {"page[number]": page, "page[size]": self.page_size}
            js = self._get("/v1/contacts", params=params)
            data = js.get("data", []) or []
            for row in data:
                attrs = row.get("attributes") or {}
                # Filtro local por fechas si se proporcionan
                if start or end:
                    dt_raw = attrs.get("created_at") or attrs.get("updated_at")
                    try:
                        created_dt = datetime.fromisoformat((dt_raw or "").replace("Z", "+00:00")).date()
                    except Exception:
                        created_dt = None
                    if start and created_dt and created_dt < start:
                        continue
                    if end and created_dt and created_dt > end:
                        continue
                yield row
            if not data or len(data) < self.page_size:
                break
            page += 1
            if max_pages is not None and page > max_pages:
                break

    def get_contact_custom_fields(self, contact_id: str) -> Dict[str, Any]:
        """Devuelve un dict {slug: value} de custom fields para el contacto.

        Intenta varias rutas compatibles de la Public API:
        1) /v1/contacts/{id}?include=custom_fields,custom_field_values
        2) /v1/contacts/{id}/custom_field_values (si existe)
        """
        slug_to_value: Dict[str, Any] = {}
        # Intento 1: include en el propio recurso
        try:
            js = self._get(f"/v1/contacts/{contact_id}", params={"include": "custom_fields,custom_field_values"})
            incl = js.get("included", []) or []
            fields_by_id: Dict[str, str] = {}
            for inc in incl:
                if inc.get("type") == "custom_field":
                    fid = inc.get("id")
                    attrs = inc.get("attributes") or {}
                    slug = attrs.get("slug") or attrs.get("name") or attrs.get("label")
                    if fid and slug:
                        fields_by_id[str(fid)] = str(slug)
            for inc in incl:
                if inc.get("type") in ("custom_field_value", "contact_custom_field_value"):
                    attrs = inc.get("attributes") or {}
                    fid = str((inc.get("relationships") or {}).get("custom_field", {}).get("data", {}).get("id") or attrs.get("custom_field_id") or "")
                    slug = fields_by_id.get(fid)
                    if slug:
                        slug_to_value[slug] = attrs.get("value")
            if slug_to_value:
                return slug_to_value
        except Exception:
            pass

        # Intento 2: endpoint directo de values
        try:
            js2 = self._get(f"/v1/contacts/{contact_id}/custom_field_values")
            data = js2.get("data", []) or []
            incl = js2.get("included", []) or []
            fields_by_id: Dict[str, str] = {}
            for inc in incl:
                if inc.get("type") == "custom_field":
                    fid = inc.get("id")
                    attrs = inc.get("attributes") or {}
                    slug = attrs.get("slug") or attrs.get("name") or attrs.get("label")
                    if fid and slug:
                        fields_by_id[str(fid)] = str(slug)
            for row in data:
                attrs = row.get("attributes") or {}
                fid = str(attrs.get("custom_field_id") or "")
                slug = fields_by_id.get(fid)
                if not slug:
                    # Fallback: intenta derivar del propio objeto si trae label
                    slug = attrs.get("label") or attrs.get("name")
                if slug:
                    slug_to_value[str(slug)] = attrs.get("value")
        except Exception:
            pass
        return slug_to_value

    def iter_purchases(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
        include: str = "customer,offer",
        max_pages: Optional[int] = None,
    ) -> Iterable[Dict[str, Any]]:
        page = 1
        while True:
            params: Dict[str, Any] = {"page[number]": page, "page[size]": self.page_size}
            if include:
                params["include"] = include
            # Nota: la API de compras de Kajabi no siempre soporta filtros por fecha con client_credentials.
            js = self._get("/v1/purchases", params=params)
            data = js.get("data", []) or []
            included = js.get("included", []) or []
            inc_map: Dict[tuple, Dict[str, Any]] = {}
            for inc in included:
                inc_map[(inc.get("type"), inc.get("id"))] = inc
            for row in data:
                rel = row.get("relationships", {}) or {}
                cust = None
                off = None
                if rel.get("customer", {}).get("data"):
                    cd = rel["customer"]["data"]
                    cust = inc_map.get((cd.get("type"), cd.get("id")))
                if rel.get("offer", {}).get("data"):
                    od = rel["offer"]["data"]
                    off = inc_map.get((od.get("type"), od.get("id")))
                # Filtro local por fechas si se proporcionan
                if start or end:
                    attrs = (row.get("attributes") or {})
                    dt_raw = attrs.get("effective_start_at") or attrs.get("created_at")
                    try:
                        paid_dt = datetime.fromisoformat((dt_raw or "").replace("Z", "+00:00")).date()
                    except Exception:
                        paid_dt = None
                    if start and paid_dt and paid_dt < start:
                        continue
                    if end and paid_dt and paid_dt > end:
                        continue
                yield {"purchase": row, "customer": cust, "offer": off}
            if not data or len(data) < self.page_size:
                break
            page += 1
            if max_pages is not None and page > max_pages:
                break

    def iter_subscriptions(
        self,
        include: str = "customer,offer",
        max_pages: Optional[int] = None,
    ) -> Iterable[Dict[str, Any]]:
        """Itera suscripciones (API pública)."""
        page = 1
        while True:
            params: Dict[str, Any] = {"page[number]": page, "page[size]": self.page_size}
            if include:
                params["include"] = include
            try:
                js = self._get("/v1/subscriptions", params=params)
            except requests.HTTPError as e:
                # Muchas cuentas no tienen este recurso activo con client_credentials
                if getattr(e, "response", None) is not None and e.response.status_code == 404:
                    return
                raise
            data = js.get("data", []) or []
            included = js.get("included", []) or []
            inc_map: Dict[tuple, Dict[str, Any]] = {}
            for inc in included:
                inc_map[(inc.get("type"), inc.get("id"))] = inc
            for row in data:
                rel = row.get("relationships", {}) or {}
                cust = None
                off = None
                if rel.get("customer", {}).get("data"):
                    cd = rel["customer"]["data"]
                    cust = inc_map.get((cd.get("type"), cd.get("id")))
                if rel.get("offer", {}).get("data"):
                    od = rel["offer"]["data"]
                    off = inc_map.get((od.get("type"), od.get("id")))
                yield {"subscription": row, "customer": cust, "offer": off}
            if not data or len(data) < self.page_size:
                break
            page += 1
            if max_pages is not None and page > max_pages:
                break



