from __future__ import annotations

from datetime import datetime, timedelta, date
from typing import Dict, Any, Optional

from sqlalchemy import text as _t
from backend.db.config import db_session, init_db
from .kajabi_client import KajabiClient


def _parse_iso(dt: Optional[str]) -> Optional[datetime]:
    if not dt:
        return None
    try:
        return datetime.fromisoformat(dt.replace('Z', '+00:00'))
    except Exception:
        return None


def _get_state(key: str) -> Optional[str]:
    with db_session() as s:
        try:
            row = s.execute(_t("SELECT value FROM sync_state WHERE key=:k ORDER BY id DESC LIMIT 1"), {"k": key}).mappings().first()
            return row["value"] if row else None
        except Exception:
            return None


def _set_state(key: str, value: str) -> None:
    with db_session() as s:
        try:
            row = s.execute(_t("SELECT id FROM sync_state WHERE key=:k ORDER BY id DESC LIMIT 1"), {"k": key}).mappings().first()
            if row:
                s.execute(_t("UPDATE sync_state SET value=:v, updated_at=NOW() WHERE id=:id"), {"v": value, "id": row["id"]})
            else:
                s.execute(_t("INSERT INTO sync_state(key, value, updated_at) VALUES (:k, :v, NOW())"), {"k": key, "v": value})
        except Exception:
            pass


def run_kajabi_leads_sync(days_back: int = 365) -> Dict[str, int]:
    """Sincroniza contactos/leads desde Kajabi y guarda UTMs/CLIDs en leads_kajabi."""
    try:
        init_db()
    except Exception:
        pass

    # Recarga defensiva del cliente por si la app mantiene un módulo antiguo
    try:
        import importlib
        cli_mod = importlib.import_module("backend.etl.kajabi_client")
        try:
            cli_mod = importlib.reload(cli_mod)  # type: ignore
        except Exception:
            pass
        _KajabiClient = getattr(cli_mod, "KajabiClient", KajabiClient)
        client = _KajabiClient()
    except Exception:
        client = KajabiClient()
    cursor_key = "kajabi_leads_cursor"
    state = _get_state(cursor_key)

    start: Optional[date] = None
    end: Optional[date] = None
    if not state:
        end = date.today()
        start = end - timedelta(days=days_back)

    detected = 0
    inserted = 0
    updated = 0
    max_seen: Optional[datetime] = None

    # Generador de contactos con fallback si el cliente no expone iter_contacts
    def _iter_contacts_fallback(_client):
        page = 1
        while True:
            try:
                js = _client._get("/v1/contacts", params={"page[number]": page, "page[size]": 500})
            except Exception:
                break
            data = js.get("data", []) or []
            for r in data:
                yield r
            if not data or len(data) < 500:
                break
            page += 1

    if hasattr(client, "iter_contacts"):
        iter_gen = client.iter_contacts(start=start, end=end)
    else:
        iter_gen = _iter_contacts_fallback(client)

    for row in iter_gen:
        detected += 1
        attrs: Dict[str, Any] = row.get("attributes") or {}
        cid = str(row.get("id") or "").strip()
        email = (attrs.get("email") or "").lower() or None
        created_at = _parse_iso(attrs.get("created_at") or attrs.get("updated_at")) or datetime.utcnow()

        utm_source = attrs.get("utm_source") or None
        utm_medium = attrs.get("utm_medium") or None
        utm_campaign = attrs.get("utm_campaign") or None
        utm_content = attrs.get("utm_content") or None
        gclid = attrs.get("gclid") or None
        fbclid = attrs.get("fbclid") or None

        # Fallback a custom fields
        try:
            cf = client.get_contact_custom_fields(cid)
        except Exception:
            cf = {}
        if cf:
            utm_source = utm_source or cf.get("utm_source") or cf.get("UTM Source")
            utm_medium = utm_medium or cf.get("utm_medium") or cf.get("UTM Medium")
            utm_campaign = utm_campaign or cf.get("utm_campaign") or cf.get("UTM Campaign")
            utm_content = utm_content or cf.get("utm_content") or cf.get("UTM Content")
            gclid = gclid or cf.get("gclid") or cf.get("GCLID")
            fbclid = fbclid or cf.get("fbclid") or cf.get("FBCLID")

        platform: Optional[str] = None
        src_l = (utm_source or "").lower()
        if gclid or any(k in src_l for k in ("google", "adwords", "google_ads")):
            platform = "google_ads"
        elif fbclid or any(k in src_l for k in ("facebook", "meta", "instagram")):
            platform = "meta"

        if max_seen is None or created_at > max_seen:
            max_seen = created_at

        with db_session() as s:
            # Crea tabla si falta (por si alguien llama directo a este sync)
            try:
                s.execute(_t("""
                    CREATE TABLE IF NOT EXISTS leads_kajabi (
                      id SERIAL PRIMARY KEY,
                      created_at TIMESTAMP NOT NULL,
                      email VARCHAR(320),
                      utm_source VARCHAR(100),
                      utm_medium VARCHAR(100),
                      utm_campaign VARCHAR(200),
                      utm_content VARCHAR(200),
                      gclid VARCHAR(255),
                      fbclid VARCHAR(255),
                      platform VARCHAR(20),
                      campaign_id VARCHAR(64),
                      adset_id VARCHAR(64),
                      ad_id VARCHAR(64)
                    )
                """))
            except Exception:
                pass
            # Evita duplicados email+created_at si hay email
            try:
                params = {
                    "created_at": created_at,
                    "email": email,
                }
                if email:
                    exists = s.execute(_t("SELECT 1 FROM leads_kajabi WHERE created_at=:created_at AND email=:email LIMIT 1"), params).scalar()
                else:
                    exists = s.execute(_t("SELECT 1 FROM leads_kajabi WHERE created_at=:created_at LIMIT 1"), params).scalar()
            except Exception:
                exists = None
            if not exists:
                s.execute(_t("""
                    INSERT INTO leads_kajabi (
                      created_at, email, utm_source, utm_medium, utm_campaign, utm_content, gclid, fbclid, platform, campaign_id, adset_id, ad_id
                    ) VALUES (
                      :created_at, :email, :utm_source, :utm_medium, :utm_campaign, :utm_content, :gclid, :fbclid, :platform, :campaign_id, :adset_id, :ad_id
                    )
                """), {
                    "created_at": created_at,
                    "email": email,
                    "utm_source": utm_source,
                    "utm_medium": utm_medium,
                    "utm_campaign": utm_campaign,
                    "utm_content": utm_content,
                    "gclid": gclid,
                    "fbclid": fbclid,
                    "platform": platform,
                    "campaign_id": None,
                    "adset_id": None,
                    "ad_id": None,
                })
                inserted += 1
            else:
                # Actualización ligera de campos no nulos
                s.execute(_t("""
                    UPDATE leads_kajabi SET
                      utm_source = COALESCE(:utm_source, utm_source),
                      utm_medium = COALESCE(:utm_medium, utm_medium),
                      utm_campaign = COALESCE(:utm_campaign, utm_campaign),
                      utm_content = COALESCE(:utm_content, utm_content),
                      gclid = COALESCE(:gclid, gclid),
                      fbclid = COALESCE(:fbclid, fbclid),
                      platform = COALESCE(:platform, platform)
                    WHERE created_at=:created_at AND (email=:email OR :email IS NULL)
                """), {
                    "utm_source": utm_source,
                    "utm_medium": utm_medium,
                    "utm_campaign": utm_campaign,
                    "utm_content": utm_content,
                    "gclid": gclid,
                    "fbclid": fbclid,
                    "platform": platform,
                    "created_at": created_at,
                    "email": email,
                })
                updated += 1

    if max_seen:
        _set_state(cursor_key, max_seen.isoformat())

    return {"detected": detected, "inserted": inserted, "updated": updated}


