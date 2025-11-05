from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable, Dict, Any, Optional

from backend.db.config import db_session
from backend.db.models import AdCostsDaily, AdCampaign, AdAdset, AdAd, SyncState
from . import google_ads_client as gads
try:
    from . import meta_client as meta
except Exception:
    meta = None


def _upsert_ad_cost_rows(rows: Iterable[Dict[str, Any]]) -> int:
    """Inserta o actualiza por clave natural (date+dimensiones) para idempotencia.
    Devuelve el número de filas afectadas (insertadas o actualizadas).
    """
    count = 0
    with db_session() as s:
        touched_campaigns: set[tuple[str, str]] = set()
        touched_adsets: set[tuple[str, str]] = set()
        touched_ads: set[tuple[str, str]] = set()
        for r in rows:
            # Upsert catálogos si vienen nombres
            if r.get("campaign_id") and r.get("campaign_name"):
                key = (r.get("platform"), r.get("campaign_id"))
                if key not in touched_campaigns:
                    camp = s.query(AdCampaign).filter_by(
                        platform=key[0],
                        campaign_id=key[1],
                    ).one_or_none()
                    if camp:
                        camp.name = r.get("campaign_name") or camp.name
                        camp.account_id = r.get("account_id") or camp.account_id
                    else:
                        s.add(AdCampaign(
                            platform=key[0],
                            account_id=r.get("account_id"),
                            campaign_id=key[1],
                            name=r.get("campaign_name"),
                        ))
                    touched_campaigns.add(key)

            if r.get("adset_id") and r.get("adset_id") != "None" and r.get("adset_id") is not None:
                key_as = (r.get("platform"), r.get("adset_id"))
                if key_as not in touched_adsets:
                    aset = s.query(AdAdset).filter_by(platform=key_as[0], adset_id=key_as[1]).one_or_none()
                    if aset:
                        if r.get("account_id"):
                            aset.account_id = r.get("account_id")
                        if r.get("adset_name"):
                            aset.name = r.get("adset_name") or aset.name
                    else:
                        s.add(AdAdset(platform=key_as[0], adset_id=key_as[1], account_id=r.get("account_id"), name=r.get("adset_name")))
                    touched_adsets.add(key_as)

            if r.get("ad_id") and r.get("ad_id") != "None" and r.get("ad_id") is not None:
                key_ad = (r.get("platform"), r.get("ad_id"))
                if key_ad not in touched_ads:
                    ad = s.query(AdAd).filter_by(platform=key_ad[0], ad_id=key_ad[1]).one_or_none()
                    if ad:
                        if r.get("account_id"):
                            ad.account_id = r.get("account_id")
                        if r.get("ad_name"):
                            ad.name = r.get("ad_name") or ad.name
                    else:
                        s.add(AdAd(platform=key_ad[0], ad_id=key_ad[1], account_id=r.get("account_id"), name=r.get("ad_name")))
                    touched_ads.add(key_ad)
            existing = s.query(AdCostsDaily).filter_by(
                date=r.get("date"),
                platform=r.get("platform"),
                account_id=r.get("account_id"),
                campaign_id=r.get("campaign_id"),
                adset_id=r.get("adset_id"),
                ad_id=r.get("ad_id"),
            ).one_or_none()
            if existing:
                existing.currency = r.get("currency")
                existing.cost_major = r.get("cost_major")
                existing.impressions = r.get("impressions")
                existing.clicks = r.get("clicks")
            else:
                s.add(AdCostsDaily(
                    date=r.get("date"),
                    platform=r.get("platform"),
                    account_id=r.get("account_id"),
                    campaign_id=r.get("campaign_id"),
                    adset_id=r.get("adset_id"),
                    ad_id=r.get("ad_id"),
                    currency=r.get("currency"),
                    cost_major=r.get("cost_major"),
                    impressions=r.get("impressions"),
                    clicks=r.get("clicks"),
                ))
            count += 1
    return count


def _get_state(key: str) -> Optional[str]:
    # Tolerante a duplicados: usa el registro más reciente
    with db_session() as s:
        st = (
            s.query(SyncState)
            .filter(SyncState.key == key)
            .order_by(SyncState.id.desc())
            .first()
        )
        return st.value if st else None


def _set_state(key: str, value: str) -> None:
    # Upsert tolerante a duplicados
    with db_session() as s:
        st = (
            s.query(SyncState)
            .filter(SyncState.key == key)
            .order_by(SyncState.id.desc())
            .first()
        )
        if st:
            st.value = value
        else:
            from datetime import datetime as _dt
            s.add(SyncState(key=key, value=value))


def run_ads_sync(
    days_back: int = 30,
    include_meta: bool = True,
    include_google: bool = True,
) -> int:
    end = date.today()
    # Incremental con cursor (lookback 1 día por idempotencia)
    cursor_key = "ads_costs_cursor"
    state = _get_state(cursor_key)
    if state:
        try:
            from datetime import datetime as _dt
            last = _dt.fromisoformat(state).date()
            start = max(date(1970, 1, 1), last - timedelta(days=1))
        except Exception:
            start = end - timedelta(days=days_back)
    else:
        start = end - timedelta(days=days_back)
    inserted = run_ads_sync_range(start, end, include_meta=include_meta, include_google=include_google)
    _set_state(cursor_key, end.isoformat())
    return inserted


def run_ads_sync_range(
    start: date,
    end: date,
    include_meta: bool = True,
    include_google: bool = True,
) -> int:
    rows: list[Dict[str, Any]] = []
    if include_google:
        try:
            rows += list(gads.fetch_costs_daily(start, end))
        except Exception:
            pass
    if include_meta and meta is not None:
        try:
            rows += list(meta.fetch_costs_daily(start, end))
        except Exception:
            pass
    return _upsert_ad_cost_rows(rows)


def run_ads_backfill(
    total_days: int = 365,
    chunk_days: int = 30,
    include_meta: bool = False,
    include_google: bool = True,
) -> int:
    """Backfill histórico en ventanas no solapadas hasta cubrir total_days."""
    end = date.today()
    start_total = end - timedelta(days=total_days)
    inserted = 0
    current_end = end
    while current_end > start_total:
        current_start = max(start_total, current_end - timedelta(days=chunk_days))
        inserted += run_ads_sync_range(
            current_start,
            current_end,
            include_meta=include_meta,
            include_google=include_google,
        )
        current_end = current_start
    return inserted


