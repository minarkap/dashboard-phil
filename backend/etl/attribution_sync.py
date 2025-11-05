from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Dict, Any, Iterable, Optional

from backend.db.config import db_session
from backend.db.models import (
    Payment,
    AttributionEvent,
    AttributionLink,
)
from . import ga4_client
try:
    from . import meta_client
except Exception:
    meta_client = None


def _upsert_events(events: Iterable[Dict[str, Any]]) -> int:
    inserted = 0
    seen: set[tuple] = set()
    with db_session() as s:
        for ev in events:
            key = (
                ev.get("platform"),
                ev.get("event_name"),
                ev.get("event_time"),
                ev.get("transaction_id") or ev.get("campaign"),
            )
            if key in seen:
                continue
            seen.add(key)

            s.add(AttributionEvent(
                platform=ev.get("platform"),
                event_name=ev.get("event_name"),
                event_time=ev.get("event_time"),
                source=ev.get("source"),
                medium=ev.get("medium"),
                campaign=ev.get("campaign"),
                term=ev.get("term"),
                content=ev.get("content"),
                gclid=ev.get("gclid"),
                fbclid=ev.get("fbclid"),
                transaction_id=ev.get("transaction_id"),
                email=ev.get("email"),
                value=ev.get("value"),
                currency=ev.get("currency"),
                raw=ev.get("raw"),
            ))
            inserted += 1
    return inserted


def sync_ga4_purchase_events(days_back: int = 30) -> int:
    end = date.today()
    start = end - timedelta(days=days_back)
    rows = list(ga4_client.fetch_purchases_by_day_tx(start, end))
    events: list[Dict[str, Any]] = []
    for r in rows:
        d: date = r.get("date")
        events.append({
            "platform": "ga4",
            "event_name": "purchase",
            "event_time": datetime(d.year, d.month, d.day),
            "source": r.get("source"),
            "medium": r.get("medium"),
            "campaign": r.get("campaign"),
            "transaction_id": r.get("transaction_id"),
            "value": None,
            "currency": None,
            "raw": r,
        })
    return _upsert_events(events)


def sync_meta_purchase_events(days_back: int = 30) -> int:
    if meta_client is None:
        return 0
    end = date.today()
    start = end - timedelta(days=days_back)
    rows = list(meta_client.fetch_purchases_daily(start, end))
    events: list[Dict[str, Any]] = []
    for r in rows:
        d: date = r.get("date")
        events.append({
            "platform": "meta",
            "event_name": "purchase",
            "event_time": datetime(d.year, d.month, d.day),
            "source": "meta",
            "medium": "paid",
            "campaign": r.get("campaign_name"),
            "value": r.get("value"),
            "currency": r.get("currency"),
            "raw": r,
        })
    return _upsert_events(events)


def build_attribution_links(lookback_days: int = 30) -> int:
    """Crea AttributionLink por last-touch:
    1) Intento exacto por transaction_id (GA4) -> payment.source_payment_id
    2) Si falla, último evento (GA4/Meta) antes de paid_at dentro del lookback.
    """
    created = 0
    cutoff = datetime.utcnow() - timedelta(days=lookback_days)
    with db_session() as s:
        payments = (
            s.query(Payment)
            .outerjoin(AttributionLink, AttributionLink.payment_id == Payment.id)
            .filter(AttributionLink.id == None)  # noqa: E711
            .filter(Payment.paid_at != None)  # noqa: E711
            .filter(Payment.paid_at >= cutoff)
            .all()
        )
        for pay in payments:
            linked = False
            txid = (pay.source_payment_id or "").strip()
            if txid:
                ev = (
                    s.query(AttributionEvent)
                    .filter(AttributionEvent.platform == "ga4")
                    .filter(AttributionEvent.transaction_id == txid)
                    .order_by(AttributionEvent.event_time.desc())
                    .first()
                )
                if ev:
                    s.add(AttributionLink(
                        payment_id=pay.id,
                        source=ev.source,
                        medium=ev.medium,
                        campaign=ev.campaign,
                        term=ev.term,
                        content=ev.content,
                        gclid=ev.gclid,
                        fbclid=ev.fbclid,
                        weight=1.0,
                    ))
                    created += 1
                    linked = True
            if linked:
                continue

            # Fallback: último evento antes de paid_at
            ev2 = (
                s.query(AttributionEvent)
                .filter(AttributionEvent.event_time <= (pay.paid_at or datetime.utcnow()))
                .filter(AttributionEvent.event_time >= cutoff)
                .order_by(AttributionEvent.event_time.desc())
                .first()
            )
            if ev2:
                s.add(AttributionLink(
                    payment_id=pay.id,
                    source=ev2.source,
                    medium=ev2.medium,
                    campaign=ev2.campaign,
                    term=ev2.term,
                    content=ev2.content,
                    gclid=ev2.gclid,
                    fbclid=ev2.fbclid,
                    weight=1.0,
                ))
                created += 1

    return created


def run_attribution_sync(days_back: int = 30) -> Dict[str, int]:
    r1 = sync_ga4_purchase_events(days_back)
    r2 = sync_meta_purchase_events(days_back)
    r3 = build_attribution_links(lookback_days=max(7, days_back))
    return {
        "ga4_events": r1,
        "meta_events": r2,
        "links_created": r3,
    }



