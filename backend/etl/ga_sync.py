from __future__ import annotations

from datetime import date, timedelta, datetime
from typing import Iterable, Dict, Any, Optional

from backend.db.config import db_session
from backend.db.models import GaSessionsDaily, SyncState
from . import ga4_client


def _get_state(key: str) -> Optional[str]:
    # Evita excepciones si existen duplicados legacy en sync_state
    with db_session() as s:
        st = (
            s.query(SyncState)
            .filter(SyncState.key == key)
            .order_by(SyncState.id.desc())
            .first()
        )
        return st.value if st else None


def _set_state(key: str, value: str) -> None:
    # Upsert tolerante a duplicados preexistentes
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
            s.add(SyncState(key=key, value=value))


def _dedupe_state_key(key: str) -> None:
    """Elimina duplicados de SyncState conservando el más reciente.
    Previene errores de MultipleResultsFound en entornos legacy.
    """
    with db_session() as s:
        rows = (
            s.query(SyncState)
            .filter(SyncState.key == key)
            .order_by(SyncState.id.desc())
            .all()
        )
        if len(rows) > 1:
            for r in rows[1:]:
                s.delete(r)


def _upsert_rows(rows: Iterable[Dict[str, Any]], insert_only: bool = True) -> int:
    count = 0
    with db_session() as s:
        for r in rows:
            existing = (
                s.query(GaSessionsDaily)
                .filter_by(
                    date=r.get("date"),
                    source=r.get("source"),
                    medium=r.get("medium"),
                    campaign=r.get("campaign"),
                )
                .order_by(GaSessionsDaily.id.asc())
                .first()
            )
            if existing:
                if not insert_only:
                    existing.sessions = r.get("sessions")
                    existing.users = r.get("users")
                    existing.conversions = r.get("conversions")
            else:
                s.add(GaSessionsDaily(
                    date=r.get("date"),
                    source=r.get("source"),
                    medium=r.get("medium"),
                    campaign=r.get("campaign"),
                    sessions=r.get("sessions"),
                    users=r.get("users"),
                    conversions=r.get("conversions"),
                ))
            count += 1
    return count


def run_ga_sync(days_back: int = 30, insert_only: bool = True) -> int:
    end = date.today()
    # incremental con cursor
    cursor_key = "ga_sessions_cursor"
    _dedupe_state_key(cursor_key)
    cursor = _get_state(cursor_key)
    start: date
    if cursor:
        try:
            last = datetime.fromisoformat(cursor).date()
            start = max(date(1970, 1, 1), last - timedelta(days=1))  # lookback 1 día
        except Exception:
            start = end - timedelta(days=days_back)
    else:
        start = end - timedelta(days=days_back)
    rows = list(ga4_client.fetch_sessions_daily(start, end))
    inserted = _upsert_rows(rows, insert_only=insert_only)
    _set_state(cursor_key, end.isoformat())
    return inserted


def run_ga_sync_range(start: date, end: date, insert_only: bool = True) -> int:
    _dedupe_state_key("ga_sessions_cursor")
    rows = list(ga4_client.fetch_sessions_daily(start, end))
    inserted = _upsert_rows(rows, insert_only=insert_only)
    _set_state("ga_sessions_cursor", end.isoformat())
    return inserted


def run_ga_backfill(total_days: int = 3650, chunk_days: int = 30, insert_only: bool = True) -> int:
    """Backfill histórico en ventanas hasta cubrir total_days (por defecto ~10 años)."""
    end = date.today()
    start_total = end - timedelta(days=total_days)
    inserted = 0
    current_end = end
    while current_end > start_total:
        current_start = max(start_total, current_end - timedelta(days=chunk_days))
        inserted += run_ga_sync_range(current_start, current_end, insert_only=insert_only)
        current_end = current_start
    return inserted




