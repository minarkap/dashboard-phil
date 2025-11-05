from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
from sqlalchemy import text

from backend.db.config import engine


def parse_amount_robust(s: str) -> float:
    val = (str(s or "").replace("€", "").replace("$", "").strip())
    if "," in val and "." in val:
        if val.rfind(",") > val.rfind("."):
            val = val.replace(".", "").replace(",", ".")
        else:
            val = val.replace(",", "")
    else:
        val = val.replace(",", ".")
    try:
        return float(val)
    except Exception:
        return 0.0


def detect_kajabi_tx_count_from_bytes(file_bytes: bytes) -> int:
    import io
    try:
        df = pd.read_csv(io.BytesIO(file_bytes), dtype=str, keep_default_na=False)
        if df.empty:
            return 0
        id_col = None
        for c in df.columns:
            n = (c or "").strip().lower()
            if n in ("id", "order no."):
                id_col = c
                break
        if id_col is None:
            return len(df)
        return int((df[id_col].astype(str).str.strip() != "").sum())
    except Exception:
        try:
            text_s = file_bytes.decode("utf-8", errors="ignore")
            lines = [l for l in text_s.splitlines() if l.strip()]
            return max(0, len(lines) - 1)
        except Exception:
            return 0


def detect_kajabi_subs_count_from_bytes(file_bytes: bytes) -> int:
    import io
    try:
        df = pd.read_csv(io.BytesIO(file_bytes), dtype=str, keep_default_na=False)
        if df.empty:
            return 0
        target = None
        for c in df.columns:
            if (c or "").strip().lower() == "kajabi subscription id":
                target = c
                break
        if target is None:
            return len(df)
        return int((df[target].astype(str).str.strip() != "").sum())
    except Exception:
        try:
            text_s = file_bytes.decode("utf-8", errors="ignore")
            lines = [l for l in text_s.splitlines() if l.strip()]
            return max(0, len(lines) - 1)
        except Exception:
            return 0


def detect_hotmart_count_from_bytes(file_bytes: bytes) -> int:
    import io, unicodedata
    try:
        df = pd.read_csv(io.BytesIO(file_bytes), dtype=str, keep_default_na=False, sep=None, engine="python")
        if df.empty:
            return 0
        def norm(s: str) -> str:
            s2 = unicodedata.normalize("NFD", s or "").encode("ascii", "ignore").decode("ascii")
            return " ".join(s2.strip().lower().split())
        tx_col = None
        for c in df.columns:
            n = norm(c)
            if n.startswith("transaccion") or n == "transaccion":
                tx_col = c
                break
        if tx_col is None:
            return len(df)
        return int((df[tx_col].astype(str).str.strip() != "").sum())
    except Exception:
        try:
            text_s = file_bytes.decode("utf-8", errors="ignore")
            lines = [l for l in text_s.splitlines() if l.strip()]
            return max(0, len(lines) - 1)
        except Exception:
            return 0


def extract_hotmart_tx_ids(file_bytes: bytes) -> list[str]:
    import io, unicodedata
    try:
        df = pd.read_csv(io.BytesIO(file_bytes), dtype=str, keep_default_na=False, sep=";")
        if df.empty:
            return []
        def norm(s: str) -> str:
            s2 = unicodedata.normalize("NFD", s or "").encode("ascii", "ignore").decode("ascii")
            return " ".join(s2.strip().lower().split())
        tx_col = None
        for c in df.columns:
            n = norm(c)
            if n.startswith("codigo de la transaccion") or n.startswith("transaccion"):
                tx_col = c
                break
        if tx_col is None:
            return []
        return [str(v).strip() for v in df[tx_col].tolist() if str(v).strip()]
    except Exception:
        return []


def count_existing_hotmart_transactions(ids: list[str]) -> int:
    if not ids:
        return 0
    sql = text("SELECT 1 FROM payments WHERE source='hotmart' AND source_payment_id=:sid LIMIT 1")
    existing = 0
    with engine.begin() as conn:
        for sid in set(ids):
            row = conn.execute(sql, {"sid": sid}).fetchone()
            if row is not None:
                existing += 1
    return existing


def extract_kajabi_sub_ids(file_bytes: bytes) -> list[str]:
    import io
    try:
        df = pd.read_csv(io.BytesIO(file_bytes), dtype=str, keep_default_na=False)
        if df.empty:
            return []
        col = None
        for c in df.columns:
            if (c or "").strip().lower() == "kajabi subscription id":
                col = c
                break
        if col is None:
            return []
        return [str(v).strip() for v in df[col].tolist() if str(v).strip()]
    except Exception:
        return []


def count_existing_kajabi_subscriptions(ids: list[str]) -> int:
    if not ids:
        return 0
    sql = text("SELECT 1 FROM subscriptions WHERE source='kajabi' AND source_id=:sid LIMIT 1")
    existing = 0
    with engine.begin() as conn:
        for sid in set(ids):
            row = conn.execute(sql, {"sid": sid}).fetchone()
            if row is not None:
                existing += 1
    return existing


def inline_import_kajabi_transactions(file_bytes: bytes) -> int:
    import io
    df = pd.read_csv(io.BytesIO(file_bytes), dtype=str, keep_default_na=False)
    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        tx_id = str(row.get("ID") or row.get("Order No.") or "").strip()
        if not tx_id:
            continue
        amount_major = parse_amount_robust(row.get("Amount"))
        currency = (row.get("Currency") or "").strip().upper() or "EUR"
        created_at_raw = row.get("Created At") or row.get("created_at") or row.get("Created") or ""
        try:
            created_at = pd.to_datetime(created_at_raw).to_pydatetime()
        except Exception:
            created_at = None
        status = (row.get("Status") or row.get("Type") or "").strip().lower()
        if not status:
            status = "completed" if (amount_major or 0.0) >= 0 else "refunded"
        amount_minor = int(round((amount_major or 0.0) * 100))
        offer_id = (row.get("Offer ID") or "").strip()
        offer_title = (row.get("Offer Title") or "").strip()
        raw = {"offer_id": offer_id, "offer_title": offer_title, "type": row.get("Type"), "status": row.get("Status")}
        rows.append({
            "tx": tx_id,
            "status": status,
            "minor": amount_minor,
            "cur": currency,
            "paid_at": created_at,
            "raw": raw,
        })

    if not rows:
        st.info("No hay filas válidas en el CSV.")
        return 0

    sql = text(
        """
        INSERT INTO payments (order_id, source, source_payment_id, status,
                              amount_original_minor, currency_original,
                              amount_eur, net_eur, paid_at, raw)
        VALUES (
            NULL,
            'kajabi',
            :tx,
            COALESCE(:status, 'completed'),
            COALESCE(:minor, 0),
            COALESCE(:cur, 'EUR'),
            NULL,
            NULL,
            :paid_at,
            CAST(:raw AS JSONB)
        )
        ON CONFLICT (source, source_payment_id) DO NOTHING
        """
    )
    inserted_or_updated = 0
    batch = 1000
    with engine.begin() as conn:
        for i in range(0, len(rows), batch):
            part = rows[i:i+batch]
            # Serializa raw a JSON por fila
            part2 = []
            import json as _json
            for r in part:
                r2 = dict(r)
                try:
                    r2["raw"] = _json.dumps(r2.get("raw") or {})
                except Exception:
                    r2["raw"] = "{}"
                part2.append(r2)
            conn.execute(sql, part2)
            inserted_or_updated += len(part)
    st.success(f"Insertados (nuevos) {inserted_or_updated} pagos de Kajabi. Existentes ignorados.")
    return inserted_or_updated


def inline_import_kajabi_subscriptions(file_bytes: bytes) -> int:
    import io
    df = pd.read_csv(io.BytesIO(file_bytes), dtype=str, keep_default_na=False)
    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        sub_id = (row.get("Kajabi Subscription ID") or "").strip()
        if not sub_id:
            continue
        amount = parse_amount_robust(row.get("Amount"))
        currency = (row.get("Currency") or "").strip().upper() or None
        interval = (row.get("Interval") or "").strip().lower() or None
        status = (row.get("Status") or "").strip().lower() or None
        trial = row.get("Trial Ends On") or ""
        canceled = row.get("Canceled On") or ""
        nextp = row.get("Next Payment Date") or ""
        created = row.get("Created At") or ""
        try:
            import pandas as _pd
            trial_dt = _pd.to_datetime(trial).to_pydatetime() if trial else None
            canceled_dt = _pd.to_datetime(canceled).to_pydatetime() if canceled else None
            next_dt = _pd.to_datetime(nextp).to_pydatetime() if nextp else None
            created_dt = _pd.to_datetime(created).to_pydatetime() if created else None
        except Exception:
            trial_dt = canceled_dt = next_dt = created_dt = None
        rows.append({
            "sid": sub_id,
            "status": status,
            "interval": interval,
            "minor": int(round(amount*100)) if amount is not None else None,
            "cur": currency,
            "trial": trial_dt,
            "canceled": canceled_dt,
            "nextp": next_dt,
            "created": created_dt,
        })

    if not rows:
        st.info("No hay filas válidas de suscripciones en el CSV.")
        return 0

    sql = text(
        """
        INSERT INTO subscriptions (source, source_id, customer_id, status, interval,
                                   amount_original_minor, currency_original,
                                   trial_ends_on, canceled_on, next_payment_date, created_at)
        VALUES ('kajabi', :sid, NULL, :status, :interval, :minor, :cur, :trial, :canceled, :nextp, COALESCE(:created, NOW()))
        ON CONFLICT (source, source_id) DO NOTHING
        """
    )
    inserted_or_updated = 0
    batch = 1000
    with engine.begin() as conn:
        for i in range(0, len(rows), batch):
            part = rows[i:i+batch]
            conn.execute(sql, part)
            inserted_or_updated += len(part)
    st.success(f"Insertadas (nuevas) {inserted_or_updated} suscripciones. Existentes ignoradas.")
    return inserted_or_updated


def import_via_backend_bytes(file_bytes: bytes, importer_callable, suffix: str = ".csv") -> object:
    tmp_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(file_bytes)
            tmp_path = tmp.name
        return importer_callable(Path(tmp_path))
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


