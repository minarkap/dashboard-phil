from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text
from datetime import date, timedelta

from backend.db.config import engine


def render_subs_tab():
    st.subheader("Suscripciones")
    try:
        with engine.begin() as conn:
            by_status = pd.read_sql(
                text(
                    """
                    SELECT COALESCE(LOWER(status),'unknown') status, COUNT(*) n
                    FROM subscriptions
                    GROUP BY 1 ORDER BY 2 DESC
                    """
                ),
                conn,
            )
    except Exception as e:
        st.warning(f"No se pudo cargar distribución de estados de suscripción: {e}")
        by_status = pd.DataFrame()
    if not by_status.empty:
        pies = px.pie(by_status, names="status", values="n", title="Distribución de estados de suscripción")
        st.plotly_chart(pies, use_container_width=True)
        
    # KPIs de suscripciones básicas
    subsq = text(
        """
        SELECT
            (SELECT COUNT(*) FROM subscriptions s
             WHERE (s.canceled_on IS NULL OR s.canceled_on::date > :end)
               AND s.created_at::date <= :end) AS activas,
            (SELECT COUNT(*) FROM subscriptions s WHERE s.created_at::date BETWEEN :start AND :end) AS altas,
            (SELECT COUNT(*) FROM subscriptions s WHERE s.canceled_on IS NOT NULL AND s.canceled_on::date BETWEEN :start AND :end) AS bajas
        """
    )
    # Valores por defecto por si no están en el contexto externo
    try:
        _today = date.today()
        _start = _today - timedelta(days=29)
        _end = _today
    except Exception:
        _start = None
        _end = None
    try:
        with engine.begin() as conn:
            subs = conn.execute(subsq, {"start": _start, "end": _end}).mappings().first()
    except Exception as e:
        st.warning(f"No se pudieron cargar KPIs de suscripciones: {e}")
        subs = {"activas": 0, "altas": 0, "bajas": 0}
    s1, s2, s3 = st.columns(3)
    s1.metric("Subs activas", f"{subs['activas']:,}")
    s2.metric("Altas (periodo)", f"{subs['altas']:,}")
    s3.metric("Bajas (periodo)", f"{subs['bajas']:,}")

    # KPIs avanzados: facturación one-shot (realizada) y MRR prorrateado
    st.subheader("KPIs financieros: One‑shot vs Recurrencia")
    today_ref = date.today()
    week_start = today_ref - timedelta(days=6)
    month_start = today_ref - timedelta(days=29)

    def _one_shot_sum_eur(_from: date, _to: date) -> float:
        with engine.begin() as conn:
            df_pay = pd.read_sql(
                text(
                    """
                    SELECT
                        p.paid_at::date AS day,
                        UPPER(p.currency_original) AS currency,
                        COALESCE(pr.name, '') AS prod_name,
                        COALESCE(p.net_eur, p.amount_eur,
                                 CASE WHEN UPPER(p.currency_original)='EUR' THEN p.amount_original_minor/100.0 ELSE NULL END) AS eur_direct,
                        CASE WHEN COALESCE(p.net_eur, p.amount_eur) IS NULL AND UPPER(p.currency_original)!='EUR'
                             THEN p.amount_original_minor/100.0 ELSE 0 END AS orig_major
                    FROM payments p
                    LEFT JOIN orders o ON o.id = p.order_id
                    LEFT JOIN order_items oi ON oi.order_id = o.id
                    LEFT JOIN products pr ON pr.id = oi.product_id
                    WHERE p.paid_at IS NOT NULL
                      AND p.paid_at::date BETWEEN :from AND :to
                    """
                ),
                conn,
                params={"from": _from, "to": _to},
            )

        if df_pay.empty:
            return 0.0

        mask_sub = (
            df_pay["prod_name"].str.contains("membre", case=False, na=False)
            | df_pay["prod_name"].str.contains("subscrip", case=False, na=False)
            | df_pay["prod_name"].str.contains("mensual", case=False, na=False)
            | df_pay["prod_name"].str.contains("trimestral", case=False, na=False)
            | df_pay["prod_name"].str.contains("semestral", case=False, na=False)
            | df_pay["prod_name"].str.contains("anual", case=False, na=False)
        )
        df_pay = df_pay[~mask_sub].copy()

        if df_pay.empty:
            return 0.0

        df_pay["day_date"] = pd.to_datetime(df_pay["day"]).dt.date
        df_pay["cur_uc"] = df_pay["currency"].fillna("EUR").str.upper()
        df_pay["eur_direct"] = pd.to_numeric(df_pay["eur_direct"], errors="coerce").fillna(0.0)
        df_pay["orig_major"] = pd.to_numeric(df_pay["orig_major"], errors="coerce").fillna(0.0)

        return float(df_pay["eur_direct"].sum() + df_pay["orig_major"].sum())

    week_one_shot = _one_shot_sum_eur(week_start, today_ref)
    month_one_shot = _one_shot_sum_eur(month_start, today_ref)

    with engine.begin() as conn:
        rows = pd.read_sql(
            text(
                """
                SELECT s.interval, s.amount_original_minor, s.currency_original
                FROM subscriptions s
                WHERE (s.canceled_on IS NULL OR s.canceled_on::date > :ref)
                  AND s.created_at::date <= :ref
                """
            ),
            conn,
            params={"ref": today_ref},
        )
    if rows.empty:
        mrr_df = pd.DataFrame(columns=["intervalo","subs","mrr_eur"])  # empty
    else:
        def _label_interval(v: str) -> str:
            vv = (v or "").lower()
            if vv in ("month","monthly"): return "mensual"
            if vv in ("quarter","quarterly","trimestral"): return "trimestral"
            if vv in ("semester","semiannual","semiannually","semestral"): return "semestral"
            if vv in ("year","annual","yearly"): return "anual"
            if vv in ("week","weekly"): return "semanal"
            return "otro"

        rows["intervalo"] = rows["interval"].map(_label_interval)
        rows["amount_major"] = rows.apply(lambda r: float(r["amount_original_minor"] or 0)/100.0, axis=1)
        rows["mrr_eur"] = rows["amount_major"].apply(lambda x: x)  # simplificado
        mrr_df = rows.groupby("intervalo", as_index=False).agg(subs=("intervalo","count"), mrr_eur=("mrr_eur","sum"))

    cA, cB, cC = st.columns(3)
    cA.metric("One‑shot última semana", f"€{float(week_one_shot or 0):,.2f}")
    cB.metric("One‑shot último mes", f"€{float(month_one_shot or 0):,.2f}")
    cC.metric("MRR total (EUR)", f"€{float(mrr_df['mrr_eur'].sum() if not mrr_df.empty else 0):,.2f}")






