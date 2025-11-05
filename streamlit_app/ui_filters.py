from __future__ import annotations

from datetime import date, timedelta
import pandas as pd
import streamlit as st
from sqlalchemy import text

from backend.db.config import engine


def render_filters(default_start: date, default_end: date, key_prefix: str = ""):
    col1, col2, col3, col4 = st.columns([1, 1, 1, 2])
    with col1:
        preset = st.selectbox(
            "Rango rápido",
            [
                "Últimos 7 días",
                "Últimos 30 días",
                "Este mes",
                "Últimos 90 días",
                "Últimos 6 meses",
                "Último año",
                "Últimos 3 años",
                "Todo",
                "Custom",
            ],
            index=1,
            key=f"{key_prefix}preset",
        )
    with col2:
        start = st.date_input("Desde", value=default_start, key=f"{key_prefix}start")
    with col3:
        end = st.date_input("Hasta", value=default_end, key=f"{key_prefix}end")
    with col4:
        try:
            prod_options = ["(Todos)"] + sorted(
                pd.read_sql(
                    text("SELECT DISTINCT name FROM products WHERE name IS NOT NULL ORDER BY 1"),
                    engine,
                ).iloc[:, 0].tolist()
            )
        except Exception as e:
            st.warning(f"No se pudieron cargar productos desde la base de datos: {e}")
            prod_options = ["(Todos)"]
        product_filter = st.selectbox(
            "Producto",
            options=prod_options,
            index=0,
            key=f"{key_prefix}product",
        )

    if preset != "Custom":
        today = date.today()
        if preset == "Últimos 7 días":
            start, end = today - timedelta(days=6), today
        elif preset == "Últimos 30 días":
            start, end = today - timedelta(days=29), today
        elif preset == "Este mes":
            start, end = today.replace(day=1), today
        elif preset == "Últimos 90 días":
            start, end = today - timedelta(days=89), today
        elif preset == "Últimos 6 meses":
            start, end = today - timedelta(days=182), today
        elif preset == "Último año":
            start, end = today - timedelta(days=365), today
        elif preset == "Últimos 3 años":
            start, end = today - timedelta(days=365 * 3), today
        elif preset == "Todo":
            start, end = default_start, default_end

    product_filter_val = None if product_filter == "(Todos)" else product_filter

    # Filtros avanzados
    colA, colB, colC, colD = st.columns([1, 1, 1, 1])
    with colA:
        source_filter = st.selectbox("Fuente", ["(Todas)", "stripe", "hotmart", "kajabi"], index=0, key=f"{key_prefix}source")
        source_filter_val = None if source_filter == "(Todas)" else source_filter
    with colB:
        grain = st.selectbox("Grano", ["Auto", "Día", "Semana", "Mes"], index=0, key=f"{key_prefix}grain")
    with colC:
        group_by = st.selectbox("Agrupar por", ["Producto", "Categoría"], index=0, key=f"{key_prefix}group_by")
    group_by_category = group_by == "Categoría"

    with colD:
        status_opt = st.selectbox(
            "Estatus de Pagos",
            [
                "Completas + Aprobadas",
                "Solo Completas",
                "Todos (excepto canceladas/reembolsadas)",
                "Todos (incluyendo pendientes)"
            ],
            index=0,
            help="Filtra pagos por estado. Aplica a todas las fuentes (Stripe, Kajabi, Hotmart).",
            key=f"{key_prefix}status",
        )

    view_mode = st.radio("Moneda", options=["EUR (convertido)", "Moneda original"], horizontal=True, index=0, key=f"{key_prefix}view_mode")

    # Grano efectivo si es Auto
    grain_effective = grain
    if grain == "Auto":
        try:
            delta_days = (end - start).days
            if delta_days <= 30:
                grain_effective = "Día"
            elif delta_days <= 365:
                grain_effective = "Semana"
            else:
                grain_effective = "Mes"
        except Exception:
            grain_effective = "Día"

    return (
        start,
        end,
        product_filter_val,
        source_filter_val,
        grain_effective,
        group_by_category,
        view_mode,
        status_opt,
    )




