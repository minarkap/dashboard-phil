from __future__ import annotations

from datetime import date, timedelta
import pandas as pd
import plotly.express as px
import streamlit as st

from .utils import build_color_map


def render_products_tab(
    df_base: pd.DataFrame,
    start: date,
    end: date,
    grain_effective: str,
):
    st.subheader("Análisis por producto")
    series_list = sorted(df_base.get("series", pd.Series(dtype=str)).dropna().unique().tolist()) if not df_base.empty else []
    prod_name = st.selectbox(
        "Selecciona producto",
        options=["(Todos)"] + series_list,
        index=0,
        key="prod_tab_select",
    )
    prod_val = None if prod_name == "(Todos)" else prod_name

    if prod_val:
        dfp_base = df_base[df_base['series'] == prod_val].copy()
    else:
        dfp_base = df_base.copy()

    if dfp_base.empty:
        st.info("Sin datos para el rango/filtrado.")
        return

    if dfp_base.empty:
        st.info("Sin datos para el rango/filtrado.")
        return
    dfp_agg = (
        dfp_base.groupby(["day_agg", "series"], as_index=False)
        .agg(gross_amount_eur=("gross_amount_eur", "sum"), num_payments=("gross_amount_eur", "count"))
    )

    dfp_agg = dfp_agg.rename(columns={"day_agg": "day"})
    freq_alias = {"Día": "D", "Semana": "W-MON", "Mes": "M"}.get(grain_effective, "D")
    start_aligned, end_aligned = start, end
    try:
        if grain_effective == "Semana":
            start_aligned = start - timedelta(days=start.weekday())
        elif grain_effective == "Mes":
            start_aligned = start.replace(day=1)
    except Exception:
        pass
    periods = pd.period_range(start=start_aligned, end=end_aligned, freq=freq_alias)
    all_days = periods.to_timestamp(how="start").normalize()

    outp = []
    for series in dfp_agg["series"].unique():
        sub = dfp_agg[dfp_agg["series"] == series]
        sub = sub.set_index("day").reindex(all_days, fill_value=0)
        sub["series"] = series
        outp.append(sub.reset_index().rename(columns={"index": "day"}))
    dfp_filled = pd.concat(outp, ignore_index=True) if outp else pd.DataFrame()

    color_map = build_color_map(dfp_filled["series"].dropna().tolist())
    figp = px.bar(
        dfp_filled,
        x="day",
        y="gross_amount_eur",
        color="series",
        title=f"{prod_name} - ingresos (grano: {grain_effective})",
        barmode="stack",
        color_discrete_map=color_map,
    )
    st.plotly_chart(figp, use_container_width=True)

    totp = float(dfp_filled["gross_amount_eur"].sum())
    cntp = int(dfp_filled["num_payments"].sum())
    aovp = (totp / cntp) if cntp > 0 else 0.0
    c1, c2, c3 = st.columns(3)
    c1.metric("Ingresos (EUR)", f"€{totp:,.2f}")
    c2.metric("Pagos", f"{cntp:,}")
    c3.metric("Ticket medio (EUR)", f"€{aovp:,.2f}")



