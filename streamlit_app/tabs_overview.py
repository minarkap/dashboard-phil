from __future__ import annotations

from datetime import date, timedelta
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text
from plotly.subplots import make_subplots
import plotly.graph_objects as go

from backend.db.config import engine
from .utils import build_color_map
from .fx import get_fx_timeseries, FALLBACK_FX_RATES
from .data import load_economics_from_sheets, load_ltv_global, load_sales_global_total


def render_overview_tab(
    df_base: pd.DataFrame,
    start: date,
    end: date,
    grain_effective: str,
    view_mode: str,
    source_filter_val: str | None,
):
    # Verificar si hay datos recientes en estado "pending" que no se están mostrando
    try:
        with engine.begin() as conn:
            q_pending = text("""
                SELECT 
                    DATE(paid_at) as fecha,
                    COUNT(*) as count
                FROM payments
                WHERE paid_at >= CURRENT_DATE - INTERVAL '7 days'
                  AND status = 'pending'
                  AND paid_at IS NOT NULL
                GROUP BY DATE(paid_at)
                ORDER BY fecha DESC
                LIMIT 1
            """)
            result = conn.execute(q_pending).fetchone()
            if result:
                pending_date, pending_count = result
                st.info(f"ℹ️ **Nota**: Hay {pending_count} pagos en estado 'pending' del {pending_date} que no se muestran con el filtro actual. Considera cambiar el filtro de estado a 'Todos (excepto canceladas/reembolsadas)' para verlos.")
    except Exception:
        pass
    
    # EBITDA y Margen (Google Sheets)
    st.subheader("EBITDA y Margen (Google Sheets)")
    
    econ = load_economics_from_sheets()
    
    # Verificar si hay errores
    if isinstance(econ, dict) and "_error" in econ:
        error_msg = econ["_error"]
        if "gspread no está instalado" in error_msg or "Módulo gspread" in error_msg:
            st.error(f"**Error de configuración**: {error_msg}")
            st.info("💡 Instala las dependencias con: `pip install gspread google-auth`")
        elif "no configurado" in error_msg:
            st.warning(f"**Falta configuración**: {error_msg}")
            st.info("💡 Configura las variables de entorno `GOOGLE_SHEETS_CREDENTIALS_JSON` y `GOOGLE_SHEETS_ECONOMICS_ID`")
        elif "Hoja no encontrada" in error_msg:
            st.error(f"**Error de acceso**: {error_msg}")
            st.info("💡 Verifica que el ID de la hoja sea correcto y que la cuenta de servicio tenga acceso")
        elif "Faltan columnas" in error_msg:
            st.error(f"**Error de formato**: {error_msg}")
            st.info("💡 La hoja debe tener columnas: 'Mes', 'EBITDA', 'Margen'")
        elif "Error de autenticación" in error_msg:
            st.error(f"**Error de autenticación**: {error_msg}")
            st.info("💡 Verifica que las credenciales JSON sean correctas y que la cuenta de servicio tenga permisos")
        else:
            st.error(f"**Error cargando Google Sheets**: {error_msg}")
            st.info("💡 Revisa la configuración y los logs para más detalles")
    elif not econ or len(econ) == 0:
        st.warning("⚠️ No hay datos disponibles de Google Sheets")
        st.info("💡 Verifica la configuración o sincroniza desde la pestaña 'Ingesta'")
    else:
        # Filtrar errores del dict (keys que empiezan con _)
        econ_clean = {k: v for k, v in econ.items() if not str(k).startswith("_")}
        
        if not econ_clean:
            st.warning("⚠️ No hay datos válidos en Google Sheets")
            st.info("💡 Verifica que la hoja tenga datos en formato correcto")
        else:
            try:
                months_in_range = sorted({d.month for d in pd.date_range(start, end, freq="D")})
                if not months_in_range:
                    months_in_range = list(range(1, 13))
            except Exception:
                months_in_range = list(range(1, 13))

            rows = []
            for m in months_in_range:
                info = econ_clean.get(int(m), {"ebitda_eur": 0.0, "margin": 0.0})
                rows.append({
                    "Mes": int(m),
                    "EBITDA (EUR)": float(info.get("ebitda_eur") or 0.0),
                    "Margen (%)": float(info.get("margin") or 0.0) * 100.0,
                })

            sel_ebitda = 0.0
            sel_margin_avg = 0.0
            if rows:
                df_econ = pd.DataFrame(rows)
                sel_ebitda = float(df_econ["EBITDA (EUR)"].sum())
                sel_margin_avg = float(df_econ["Margen (%)"].mean()) if len(df_econ) else 0.0
            c1, c2 = st.columns(2)
            c1.metric("EBITDA (meses seleccionados)", f"€{sel_ebitda:,.0f}")
            c2.metric("Margen medio (meses seleccionados)", f"{sel_margin_avg:,.1f}%")

            # Gráfico anual (12 meses)
            month_names = {1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun", 7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"}
            annual_rows = []
            for m in range(1, 13):
                info = econ_clean.get(int(m), {"ebitda_eur": 0.0, "margin": 0.0})
                annual_rows.append({
                    "Mes": month_names[m],
                    "EBITDA (EUR)": float(info.get("ebitda_eur") or 0.0),
                    "Margen (%)": float(info.get("margin") or 0.0) * 100.0,
                })
            df_annual = pd.DataFrame(annual_rows)
            st.markdown("### Gráfico anual: EBITDA (EUR) y Margen (%)")
            
            # Calcular rangos para alinear los ceros visualmente
            ebitda_values = df_annual["EBITDA (EUR)"].values
            margen_values = df_annual["Margen (%)"].values
            
            ebitda_min, ebitda_max = float(ebitda_values.min()), float(ebitda_values.max())
            margen_min, margen_max = float(margen_values.min()), float(margen_values.max())
            
            # Calcular distancias desde cero en ambas direcciones
            ebitda_neg = abs(ebitda_min) if ebitda_min < 0 else 0
            ebitda_pos = abs(ebitda_max) if ebitda_max > 0 else 0
            ebitda_max_dist = max(ebitda_neg, ebitda_pos, 1)
            
            margen_neg = abs(margen_min) if margen_min < 0 else 0
            margen_pos = abs(margen_max) if margen_max > 0 else 0
            margen_max_dist = max(margen_neg, margen_pos, 1)
            
            # Calcular factor de escala para que las distancias desde cero sean proporcionales
            # Esto asegura que el cero esté alineado visualmente
            if ebitda_max_dist > 0 and margen_max_dist > 0:
                scale_factor = ebitda_max_dist / margen_max_dist
            else:
                scale_factor = 1.0
            
            # Aplicar padding del 10% en ambos lados
            ebitda_y_min = -ebitda_max_dist * 1.1
            ebitda_y_max = ebitda_max_dist * 1.1
            
            # Escalar el margen para que el cero esté alineado con el de EBITDA
            margen_scaled_min = -margen_max_dist * scale_factor * 1.1
            margen_scaled_max = margen_max_dist * scale_factor * 1.1
            
            fig_ann = make_subplots(specs=[[{"secondary_y": True}]])
            fig_ann.add_trace(
                go.Bar(x=df_annual["Mes"], y=df_annual["EBITDA (EUR)"], name="EBITDA (EUR)"),
                secondary_y=False,
            )
            
            # Escalar los valores de margen visualmente para alinearlos, pero mantener tooltip original
            margen_scaled = df_annual["Margen (%)"] * scale_factor
            fig_ann.add_trace(
                go.Scatter(
                    x=df_annual["Mes"], 
                    y=margen_scaled, 
                    name="Margen (%)", 
                    mode="lines+markers",
                    customdata=df_annual["Margen (%)"].values,  # Valores originales para tooltip
                    hovertemplate="<b>Margen (%)</b><br>Mes: %{x}<br>Margen: %{customdata:.2f}%<extra></extra>"
                ),
                secondary_y=True,
            )
            
            # Configurar ejes: ambos con cero alineado
            fig_ann.update_yaxes(
                title_text="EBITDA (EUR)", 
                secondary_y=False,
                range=[ebitda_y_min, ebitda_y_max],
                zeroline=True,
                zerolinewidth=2,
                zerolinecolor='rgba(128,128,128,0.5)'
            )
            
            # Crear ticks personalizados para el eje derecho que muestren los valores originales
            num_ticks = 6
            margen_tick_step = (margen_max - margen_min) / (num_ticks - 1) if margen_max > margen_min else 10
            margen_tick_values = [margen_min + i * margen_tick_step for i in range(num_ticks)]
            margen_tick_labels = [f"{val:.1f}" for val in margen_tick_values]
            margen_tick_positions_scaled = [val * scale_factor for val in margen_tick_values]
            
            fig_ann.update_yaxes(
                title_text="Margen (%)", 
                secondary_y=True,
                range=[margen_scaled_min, margen_scaled_max],
                zeroline=True,
                zerolinewidth=2,
                zerolinecolor='rgba(128,128,128,0.5)',
                tickmode='array',
                tickvals=margen_tick_positions_scaled,
                ticktext=margen_tick_labels
            )
            
            fig_ann.update_layout(
                margin=dict(l=10, r=10, t=40, b=10), 
                legend=dict(orientation="h")
            )
            st.plotly_chart(fig_ann, use_container_width=True)
        
        # LTV global (cabecera)
        try:
            ltv_global = load_ltv_global()
            sales_global = load_sales_global_total(start, end)
            c1, c2 = st.columns(2)
            c1.metric("Ventas globales (EUR)", f"€{float(sales_global or 0):,.0f}")
            c2.metric("LTV global (EUR)", f"€{float(ltv_global or 0):,.0f}")
        except Exception:
            pass
        
        st.markdown("---")
    
    st.subheader("KPIs del período")
    # Toggle de enmascarado (persistente en sesión)
    if "mask_kpis" not in st.session_state:
        st.session_state["mask_kpis"] = False
    col_eye, _sp = st.columns([1,9])
    with col_eye:
        label = "Ocultar KPIs" if not st.session_state["mask_kpis"] else "Mostrar KPIs"
        icon = "👁️" if not st.session_state["mask_kpis"] else "🙈"
        if st.button(f"{icon} {label}", key="btn_toggle_mask_kpis"):
            st.session_state["mask_kpis"] = not st.session_state["mask_kpis"]
    k1, k2, k3 = st.columns(3)
    if df_base.empty:
        k1.metric("Ingresos Totales (EUR)", "€0.00")
        k2.metric("Pagos Totales", "0")
        k3.metric("Ticket Medio (EUR)", "€0.00")
        st.info("Sin datos de pagos en el rango seleccionado.")
        return

    total_revenue_eur = df_base["gross_amount_eur"].sum()
    total_payments = len(df_base)
    aov_eur = (total_revenue_eur / total_payments) if total_payments > 0 else 0
    if st.session_state["mask_kpis"]:
        k1.metric("Ingresos Totales (EUR)", "********")
        k2.metric("Pagos Totales", "*****")
        k3.metric("Ticket Medio (EUR)", "********")
    else:
        k1.metric("Ingresos Totales (EUR)", f"€{total_revenue_eur:,.2f}")
        k2.metric("Pagos Totales", f"{total_payments:,}")
        k3.metric("Ticket Medio (EUR)", f"€{aov_eur:,.2f}")

    # Agregación
    if view_mode == "EUR (convertido)":
        df_agg = (
            df_base.groupby(["day_agg", "series"], as_index=False)
            .agg(gross_amount_eur=("gross_amount_eur", "sum"))
        )
        color_field = "series"
        hover_name = "series"
    else:
        df_agg = (
            df_base.groupby(["day_agg", "currency_original"], as_index=False)
            .agg(gross_amount_eur=("gross_amount_eur", "sum"))
        )
        color_field = "currency_original"
        hover_name = "currency_original"

    # Relleno de fechas
    df_agg = df_agg.rename(columns={"day_agg": "day"})
    df_agg["day"] = pd.to_datetime(df_agg["day"]).dt.normalize()
    # Usar PeriodIndex para alinear exactamente con la agregación de data.py (D, W-MON, M)
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

    out = []
    unique_series = df_agg[color_field].unique()
    for s in unique_series:
        sub = df_agg[df_agg[color_field] == s].set_index("day").reindex(all_days, fill_value=0.0)
        sub[color_field] = s
        sub = sub.reset_index().rename(columns={"index": "day"})
        out.append(sub)
    df_filled = pd.concat(out, ignore_index=True) if out else pd.DataFrame()

    # Gráfico
    titulo = f"Ingresos en EUR (grano: {grain_effective})"
    color_map = build_color_map(df_filled[color_field].dropna().unique().tolist())
    fig = px.bar(
        df_filled,
        x="day",
        y="gross_amount_eur",
        color=color_field,
        title=titulo,
        barmode="stack",
        color_discrete_map=color_map,
        hover_name=hover_name,
        labels={"day": "Fecha", "gross_amount_eur": "Ingresos (EUR)", "series": "Producto/Categoría", "currency_original": "Moneda"},
    )
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df_filled.sort_values(["day", color_field]).reset_index(drop=True), use_container_width=True)

    # Diagnóstico por moneda
    try:
        with engine.begin() as conn:
            diag = pd.read_sql(
                text(
                    """
                    SELECT UPPER(currency_original) AS currency,
                           COUNT(*) AS pagos,
                           SUM(amount_original_minor)/100.0 AS amount_sum_major
                    FROM payments
                    WHERE paid_at IS NOT NULL
                      AND paid_at::date BETWEEN :start AND :end
                    GROUP BY 1
                    ORDER BY pagos DESC
                    """
                ),
                conn,
                params={"start": start, "end": end},
            )
    except Exception as e:
        st.warning(f"No se pudo cargar diagnóstico por moneda: {e}")
        diag = pd.DataFrame()
    st.markdown("**Diagnóstico por moneda (BD cruda, sin conversión):**")
    st.dataframe(diag, use_container_width=True)

    # Top productos
    st.subheader("Análisis detallado del período")
    top_df = (
        df_base.groupby("series", as_index=False)
        .agg(eur_sum=("gross_amount_eur", "sum"), pagos=("gross_amount_eur", "count"))
        .sort_values("eur_sum", ascending=False)
        .reset_index(drop=True)
    )
    st.markdown("**Top productos/categorías (EUR):**")
    c1, c2 = st.columns(2)
    with c1:
        st.dataframe(top_df, use_container_width=True)
    with c2:
        src_df = (
            df_base.groupby("currency_original", as_index=False)
            .agg(eur_sum=("gross_amount_eur", "sum"))
        )
        if not src_df.empty:
            pie = px.pie(src_df, names="currency_original", values="eur_sum", title="Mix por moneda original (en EUR)")
            st.plotly_chart(pie, use_container_width=True)

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
    try:
        with engine.begin() as conn:
            subs = conn.execute(subsq, {"start": start, "end": end}).mappings().first()
    except Exception as e:
        st.warning(f"No se pudieron cargar KPIs de suscripciones: {e}")
        subs = {"activas": 0, "altas": 0, "bajas": 0}
    s1, s2, s3 = st.columns(3)
    if st.session_state["mask_kpis"]:
        s1.metric("Subs activas", "*****")
        s2.metric("Altas (periodo)", "*****")
        s3.metric("Bajas (periodo)", "*****")
    else:
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
                      AND (:source IS NULL OR p.source = :source)
                    """
                ),
                conn,
                params={"from": _from, "to": _to, "source": source_filter_val},
            )

        if df_pay.empty:
            return 0.0

        # Excluir probables suscripciones por nombre
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

        # Mapear FX por día/moneda
        all_curs = [c for c in df_pay["cur_uc"].unique() if c and c != 'EUR']
        rate_map: dict[tuple[date, str], float] = {}
        for c in all_curs:
            rates = get_fx_timeseries(_from, _to, c)
            for d, r in rates.items():
                rate_map[(d, c)] = float(r or 0.0) or float(FALLBACK_FX_RATES.get(c, 1.0))
        # Fallback si no hay rate
        def _rate(row):
            if row["cur_uc"] == 'EUR' or row["orig_major"] <= 0:
                return 1.0
            return rate_map.get((row["day_date"], row["cur_uc"]), float(FALLBACK_FX_RATES.get(row["cur_uc"], 1.0)))
        df_pay["fx"] = df_pay.apply(_rate, axis=1)
        df_pay["eur"] = df_pay["eur_direct"] + df_pay["orig_major"] * df_pay["fx"]
        return float(df_pay["eur"].sum())

    week_one_shot = _one_shot_sum_eur(week_start, today_ref)
    month_one_shot = _one_shot_sum_eur(month_start, today_ref)

    # MRR prorrateado con FX para no-EUR
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

        unique_currencies = rows["currency_original"].unique()
        rates_today_map = {}
        for currency in unique_currencies:
            if currency and str(currency).upper() != 'EUR':
                rate_data = get_fx_timeseries(today_ref, today_ref, str(currency))
                rates_today_map[currency] = rate_data.get(today_ref, FALLBACK_FX_RATES.get(str(currency).upper(), 0.0))
            elif currency and str(currency).upper() == 'EUR':
                rates_today_map[currency] = 1.0

        rows["fx"] = rows["currency_original"].map(rates_today_map).fillna(0.0)
        rows["amount_eur"] = rows["amount_major"] * rows["fx"]
        def _prorr(amount: float, intervalo: str) -> float:
            if intervalo == "mensual": return amount
            if intervalo == "trimestral": return amount/3.0
            if intervalo == "semestral": return amount/6.0
            if intervalo == "anual": return amount/12.0
            if intervalo == "semanal": return amount*4.34524
            return 0.0
        rows["mrr_eur"] = rows.apply(lambda r: _prorr(float(r["amount_eur"] or 0.0), r["intervalo"]), axis=1)
        mrr_df = rows.groupby("intervalo", as_index=False).agg(subs=("intervalo","count"), mrr_eur=("mrr_eur","sum"))

    cA, cB, cC = st.columns(3)
    if st.session_state["mask_kpis"]:
        cA.metric("One‑shot última semana", "********")
        cB.metric("One‑shot último mes", "********")
        cC.metric("MRR total (EUR)", "********")
    else:
        cA.metric("One‑shot última semana", f"€{float(week_one_shot or 0):,.2f}")
        cB.metric("One‑shot último mes", f"€{float(month_one_shot or 0):,.2f}")
        cC.metric("MRR total (EUR)", f"€{float(mrr_df['mrr_eur'].sum() if not mrr_df.empty else 0):,.2f}")

    if not mrr_df.empty:
        st.markdown("**MRR por intervalo (prorrateado a mensual):**")
        mrr_out = mrr_df.rename(columns={"intervalo":"Intervalo","subs":"Subs","mrr_eur":"MRR EUR"}).copy()
        mrr_out["MRR EUR"] = mrr_out["MRR EUR"].astype(float).round(2)
        mrr_out["Subs"] = mrr_out["Subs"].astype(int)
        try:
            st.dataframe(mrr_out.style.format({"MRR EUR": "€{:.2f}"}), width='stretch')
        except Exception:
            st.dataframe(mrr_out, width='stretch')


