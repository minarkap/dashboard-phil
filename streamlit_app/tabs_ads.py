from __future__ import annotations

from datetime import date
import re
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from .data import (
    load_ads_costs,
    load_leads_by_utm,
    load_ads_event_revenue,
)

# Funciones de caché para optimizar la carga
@st.cache_data(ttl=300)  # Cache por 5 minutos
def load_platform_performance_cached(platform: str, start: date, end: date):
    """Carga datos de performance de una plataforma: costos, impressions, clicks, leads"""
    costs = load_ads_costs(start, end, platform)
    leads = load_leads_by_utm(start, end, platform)
    return costs, leads


def _normalize_campaign_name(name: str) -> str:
    """Normaliza el nombre de campaña para matching más robusto.
    
    Elimina caracteres especiales comunes, espacios extra, y normaliza a mayúsculas
    para mejorar el matching entre costos (con nombres de campaña) y leads (con utm_campaign).
    """
    if pd.isna(name) or name == "":
        return ""
    # Convertir a string y limpiar
    s = str(name).strip().upper()
    # Reemplazar múltiples espacios por uno solo
    s = re.sub(r'\s+', ' ', s)
    # Remover caracteres especiales comunes que pueden variar ([, ], -, _, etc.)
    s = re.sub(r'[\[\](){}]', '', s)
    # Normalizar guiones y guiones bajos a espacios (luego se eliminarán espacios múltiples)
    s = s.replace('-', ' ').replace('_', ' ')
    # Eliminar espacios múltiples nuevamente
    s = re.sub(r'\s+', '', s)
    return s


def _calculate_performance_metrics_by_level(
    costs_df: pd.DataFrame, 
    leads_df: pd.DataFrame, 
    conversions_df: pd.DataFrame = None,
    level: str = "campaign"
) -> pd.DataFrame:
    """Calcula métricas de performance agregadas por nivel (campaign, adset, ad)"""
    if costs_df.empty and leads_df.empty:
        return pd.DataFrame()
    
    # Determinar columnas de agrupación según el nivel
    if level == "campaign":
        group_cols = ["campaign_id", "campaign_name"]
        cost_group_cols = ["campaign_name_norm"]
        lead_group_cols = ["campaign_name_norm"]
        conv_group_cols = ["campaign_id"]
    elif level == "adset":
        group_cols = ["campaign_id", "campaign_name", "adset_id", "adset_name"]
        cost_group_cols = ["campaign_name_norm", "adset_id"]
        lead_group_cols = ["campaign_name_norm"]  # Leads no tienen adset_id, solo campaign
        conv_group_cols = ["campaign_id", "adset_id"]
    else:  # ad
        group_cols = ["campaign_id", "campaign_name", "adset_id", "adset_name", "ad_id", "ad_name"]
        cost_group_cols = ["campaign_name_norm", "adset_id", "ad_id"]
        lead_group_cols = ["campaign_name_norm"]  # Leads no tienen ad/adset_id
        conv_group_cols = ["campaign_id", "adset_id", "ad_id"]
    
    # Agregar costos
    if not costs_df.empty:
        costs_df = costs_df.copy()
        # Filtrar nombres vacíos o inválidos antes de normalizar
        costs_df = costs_df[
            (costs_df["campaign_name"].astype(str).str.strip() != "") &
            (costs_df["campaign_name"].astype(str).str.strip() != "0") &
            (costs_df["campaign_name"].astype(str).str.strip() != "None")
        ]
        costs_df["campaign_name_norm"] = costs_df["campaign_name"].apply(_normalize_campaign_name)
        
        agg_dict = {
            "cost_eur": "sum",
            "impressions": "sum",
            "clicks": "sum",
            "campaign_name": "first",
            "campaign_id": "first",
        }
        if level in ["adset", "ad"]:
            if "adset_id" in costs_df.columns:
                agg_dict["adset_id"] = "first"
            if "adset_name" in costs_df.columns:
                agg_dict["adset_name"] = "first"
        if level == "ad":
            if "ad_id" in costs_df.columns:
                agg_dict["ad_id"] = "first"
            if "ad_name" in costs_df.columns:
                agg_dict["ad_name"] = "first"
        
        # Filtrar cost_group_cols para incluir solo columnas que existen
        valid_cost_group_cols = [col for col in cost_group_cols if col in costs_df.columns]
        costs_agg = costs_df.groupby(valid_cost_group_cols, as_index=False).agg(agg_dict)
    else:
        costs_agg = pd.DataFrame()
    
    # Agregar leads (siempre por campaign_name porque leads solo tienen utm_campaign)
    if not leads_df.empty:
        leads_df = leads_df.copy()
        # Filtrar nombres vacíos o inválidos antes de normalizar
        leads_df = leads_df[
            (leads_df["campaign_name"].astype(str).str.strip() != "") &
            (leads_df["campaign_name"].astype(str).str.strip() != "0") &
            (leads_df["campaign_name"].astype(str).str.strip() != "None")
        ]
        leads_df["campaign_name_norm"] = leads_df["campaign_name"].apply(_normalize_campaign_name)
        leads_agg = leads_df.groupby(lead_group_cols, as_index=False).agg({
            "leads": "sum",
            "campaign_name": "first",
        })
    else:
        leads_agg = pd.DataFrame()
    
    # Agregar conversiones
    if conversions_df is not None and not conversions_df.empty:
        conversions_df = conversions_df.copy()
        conversions_df["campaign_id"] = conversions_df["campaign_id"].astype(str)
        # Filtrar conv_group_cols para incluir solo columnas que existen
        valid_conv_group_cols = [col for col in conv_group_cols if col in conversions_df.columns]
        conversions_agg = conversions_df.groupby(valid_conv_group_cols, as_index=False).agg({
            "purchases": "sum",
            "revenue_eur": "sum",
        })
    else:
        conversions_agg = pd.DataFrame()
    
    # Merge todos los datos
    result = costs_agg.copy() if not costs_agg.empty else pd.DataFrame()
    
    if not leads_agg.empty:
        if result.empty:
            result = leads_agg.copy()
            # Asegurar que la columna leads existe
            if "leads" not in result.columns:
                result["leads"] = 0
        else:
            # Asegurar que result tiene la columna leads inicializada
            if "leads" not in result.columns:
                result["leads"] = 0
            
            result = result.merge(leads_agg, on="campaign_name_norm", how="outer", suffixes=("", "_leads"))
            
            # Manejar la columna leads después del merge
            if "leads_leads" in result.columns:
                # Si hay leads_leads, combinar con leads existente
                result["leads"] = result["leads"].fillna(0) + result["leads_leads"].fillna(0)
                result = result.drop(columns=["leads_leads"], errors="ignore")
            elif "leads" not in result.columns:
                # Si no hay columna leads, usar la de leads_agg
                result["leads"] = result.get("leads", 0)
            
            # Llenar NaN en otras columnas numéricas
            numeric_cols = result.select_dtypes(include=[float, int]).columns
            result[numeric_cols] = result[numeric_cols].fillna(0)
            
            if "campaign_name_leads" in result.columns:
                result["campaign_name"] = result["campaign_name"].fillna(result["campaign_name_leads"])
                result = result.drop(columns=["campaign_name_leads"], errors="ignore")
    else:
        # Si no hay leads_agg pero hay result, asegurar que la columna leads existe
        if not result.empty and "leads" not in result.columns:
            result["leads"] = 0
    
    if not conversions_agg.empty:
        if result.empty:
            result = conversions_agg.copy()
        else:
            # Merge por campaign_id (y adset_id/ad_id si aplica)
            merge_on = ["campaign_id"]
            if level in ["adset", "ad"]:
                if "adset_id" in result.columns and "adset_id" in conversions_agg.columns:
                    merge_on.append("adset_id")
            if level == "ad":
                if "ad_id" in result.columns and "ad_id" in conversions_agg.columns:
                    merge_on.append("ad_id")
            
            conversions_agg = conversions_agg.rename(columns={"revenue_eur": "revenue_from_insights"})
            # Asegurar que todas las columnas de merge_on existan en ambos DataFrames
            merge_on_valid = [col for col in merge_on if col in result.columns and col in conversions_agg.columns]
            if merge_on_valid:
                result = result.merge(
                    conversions_agg[merge_on_valid + ["purchases", "revenue_from_insights"]], 
                    on=merge_on_valid, 
                    how="left"
                ).fillna(0)
            else:
                # Fallback: solo merge por campaign_id si las otras columnas no existen
                result = result.merge(
                    conversions_agg[["campaign_id", "purchases", "revenue_from_insights"]], 
                    on="campaign_id", 
                    how="left"
                ).fillna(0)
            result["revenue_eur"] = result.get("revenue_from_insights", 0)
            result = result.drop(columns=["revenue_from_insights"], errors="ignore")
    else:
        result["purchases"] = 0
        result["revenue_eur"] = 0
    
    # Limpiar columnas auxiliares
    if "campaign_name_norm" in result.columns:
        result = result.drop(columns=["campaign_name_norm"], errors="ignore")
    
    # Filtrar filas sin datos
    if not result.empty:
        result = result[
            (result.get("impressions", 0) > 0) | 
            (result.get("clicks", 0) > 0) | 
            (result.get("leads", 0) > 0) |
            (result.get("purchases", 0) > 0)
        ]
    
    # Calcular métricas derivadas
    if not result.empty:
        result["ctr"] = (result.get("clicks", 0) / result.get("impressions", 1) * 100).replace([float('inf'), float('-inf')], 0).fillna(0)
        result["cpm"] = (result.get("cost_eur", 0) / result.get("impressions", 1) * 1000).replace([float('inf'), float('-inf')], 0).fillna(0)
        result["cpc"] = (result.get("cost_eur", 0) / result.get("clicks", 1)).replace([float('inf'), float('-inf')], 0).fillna(0)
        result["cpl"] = (result.get("cost_eur", 0) / result.get("leads", 1)).replace([float('inf'), float('-inf')], 0).fillna(0)
        result["leads_rate"] = (result.get("leads", 0) / result.get("clicks", 1) * 100).replace([float('inf'), float('-inf')], 0).fillna(0)
        
        if "purchases" in result.columns:
            result["conversion_rate"] = (result.get("purchases", 0) / result.get("clicks", 1) * 100).replace([float('inf'), float('-inf')], 0).fillna(0)
            result["cpa"] = (result.get("cost_eur", 0) / result.get("purchases", 1)).replace([float('inf'), float('-inf')], 0).fillna(0)
            result["roas"] = (result.get("revenue_eur", 0) / result.get("cost_eur", 1)).replace([float('inf'), float('-inf')], 0).fillna(0)
    
    return result


def _calculate_performance_metrics(costs_df: pd.DataFrame, leads_df: pd.DataFrame, conversions_df: pd.DataFrame = None) -> pd.DataFrame:
    """Alias para mantener compatibilidad - calcula métricas por campaña"""
    return _calculate_performance_metrics_by_level(costs_df, leads_df, conversions_df, "campaign")


def _render_table_view_performance(df: pd.DataFrame, platform: str, level: str):
    """Renderiza tabla de performance"""
    if df.empty:
        st.info(f"No hay datos de {platform.replace('_', ' ').title()} para mostrar.")
        return

    # Ordenar por impressions descendente (o leads si no hay impressions)
    if "impressions" in df.columns:
        df_display = df.sort_values("impressions", ascending=False)
    elif "leads" in df.columns:
        df_display = df.sort_values("leads", ascending=False)
    else:
        df_display = df.copy()
    
    # Formatear columnas numéricas
    display_cols = {
        "campaign_name": "Campaña",
        "cost_eur": "Coste (€)",
        "impressions": "Impressions",
        "clicks": "Clicks",
        "ctr": "CTR (%)",
        "cpm": "CPM (€)",
        "cpc": "CPC (€)",
        "leads": "Leads",
        "cpl": "CPL (€)",
        "leads_rate": "Tasa Leads (%)",
        "purchases": "Conversiones",
        "revenue_eur": "Revenue (€)",
        "conversion_rate": "Tasa Conversión (%)",
        "cpa": "CPA (€)",
        "roas": "ROAS",
    }
    
    # Seleccionar solo las columnas que existen
    available_cols = {k: v for k, v in display_cols.items() if k in df_display.columns}
    df_display = df_display[list(available_cols.keys())].copy()
    df_display = df_display.rename(columns=available_cols)
    
    # Formatear valores
    for col in df_display.columns:
        if "€" in col or col == "Coste (€)":
            df_display[col] = df_display[col].apply(lambda x: f"€{x:,.2f}" if pd.notna(x) and x != 0 else "€0.00")
        elif "%" in col:
            df_display[col] = df_display[col].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "0.00%")
        elif col in ["Impressions", "Clicks", "Leads", "Conversiones"]:
            df_display[col] = df_display[col].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")
        elif col == "ROAS":
            df_display[col] = df_display[col].apply(lambda x: f"{x:.2f}x" if pd.notna(x) and x > 0 else "0.00x")
    
    st.dataframe(df_display, use_container_width=True, hide_index=True)
    
    # Botón de descarga
    try:
        csv_data = df.to_csv(index=False)
        st.download_button(
            label=f"📥 Descargar CSV ({level})",
            data=csv_data.encode("utf-8"),
            file_name=f"{platform}_{level}_performance.csv",
            mime="text/csv"
        )
    except Exception:
        pass


def _render_graph_view_performance(costs_df: pd.DataFrame, leads_df: pd.DataFrame, platform: str, start: date, end: date, conversions_df: pd.DataFrame = None):
    """Renderiza gráficos de performance"""
    
    # 1. Timeline: Impressions y Clicks por día
    if not costs_df.empty:
        costs_daily = costs_df.groupby("day").agg({
            "impressions": "sum",
            "clicks": "sum",
            "cost_eur": "sum",
        }).reset_index()
        
        # Asegurar que los valores sean numéricos y no NaN
        costs_daily["impressions"] = pd.to_numeric(costs_daily["impressions"], errors="coerce").fillna(0)
        costs_daily["clicks"] = pd.to_numeric(costs_daily["clicks"], errors="coerce").fillna(0)
        costs_daily["cost_eur"] = pd.to_numeric(costs_daily["cost_eur"], errors="coerce").fillna(0)
        
        st.markdown("#### 📊 Evolución Temporal: Impressions, Clicks y Costes")
        fig_timeline = go.Figure()
        
        # Impressions en eje Y izquierdo
        fig_timeline.add_trace(go.Scatter(
            x=costs_daily["day"],
            y=costs_daily["impressions"],
            name="Impressions",
            line=dict(color="blue", width=2),
            mode="lines+markers",
            yaxis="y",
            hovertemplate="<b>Impressions</b><br>" +
                         "Fecha: %{x}<br>" +
                         "Impressions: %{y:,}<br>" +
                         "<extra></extra>"
        ))
        
        # Clicks multiplicados por 100 en eje Y izquierdo
        fig_timeline.add_trace(go.Scatter(
            x=costs_daily["day"],
            y=costs_daily["clicks"] * 100,
            name="Clicks (×100)",
            line=dict(color="green", width=2),
            mode="lines+markers",
            yaxis="y",
            customdata=costs_daily["clicks"],
            hovertemplate="<b>Clicks</b><br>" +
                         "Fecha: %{x}<br>" +
                         "Clicks: %{customdata:,}<br>" +
                         "<extra></extra>"
        ))
        
        # Costes en eje Y derecho (escala separada)
        fig_timeline.add_trace(go.Scatter(
            x=costs_daily["day"],
            y=costs_daily["cost_eur"],
            name="Costes (€)",
            line=dict(color="red", width=2, dash="dash"),
            mode="lines+markers",
            yaxis="y2",
            hovertemplate="<b>Costes (€)</b><br>" +
                         "Fecha: %{x}<br>" +
                         "Coste: €%{y:,.2f}<br>" +
                         "<extra></extra>"
        ))
        
        fig_timeline.update_layout(
            xaxis_title="Fecha",
            yaxis=dict(title="Impressions / Clicks (×100)", side="left"),
            yaxis2=dict(title="Costes (€)", overlaying="y", side="right"),
            height=400,
            hovermode="x unified",
            legend=dict(x=0, y=1)
        )
        st.plotly_chart(fig_timeline, use_container_width=True)
    
    # 2. Impressions por Campaña (Top 10)
    if not costs_df.empty:
        camp_agg = costs_df.groupby(["campaign_id", "campaign_name"], as_index=False).agg({
            "impressions": "sum",
            "clicks": "sum",
        }).sort_values("impressions", ascending=False).head(10)
        
        if not camp_agg.empty:
            st.markdown("#### 📈 Impressions por Campaña (Top 10)")
            fig_camp = px.bar(
                camp_agg,
                x="campaign_name" if "campaign_name" in camp_agg.columns else "campaign_id",
                y="impressions",
                color="clicks",
                color_continuous_scale="Blues",
                labels={"impressions": "Impressions", "clicks": "Clicks", "campaign_name": "Campaña"},
                text="impressions"
            )
            fig_camp.update_traces(texttemplate="%{text:,}", textposition="outside")
            fig_camp.update_layout(height=400, showlegend=True)
            st.plotly_chart(fig_camp, use_container_width=True)
    
    # 3. CTR vs CPM (Scatter)
    if not costs_df.empty:
        camp_metrics = costs_df.groupby(["campaign_id", "campaign_name"], as_index=False).agg({
            "cost_eur": "sum",
            "impressions": "sum",
            "clicks": "sum",
        })
        camp_metrics["ctr"] = (camp_metrics["clicks"] / camp_metrics["impressions"] * 100).fillna(0)
        camp_metrics["cpm"] = (camp_metrics["cost_eur"] / camp_metrics["impressions"] * 1000).fillna(0)
        camp_metrics = camp_metrics[(camp_metrics["ctr"] > 0) & (camp_metrics["cpm"] > 0)]
        
        if not camp_metrics.empty:
            st.markdown("#### 🎯 CTR vs CPM por Campaña")
            fig_scatter = px.scatter(
                camp_metrics,
                x="cpm",
                y="ctr",
                size="impressions",
                color="clicks",
                hover_name="campaign_name" if "campaign_name" in camp_metrics.columns else "campaign_id",
                labels={"cpm": "CPM (€)", "ctr": "CTR (%)", "impressions": "Impressions", "clicks": "Clicks"},
                color_continuous_scale="Viridis"
            )
            fig_scatter.update_layout(height=400)
            st.plotly_chart(fig_scatter, use_container_width=True)
    
    # 4. Leads vs Conversiones por día (si hay datos)
    has_leads = not leads_df.empty
    has_conversions = conversions_df is not None and not conversions_df.empty
    
    if has_leads or has_conversions:
        fig_comparison = go.Figure()
        
        if has_leads:
            leads_df_copy = leads_df.copy()
            leads_df_copy["leads"] = pd.to_numeric(leads_df_copy["leads"], errors="coerce").fillna(0)
            leads_daily = leads_df_copy.groupby("day").agg({"leads": "sum"}).reset_index()
            
            fig_comparison.add_trace(go.Bar(
                x=leads_daily["day"],
                y=leads_daily["leads"],
                name="Leads",
                marker_color="green",
                yaxis="y"
            ))
        
        if has_conversions:
            conversions_df_copy = conversions_df.copy()
            conversions_df_copy["purchases"] = pd.to_numeric(conversions_df_copy.get("purchases", 0), errors="coerce").fillna(0)
            conversions_daily = conversions_df_copy.groupby("day").agg({"purchases": "sum"}).reset_index()
            
            fig_comparison.add_trace(go.Bar(
                x=conversions_daily["day"],
                y=conversions_daily["purchases"],
                name="Conversiones",
                marker_color="orange",
                yaxis="y"
            ))
        
        if has_leads or has_conversions:
            st.markdown("#### 👥 Leads vs Conversiones por Día")
            fig_comparison.update_layout(
                xaxis_title="Fecha",
                yaxis_title="Cantidad",
                height=300,
                barmode="group",
                legend=dict(x=0, y=1)
            )
            st.plotly_chart(fig_comparison, use_container_width=True)


def render_ads_tab(start: date, end: date):
    st.markdown("### 📊 Performance de Anuncios")
    
    # Calcular métricas globales
    try:
        costs_meta = load_ads_costs(start, end, "meta")
        costs_ga = load_ads_costs(start, end, "google_ads")
        leads_meta = load_leads_by_utm(start, end, "meta")
        leads_ga = load_leads_by_utm(start, end, "google_ads")
        
        # Agregar globales
        total_impressions = int((costs_meta["impressions"].sum() if not costs_meta.empty else 0) + 
                               (costs_ga["impressions"].sum() if not costs_ga.empty else 0))
        total_clicks = int((costs_meta["clicks"].sum() if not costs_meta.empty else 0) + 
                          (costs_ga["clicks"].sum() if not costs_ga.empty else 0))
        total_cost = float((costs_meta["cost_eur"].sum() if not costs_meta.empty else 0.0) + 
                          (costs_ga["cost_eur"].sum() if not costs_ga.empty else 0.0))
        total_leads = int((leads_meta["leads"].sum() if not leads_meta.empty else 0) + 
                         (leads_ga["leads"].sum() if not leads_ga.empty else 0))
        
        # Calcular métricas globales
        global_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0.0
        global_cpm = (total_cost / total_impressions * 1000) if total_impressions > 0 else 0.0
        global_cpc = (total_cost / total_clicks) if total_clicks > 0 else 0.0
        global_cpl = (total_cost / total_leads) if total_leads > 0 else 0.0
        
        # Mostrar KPIs
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Impressions Totales", f"{total_impressions:,}")
        col2.metric("Clicks Totales", f"{total_clicks:,}")
        col3.metric("Coste Total", f"€{total_cost:,.2f}")
        col4.metric("Leads Totales", f"{total_leads:,}")
        
        col5, col6, col7, col8 = st.columns(4)
        col5.metric("CTR Global", f"{global_ctr:.2f}%")
        col6.metric("CPM Global", f"€{global_cpm:.2f}")
        col7.metric("CPC Global", f"€{global_cpc:.2f}")
        col8.metric("CPL Global", f"€{global_cpl:.2f}")
        
    except Exception as e:
        st.warning(f"Error calculando métricas globales: {e}")
    
    st.markdown("---")
    st.markdown("### Métricas por Plataforma")
    
    # Botones de control
    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        if st.button("🔄 Limpiar caché", help="Limpia el caché de datos"):
            st.cache_data.clear()
            st.success("Caché limpiado")
    with col2:
        view_mode = st.radio("Vista", ["📊 Tablas", "📈 Gráficos"], horizontal=True, key="view_mode_ads")
    
    tab_g, tab_m = st.tabs(["Google Ads", "Meta Ads"])
    
    def render_platform_block(platform: str):
        with st.spinner(f"Cargando datos de {platform.replace('_', ' ').title()}..."):
            costs, leads = load_platform_performance_cached(platform, start, end)
        
        # KPIs de la plataforma
        total_cost = float(costs["cost_eur"].sum()) if not costs.empty else 0.0
        total_impressions = int(costs["impressions"].sum()) if not costs.empty else 0
        total_clicks = int(costs["clicks"].sum()) if not costs.empty else 0
        total_leads = int(pd.to_numeric(leads["leads"], errors="coerce").sum()) if not leads.empty else 0
        
        # Calcular métricas
        ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0.0
        cpm = (total_cost / total_impressions * 1000) if total_impressions > 0 else 0.0
        cpc = (total_cost / total_clicks) if total_clicks > 0 else 0.0
        cpl = (total_cost / total_leads) if total_leads > 0 else 0.0
        leads_rate = (total_leads / total_clicks * 100) if total_clicks > 0 else 0.0
        
        # Mostrar KPIs
        st.markdown(f"#### {platform.replace('_', ' ').title()}")
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("Impressions", f"{total_impressions:,}")
        c2.metric("Clicks", f"{total_clicks:,}")
        c3.metric("Coste", f"€{total_cost:,.2f}")
        c4.metric("Leads", f"{total_leads:,}")
        c5.metric("CTR", f"{ctr:.2f}%")
        c6.metric("CPM", f"€{cpm:.2f}")
        
        c7, c8, c9, c10 = st.columns(4)
        c7.metric("CPC", f"€{cpc:.2f}")
        c8.metric("CPL", f"€{cpl:.2f}")
        c9.metric("Tasa Leads", f"{leads_rate:.2f}%")
        c10.metric("Leads/Clicks", f"{(total_leads/total_clicks*100):.2f}%" if total_clicks > 0 else "0%")
        
        # Cargar conversiones/purchases si están disponibles
        try:
            conversions = load_ads_event_revenue(start, end, platform)
        except Exception:
            conversions = pd.DataFrame()
        
        # Selector de nivel de agregación
        level_selector = st.selectbox(
            "Nivel de detalle",
            ["Campaña", "AdSet", "Anuncio"],
            key=f"level_{platform}"
        )
        
        # Validar totales de leads
        total_leads_from_raw = total_leads  # Guardar el total original
        unmatched_leads = 0
        
        # Renderizar según el modo de vista
        if view_mode == "📊 Tablas":
            if level_selector == "Campaña":
                perf_data = _calculate_performance_metrics_by_level(costs, leads, conversions, "campaign")
                if not perf_data.empty:
                    # Validar que la suma de leads en la tabla coincide con el total
                    total_leads_in_table = int(pd.to_numeric(perf_data["leads"], errors="coerce").sum()) if "leads" in perf_data.columns else 0
                    unmatched_leads = total_leads_from_raw - total_leads_in_table
                    
                    if unmatched_leads != 0:
                        st.warning(
                            f"⚠️ Discrepancia detectada: {total_leads_from_raw} leads totales vs {total_leads_in_table} en tabla "
                            f"({unmatched_leads} leads sin coincidencia de nombre de campaña). "
                            f"Esto puede deberse a diferencias entre nombres de campaña en costos y UTMs en leads."
                        )
                    
                    st.markdown("##### Por Campaña")
                    _render_table_view_performance(perf_data, platform, "campaign")
            elif level_selector == "AdSet":
                perf_data = _calculate_performance_metrics_by_level(costs, leads, conversions, "adset")
                if not perf_data.empty:
                    # Validar que la suma de leads en la tabla coincide con el total
                    total_leads_in_table = int(pd.to_numeric(perf_data["leads"], errors="coerce").sum()) if "leads" in perf_data.columns else 0
                    unmatched_leads = total_leads_from_raw - total_leads_in_table
                    
                    if unmatched_leads != 0:
                        st.warning(
                            f"⚠️ Discrepancia detectada: {total_leads_from_raw} leads totales vs {total_leads_in_table} en tabla "
                            f"({unmatched_leads} leads sin coincidencia de nombre de campaña). "
                            f"Esto puede deberse a diferencias entre nombres de campaña en costos y UTMs en leads."
                        )
                    
                    st.markdown("##### Por AdSet")
                    _render_table_view_performance(perf_data, platform, "adset")
            else:  # Anuncio
                perf_data = _calculate_performance_metrics_by_level(costs, leads, conversions, "ad")
                if not perf_data.empty:
                    # Validar que la suma de leads en la tabla coincide con el total
                    total_leads_in_table = int(pd.to_numeric(perf_data["leads"], errors="coerce").sum()) if "leads" in perf_data.columns else 0
                    unmatched_leads = total_leads_from_raw - total_leads_in_table
                    
                    if unmatched_leads != 0:
                        st.warning(
                            f"⚠️ Discrepancia detectada: {total_leads_from_raw} leads totales vs {total_leads_in_table} en tabla "
                            f"({unmatched_leads} leads sin coincidencia de nombre de campaña). "
                            f"Esto puede deberse a diferencias entre nombres de campaña en costos y UTMs en leads."
                        )
                    
                    st.markdown("##### Por Anuncio")
                    _render_table_view_performance(perf_data, platform, "ad")
        else:
            _render_graph_view_performance(costs, leads, platform, start, end, conversions)
    
    with tab_g:
        render_platform_block("google_ads")
    
    with tab_m:
        render_platform_block("meta")
