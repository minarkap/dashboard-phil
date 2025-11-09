from __future__ import annotations

from datetime import date
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from backend.etl import ga4_client


@st.cache_data(ttl=300)
def _load_pages_screens(start: date, end: date) -> pd.DataFrame:
    # Evita módulos cacheados sin la función recién añadida
    if not hasattr(ga4_client, "fetch_pages_screens"):
        try:
            import importlib
            importlib.reload(ga4_client)  # type: ignore
        except Exception:
            pass
    fetch_fn = getattr(ga4_client, "fetch_pages_screens", None)
    if fetch_fn is None:
        raise AttributeError("backend.etl.ga4_client no expone fetch_pages_screens()")
    rows = fetch_fn(start, end, limit=1000) or []
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    # Unificar nombre de métrica de eventos clave
    if "key_events" not in df.columns and "conversions" in df.columns:
        df["key_events"] = df["conversions"]
    # Asegurar tipos numéricos
    for c in ["views", "views_per_user", "user_engagement_duration_sec", "active_users", "key_events"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    # Orden por vistas desc
    df = df.sort_values("views", ascending=False).reset_index(drop=True)
    return df


@st.cache_data(ttl=300)
def _load_acquisition_channels(start: date, end: date) -> pd.DataFrame:
    # Evita módulos cacheados sin la función recién añadida
    if not hasattr(ga4_client, "fetch_acquisition_channels"):
        try:
            import importlib
            importlib.reload(ga4_client)  # type: ignore
        except Exception:
            pass
    fetch_fn = getattr(ga4_client, "fetch_acquisition_channels", None)
    if fetch_fn is None:
        raise AttributeError("backend.etl.ga4_client no expone fetch_acquisition_channels()")
    rows = fetch_fn(start, end, limit=1000) or []
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    # Unificar nombre de métrica de eventos clave
    if "key_events" not in df.columns and "conversions" in df.columns:
        df["key_events"] = df["conversions"]
    for c in ["sessions", "engagement_rate", "key_events"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    # Orden por sesiones desc
    df = df.sort_values("sessions", ascending=False).reset_index(drop=True)
    return df


@st.cache_data(ttl=300)
def _load_funnel_metrics(start: date, end: date) -> dict:
    if not hasattr(ga4_client, "fetch_funnel_metrics"):
        try:
            import importlib
            importlib.reload(ga4_client)  # type: ignore
        except Exception:
            pass
    fetch_fn = getattr(ga4_client, "fetch_funnel_metrics", None)
    if fetch_fn is None:
        return {"sessions": 0, "views": 0, "key_events": 0, "conversion_rate": 0.0}
    return fetch_fn(start, end) or {"sessions": 0, "views": 0, "key_events": 0, "conversion_rate": 0.0}


@st.cache_data(ttl=300)
def _load_trends_daily(start: date, end: date) -> pd.DataFrame:
    if not hasattr(ga4_client, "fetch_trends_daily"):
        try:
            import importlib
            importlib.reload(ga4_client)  # type: ignore
        except Exception:
            pass
    fetch_fn = getattr(ga4_client, "fetch_trends_daily", None)
    if fetch_fn is None:
        return pd.DataFrame()
    rows = fetch_fn(start, end) or []
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "key_events" not in df.columns and "conversions" in df.columns:
        df["key_events"] = df["conversions"]
    for c in ["sessions", "views", "engagement_rate", "key_events"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df = df.sort_values("date").reset_index(drop=True)
    return df


@st.cache_data(ttl=300)
def _load_landing_pages(start: date, end: date) -> pd.DataFrame:
    if not hasattr(ga4_client, "fetch_landing_pages"):
        try:
            import importlib
            importlib.reload(ga4_client)  # type: ignore
        except Exception:
            pass
    fetch_fn = getattr(ga4_client, "fetch_landing_pages", None)
    if fetch_fn is None:
        return pd.DataFrame()
    rows = fetch_fn(start, end, limit=50) or []
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "key_events" not in df.columns and "conversions" in df.columns:
        df["key_events"] = df["conversions"]
    for c in ["sessions", "bounce_rate", "key_events", "conversion_rate"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df = df.sort_values("sessions", ascending=False).reset_index(drop=True)
    return df


def _format_seconds_mmss(seconds: float) -> str:
    try:
        total = int(round(float(seconds or 0)))
        m = total // 60
        s = total % 60
        return f"{m}m {s:02d}s"
    except Exception:
        return "0m 00s"


def render_analytics_tab(start: date, end: date) -> None:
    st.markdown("### 📈 Analytics (GA4 en vivo)")
    with st.spinner("Consultando GA4…"):
        df_pages = _load_pages_screens(start, end)
        df_channels = _load_acquisition_channels(start, end)

    # ---- Sección 1: Páginas y pantallas ----
    st.subheader("Páginas y pantallas")
    if df_pages.empty:
        st.info("No hay datos de páginas para el rango seleccionado.")
    else:
        # Calcular tiempo medio por usuario activo
        if "user_engagement_duration_sec" in df_pages.columns and "active_users" in df_pages.columns:
            with pd.option_context("mode.use_inf_as_na", True):
                df_pages["avg_engagement_time_sec"] = (df_pages["user_engagement_duration_sec"] / df_pages["active_users"]).replace([float("inf"), float("-inf")], 0).fillna(0.0)
        else:
            df_pages["avg_engagement_time_sec"] = 0.0

        df_pages_display = df_pages.rename(
            columns={
                "page_path": "Ruta de página",
                "views": "Vistas",
                "views_per_user": "Vistas por usuario",
                "avg_engagement_time_sec": "Tiempo de interacción medio por usuario (s)",
                "key_events": "Eventos clave",
            }
        ).copy()
        # Formateos
        time_col = "Tiempo de interacción medio por usuario (s)"
        if time_col in df_pages_display.columns:
            df_pages_display["Tiempo de interacción medio por usuario"] = df_pages_display[time_col].apply(_format_seconds_mmss)
            df_pages_display = df_pages_display.drop(columns=[time_col])

        # Mostrar tabla
        st.dataframe(
            df_pages_display[["Ruta de página", "Vistas", "Eventos clave", "Vistas por usuario", "Tiempo de interacción medio por usuario"]],
            use_container_width=True,
            hide_index=True,
        )

        # Descarga CSV
        try:
            csv_pages = df_pages.to_csv(index=False)
            st.download_button(
                "📥 Descargar CSV (Páginas y pantallas)",
                data=csv_pages.encode("utf-8"),
                file_name="ga4_paginas_y_pantallas.csv",
                mime="text/csv",
            )
        except Exception:
            pass

        # Gráfico: Top 10 páginas por vistas
        try:
            top_pages = df_pages.head(10)
            fig_pages = px.bar(
                top_pages,
                x="page_path",
                y="views",
                title="Top 10 páginas por Vistas",
                labels={"page_path": "Ruta de página", "views": "Vistas"},
            )
            fig_pages.update_layout(xaxis_tickangle=-30, height=400)
            st.plotly_chart(fig_pages, use_container_width=True)
        except Exception:
            pass

    st.markdown("---")

    # ---- Sección 2: Adquisición de tráfico ----
    st.subheader("Adquisición de tráfico")
    if df_channels.empty:
        st.info("No hay datos de adquisición para el rango seleccionado.")
    else:
        df_channels_display = df_channels.rename(
            columns={
                "channel": "Canal",
                "sessions": "Sesiones",
                "engagement_rate": "Porcentaje de interacciones (ratio)",
                "key_events": "Eventos clave",
            }
        ).copy()
        # Formatear porcentaje
        if "Porcentaje de interacciones (ratio)" in df_channels_display.columns:
            df_channels_display["% interacciones"] = (df_channels_display["Porcentaje de interacciones (ratio)"] * 100.0).round(2)
            df_channels_display = df_channels_display.drop(columns=["Porcentaje de interacciones (ratio)"])

        st.dataframe(
            df_channels_display[["Canal", "Sesiones", "% interacciones", "Eventos clave"]],
            use_container_width=True,
            hide_index=True,
        )

        # Descarga CSV
        try:
            # Añadir columna de % para el CSV también
            df_csv = df_channels.copy()
            df_csv["engagement_rate_pct"] = (df_csv["engagement_rate"] * 100.0).round(2)
            csv_channels = df_csv.to_csv(index=False)
            st.download_button(
                "📥 Descargar CSV (Adquisición de tráfico)",
                data=csv_channels.encode("utf-8"),
                file_name="ga4_adquisicion_trafico.csv",
                mime="text/csv",
            )
        except Exception:
            pass

        # Gráfico: Sesiones por canal (Top 10)
        try:
            top_ch = df_channels.head(10)
            fig_ch = px.bar(
                top_ch,
                x="channel",
                y="sessions",
                title="Sesiones por canal (Top 10)",
                labels={"channel": "Canal", "sessions": "Sesiones"},
            )
            fig_ch.update_layout(height=380)
            st.plotly_chart(fig_ch, use_container_width=True)
        except Exception:
            pass

    st.markdown("---")

    # ---- Sección 3: Funnel de conversión ----
    st.subheader("🔽 Funnel de conversión")
    try:
        funnel = _load_funnel_metrics(start, end)
        if funnel.get("sessions", 0) > 0:
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Sesiones", f"{int(funnel['sessions']):,}")
            col2.metric("Vistas", f"{int(funnel['views']):,}")
            col3.metric("Eventos clave", f"{int(funnel['key_events']):,}")
            col4.metric("Tasa de conversión", f"{funnel['conversion_rate']:.2f}%")

            # Visualización del funnel
            funnel_data = pd.DataFrame({
                "Etapa": ["Sesiones", "Vistas", "Eventos clave"],
                "Cantidad": [funnel["sessions"], funnel["views"], funnel["key_events"]],
            })
            # Normalizar para visualización (porcentaje respecto a sesiones)
            funnel_data["Porcentaje"] = (funnel_data["Cantidad"] / funnel["sessions"] * 100.0).round(1)

            fig_funnel = go.Figure()
            fig_funnel.add_trace(go.Funnel(
                y=funnel_data["Etapa"],
                x=funnel_data["Cantidad"],
                textposition="inside",
                textinfo="value+percent initial",
                marker={"color": ["#1f77b4", "#ff7f0e", "#2ca02c"]},
            ))
            fig_funnel.update_layout(title="Funnel de conversión", height=300)
            st.plotly_chart(fig_funnel, use_container_width=True)
        else:
            st.info("No hay datos suficientes para mostrar el funnel.")
    except Exception as e:
        st.warning(f"No se pudo cargar el funnel: {e}")

    st.markdown("---")

    # ---- Sección 4: Tendencias temporales ----
    st.subheader("📈 Tendencias temporales")
    try:
        df_trends = _load_trends_daily(start, end)
        if not df_trends.empty:
            # Gráfico de líneas con doble eje Y
            fig_trends = go.Figure()
            # Eje izquierdo: Sesiones y Vistas
            fig_trends.add_trace(go.Scatter(
                x=df_trends["date"],
                y=df_trends["sessions"],
                name="Sesiones",
                line=dict(color="blue", width=2),
                yaxis="y",
            ))
            fig_trends.add_trace(go.Scatter(
                x=df_trends["date"],
                y=df_trends["views"],
                name="Vistas",
                line=dict(color="green", width=2),
                yaxis="y",
            ))
            # Eje derecho: % Interacciones y Eventos clave
            fig_trends.add_trace(go.Scatter(
                x=df_trends["date"],
                y=df_trends["engagement_rate"] * 100,
                name="% Interacciones",
                line=dict(color="orange", width=2, dash="dash"),
                yaxis="y2",
            ))
            fig_trends.add_trace(go.Scatter(
                x=df_trends["date"],
                y=df_trends["key_events"],
                name="Eventos clave",
                line=dict(color="red", width=2, dash="dot"),
                yaxis="y2",
            ))

            fig_trends.update_layout(
                title="Evolución diaria de métricas clave",
                xaxis_title="Fecha",
                yaxis=dict(title="Sesiones / Vistas", side="left"),
                yaxis2=dict(title="% Interacciones / Eventos clave", overlaying="y", side="right"),
                height=450,
                hovermode="x unified",
                legend=dict(x=0, y=1),
            )
            st.plotly_chart(fig_trends, use_container_width=True)
        else:
            st.info("No hay datos de tendencias para el rango seleccionado.")
    except Exception as e:
        st.warning(f"No se pudieron cargar las tendencias: {e}")

    st.markdown("---")

    # ---- Sección 5: Top Landing Pages ----
    st.subheader("🚀 Top Landing Pages")
    try:
        df_landing = _load_landing_pages(start, end)
        if not df_landing.empty:
            df_landing_display = df_landing.rename(
                columns={
                    "landing_page": "Landing Page",
                    "sessions": "Sesiones",
                    "bounce_rate": "Tasa de rebote (%)",
                    "key_events": "Eventos clave",
                    "conversion_rate": "Tasa de conversión (%)",
                }
            ).copy()
            # Formatear porcentajes
            if "Tasa de rebote (%)" in df_landing_display.columns:
                df_landing_display["Tasa de rebote (%)"] = (df_landing_display["Tasa de rebote (%)"] * 100.0).round(2)
            if "Tasa de conversión (%)" in df_landing_display.columns:
                df_landing_display["Tasa de conversión (%)"] = df_landing_display["Tasa de conversión (%)"].round(2)

            st.dataframe(
                df_landing_display[["Landing Page", "Sesiones", "Tasa de rebote (%)", "Tasa de conversión (%)", "Eventos clave"]],
                use_container_width=True,
                hide_index=True,
            )

            # Descarga CSV
            try:
                csv_landing = df_landing.to_csv(index=False)
                st.download_button(
                    "📥 Descargar CSV (Landing Pages)",
                    data=csv_landing.encode("utf-8"),
                    file_name="ga4_landing_pages.csv",
                    mime="text/csv",
                )
            except Exception:
                pass

            # Gráfico: Tasa de conversión vs Tasa de rebote (scatter)
            try:
                fig_scatter = px.scatter(
                    df_landing.head(20),
                    x="bounce_rate",
                    y="conversion_rate",
                    size="sessions",
                    color="key_events",
                    hover_name="landing_page",
                    title="Tasa de conversión vs Tasa de rebote (Top 20)",
                    labels={
                        "bounce_rate": "Tasa de rebote (%)",
                        "conversion_rate": "Tasa de conversión (%)",
                        "sessions": "Sesiones",
                        "key_events": "Eventos clave",
                    },
                    color_continuous_scale="Viridis",
                )
                fig_scatter.update_layout(height=400)
                st.plotly_chart(fig_scatter, use_container_width=True)
            except Exception:
                pass
        else:
            st.info("No hay datos de landing pages para el rango seleccionado.")
    except Exception as e:
        st.warning(f"No se pudieron cargar las landing pages: {e}")



