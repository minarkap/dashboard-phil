from datetime import date, timedelta
import os
import streamlit as st
import sys
from pathlib import Path

# Asegura que el root del proyecto esté en sys.path para importar 'streamlit_app.*'
_ROOT_DIR = Path(__file__).resolve().parents[1]
if str(_ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(_ROOT_DIR))

from backend.db.config import init_db
from streamlit_app.data import load_all_converted_sales, load_global_range
from streamlit_app.ui_filters import render_filters
from streamlit_app.tabs_overview import render_overview_tab
from streamlit_app.tabs_products import render_products_tab
from streamlit_app.tabs_subs import render_subs_tab
from streamlit_app.tabs_ingest import render_ingest_tab
from streamlit_app.tabs_ads import render_ads_tab


st.set_page_config(page_title="Ventas - Dashboard", layout="wide")
st.title("Dashboard de Ventas")

# Asegura que el esquema está creado (nuevas tablas ads)
try:
    init_db()
except Exception:
    pass

# Autenticación sencilla por contraseña desde .env (DASHBOARD_PASSWORD o APP_PASSWORD)
_ENV_PASSWORD = os.getenv("DASHBOARD_PASSWORD") or os.getenv("APP_PASSWORD")
if _ENV_PASSWORD:
    if "authed" not in st.session_state:
        st.session_state["authed"] = False
    if not st.session_state["authed"]:
        st.info("Esta aplicación está protegida por contraseña.")
        pwd = st.text_input("Contraseña", type="password", key="login_pwd")
        col_l1, col_l2 = st.columns([1,4])
        with col_l1:
            if st.button("Entrar", key="btn_login"):
                if pwd == _ENV_PASSWORD:
                    st.session_state["authed"] = True
                    st.success("Acceso concedido")
                    st.rerun()
                else:
                    st.error("Contraseña incorrecta")
        st.stop()
    else:
        st.sidebar.success("Sesión iniciada")
        if st.sidebar.button("Cerrar sesión", key="btn_logout"):
            st.session_state["authed"] = False
            st.rerun()

min_d, max_d = load_global_range()
default_start = min_d or (date.today() - timedelta(days=30))
default_end = max_d or date.today()

tab_overview, tab_products, tab_subs, tab_ingest, tab_ads = st.tabs(["Visión general", "Productos", "Suscripciones", "Ingesta", "Ads"])

with tab_overview:
    # Filtros solo para Visión general
    start, end, product_filter_val, source_filter_val, grain_effective, group_by_category, view_mode, status_opt = render_filters(default_start, default_end, key_prefix="overview_")
    df_base = load_all_converted_sales(start, end, product_filter_val, source_filter_val, grain_effective, group_by_category, status_opt=status_opt)
    render_overview_tab(df_base, start, end, grain_effective, view_mode, source_filter_val)

with tab_products:
    # Filtros solo para Productos
    start, end, product_filter_val, source_filter_val, grain_effective, group_by_category, view_mode, status_opt = render_filters(default_start, default_end, key_prefix="products_")
    df_base = load_all_converted_sales(start, end, product_filter_val, source_filter_val, grain_effective, group_by_category, status_opt=status_opt)
    render_products_tab(df_base, start, end, grain_effective)

with tab_subs:
    render_subs_tab()

with tab_ingest:
    render_ingest_tab()

with tab_ads:
    # Para Ads, solo necesitamos fechas básicas sin filtros de fuente
    col1, col2 = st.columns(2)
    with col1:
        start = st.date_input("Desde", value=default_start, key="ads_start")
    with col2:
        end = st.date_input("Hasta", value=default_end, key="ads_end")
    render_ads_tab(start, end)

# Depuración rápida del estado de filtros y datos base (solo para desarrollo)
# Nota: Las variables ahora están separadas por página, esto solo muestra el estado de la última página visitada
if False:  # Desactivado por defecto, cambiar a True para depuración
    with st.expander("Depuración rápida (filtros y dataset base)", expanded=False):
        st.write("Nota: Cada página tiene su propio estado separado")
        if "overview_start" in st.session_state:
            st.write("Visión general:", {
                "start": st.session_state.get("overview_start"),
                "end": st.session_state.get("overview_end"),
                "source": st.session_state.get("overview_source"),
            })
        if "products_start" in st.session_state:
            st.write("Productos:", {
                "start": st.session_state.get("products_start"),
                "end": st.session_state.get("products_end"),
                "source": st.session_state.get("products_source"),
            })


