from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path
import concurrent.futures
from datetime import date, timedelta

import pandas as pd
import streamlit as st
from sqlalchemy import text, inspect as sqla_inspect
import streamlit.components.v1 as components
from streamlit import components as _components

from backend.db.config import engine
from .backend_loader import load_backend_modules
from .ingest.helpers import (
    import_via_backend_bytes as _import_via_backend_bytes,
    inline_import_kajabi_transactions as _inline_import_kajabi_transactions,
    inline_import_kajabi_subscriptions as _inline_import_kajabi_subscriptions,
    detect_kajabi_tx_count_from_bytes as _detect_kajabi_tx_count_from_bytes,
    detect_kajabi_subs_count_from_bytes as _detect_kajabi_subs_count_from_bytes,
    extract_kajabi_sub_ids as _extract_kajabi_sub_ids,
    count_existing_kajabi_subscriptions as _count_existing_kajabi_subscriptions,
    detect_hotmart_count_from_bytes as _detect_hotmart_count_from_bytes,
    extract_hotmart_tx_ids as _extract_hotmart_tx_ids,
    count_existing_hotmart_transactions as _count_existing_hotmart_transactions,
)
from backend.etl.meta_client import sync_meta_insights
from backend.etl.google_ads_sync import sync_google_ads_insights
# GA4 purchases sync deshabilitado - no se usa en el dashboard reformulado
# from backend.etl.ga4_purchases_sync import sync_ga4_purchases
try:
    from .upload_server import ensure_server_running
except Exception:
    ensure_server_running = None  # Flask no disponible; desactivamos subida HTTP


# Helpers movidos a streamlit_app/ingest/helpers.py


def render_ingest_tab():
    st.subheader("Ingesta de datos (CSV y Stripe)")

    # Asegura que el esquema está actualizado (nuevas tablas de atribución, etc.)
    try:
        from backend.db.config import init_db as _init_db
        _init_db()
    except Exception:
        pass

    (kajabi_import_tx, kajabi_import_subs, hotmart_import,
     run_stripe_sync, run_ads_sync, run_ga_sync, run_hotmart_sync, run_kajabi_sync, run_attribution_sync, run_kajabi_subs_sync,
     backend_error) = load_backend_modules()

    # Diagnóstico de base de datos: conexión y conteos clave
    with st.expander("Diagnóstico BD (conexion, conteos y estados)", expanded=False):
        try:
            from backend.db.config import engine as _engine
            st.write({
                "DATABASE_URL": os.getenv("DATABASE_URL"),
                "engine_url": str(_engine.url),
                "backend": _engine.url.get_backend_name(),
            })
            with _engine.begin() as conn:
                # ====== Esquema: tablas, columnas, filas y PK/FK ======
                try:
                    ins = sqla_inspect(_engine)
                    tables = sorted(ins.get_table_names())
                except Exception as e:
                    tables = []
                    st.warning(f"Inspector no disponible: {e}")

                if tables:
                    schema_rows: list[dict] = []
                    for t in tables:
                        # Columnas
                        try:
                            cols = ins.get_columns(t)
                        except Exception:
                            cols = []
                        ncols = len(cols)
                        # PK y FKs
                        try:
                            pk_cols = (ins.get_pk_constraint(t) or {}).get("constrained_columns") or []
                        except Exception:
                            pk_cols = []
                        try:
                            fk_list = ins.get_foreign_keys(t) or []
                        except Exception:
                            fk_list = []
                        # Recuento de filas
                        try:
                            n = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
                            nrows = int(n or 0)
                        except Exception as e:
                            nrows = None
                        schema_rows.append({
                            "tabla": t,
                            "columnas": ncols,
                            "filas": nrows,
                            "pk": ", ".join(pk_cols) if pk_cols else "",
                            "fks": len(fk_list),
                        })
                    st.markdown("### Esquema (resumen por tabla)")
                    st.dataframe(pd.DataFrame(schema_rows))

                    # Relaciones (FK)
                    try:
                        edges: list[dict] = []
                        for t in tables:
                            for fk in ins.get_foreign_keys(t) or []:
                                edges.append({
                                    "table": t,
                                    "columns": ", ".join(fk.get("constrained_columns") or []),
                                    "ref_table": fk.get("referred_table"),
                                    "ref_columns": ", ".join(fk.get("referred_columns") or []),
                                    "fk_name": fk.get("name") or "",
                                })
                        st.markdown("### Relaciones (claves foráneas)")
                        if edges:
                            st.table(pd.DataFrame(edges))
                        else:
                            st.info("No se detectaron claves foráneas.")
                    except Exception as e:
                        st.info(f"No se pudieron obtener relaciones: {e}")

                    # Detalle por tabla (columnas)
                    st.markdown("### Columnas por tabla")
                    for t in tables:
                        with st.expander(f"Tabla: {t}", expanded=False):
                            try:
                                cols = ins.get_columns(t)
                            except Exception:
                                cols = []
                            rows_c: list[dict] = []
                            for c in cols:
                                rows_c.append({
                                    "columna": c.get("name"),
                                    "tipo": str(c.get("type")),
                                    "nullable": bool(c.get("nullable")),
                                    "default": c.get("default"),
                                })
                            if rows_c:
                                st.table(pd.DataFrame(rows_c))
                            else:
                                st.info("Sin columnas detectadas (o introspección no disponible).")

                # Conteos por tabla
                tbls = [
                    "customers","products","orders","order_items","payments","refunds","subscriptions","ga_sessions_daily","ad_costs_daily",
                ]
                rows = []
                for t in tbls:
                    try:
                        n = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
                        rows.append({"tabla": t, "filas": int(n or 0)})
                    except Exception as e:
                        rows.append({"tabla": t, "filas": f"error: {e}"})
                st.table(pd.DataFrame(rows))

                # Estados de payments
                try:
                    q = text(
                        """
                        SELECT COALESCE(LOWER(status),'(null)') AS status, COUNT(*) AS n
                        FROM payments
                        GROUP BY 1
                        ORDER BY 2 DESC
                        LIMIT 20
                        """
                    )
                    df_status = pd.read_sql(q, conn)
                    st.markdown("Estados en payments (top 20):")
                    st.table(df_status)
                except Exception as e:
                    st.info(f"No se pudo obtener distribución de estados: {e}")

                # Rango de paid_at
                try:
                    q2 = text("SELECT MIN(paid_at) AS min_paid, MAX(paid_at) AS max_paid FROM payments WHERE paid_at IS NOT NULL")
                    row = conn.execute(q2).mappings().first()
                    st.write({"paid_at_min": row.get("min_paid") if row else None, "paid_at_max": row.get("max_paid") if row else None})
                except Exception as e:
                    st.info(f"No se pudo obtener rango paid_at: {e}")

                # Últimos 5 pagos
                try:
                    q3 = text(
                        """
                        SELECT id, source, status, paid_at, currency_original,
                               ROUND(amount_original_minor/100.0, 2) AS amount_major
                        FROM payments
                        ORDER BY paid_at DESC NULLS LAST, id DESC
                        LIMIT 5
                        """
                    )
                    df_last = pd.read_sql(q3, conn)
                    st.markdown("Últimos pagos:")
                    st.table(df_last)
                except Exception as e:
                    st.info(f"No se pudieron obtener pagos recientes: {e}")
        except Exception as e:
            st.error(f"Diagnóstico BD: {e}")

    # Reparaciones rápidas
    with st.expander("Reparaciones (one‑click)", expanded=False):
        col_r1, col_r2 = st.columns([1,3])
        with col_r1:
            days = st.number_input("Días atrás", min_value=1, max_value=3650, value=30, step=1, help="Ventana para la reparación")
            if st.button("Reparar pedidos Kajabi sin producto", help="Crea líneas de pedido usando Payment.raw (Offer)"):
                try:
                    from backend.maintenance.kajabi_tools import backfill_kajabi_items_from_payment_raw as _fix
                    res = _fix(days_back=int(days))
                    st.success(f"Reparación completada: {res}")
                except Exception as e:
                    st.error(f"Error en reparación: {e}")

    # Botón único: Sincronizar dashboard (incremental)
    col_sync, _ = st.columns([1,4])
    with col_sync:
        if st.button("Sincronizar dashboard", type="primary"):
            msgs = []
            try:
                from dotenv import load_dotenv as _ld
                _ld()
            except Exception:
                pass
            # Limpia cachés (FX y otras) para evitar resultados obsoletos
            try:
                st.cache_data.clear()
            except Exception:
                pass
            try:
                st.cache_resource.clear()
            except Exception:
                pass
            with st.spinner("Sincronizando todo (paralelo e incremental)…"):
                futures: dict[str, concurrent.futures.Future] = {}

                def _submit(ex, name: str, fn, *args, **kwargs):
                    if fn is None:
                        return
                    def _wrap():
                        try:
                            return fn(*args, **kwargs)
                        except Exception as e:
                            return e
                    futures[name] = ex.submit(_wrap)

                # Calcular fechas para conversiones de ads (últimos 30 días)
                end_date_sync = date.today()
                start_date_sync = end_date_sync - timedelta(days=30)

                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
                    # Sincronizaciones básicas
                    # Forzar backfill de 90 días en Stripe para asegurar datos recientes
                    _submit(ex, "stripe", run_stripe_sync, 90)  # force_backfill_days=90
                    _submit(ex, "ga4", run_ga_sync, 30, True)  # days_back=30, insert_only=True
                    _submit(ex, "ads", run_ads_sync, 30, True, True)  # days_back=30
                    _submit(ex, "kajabi", run_kajabi_sync, 365, False)  # days_back=365, insert_only=False (ahora fuerza 90 días mínimo)
                    _submit(ex, "hotmart", run_hotmart_sync, 365, False)  # days_back=365, insert_only=False
                    _submit(ex, "kajabi_subs", run_kajabi_subs_sync)

                    # Conversiones de ads (Meta y Google)
                    try:
                        _submit(ex, "meta_insights", sync_meta_insights, start_date_sync, end_date_sync)
                    except Exception:
                        pass
                    try:
                        _submit(ex, "google_ads_insights", sync_google_ads_insights, start_date_sync, end_date_sync)
                    except Exception:
                        pass

                    # Leads de Kajabi
                    try:
                        import importlib
                        mod = importlib.import_module("backend.etl.kajabi_sync")
                        _run_kajabi_leads_sync = getattr(mod, "run_kajabi_leads_sync", None)
                        if _run_kajabi_leads_sync is None:
                            mod_alt = importlib.import_module("backend.etl.kajabi_leads_sync")
                            _run_kajabi_leads_sync = getattr(mod_alt, "run_kajabi_leads_sync", None)
                        if _run_kajabi_leads_sync:
                            _submit(ex, "kajabi_leads", _run_kajabi_leads_sync, 365)
                    except Exception:
                        pass

                    # Google Sheets (ligero) también en paralelo
                    try:
                        from .data import load_economics_from_sheets as _load_sheets
                        def _sheets_wrap():
                            try:
                                return _load_sheets()
                            except Exception as _e:
                                return _e
                        futures["sheets"] = ex.submit(_sheets_wrap)
                    except Exception:
                        pass

                    # Recoger resultados en orden
                    order = ["stripe", "ga4", "ads", "kajabi", "hotmart", "kajabi_subs", 
                             "meta_insights", "google_ads_insights", "kajabi_leads", "sheets"]
                    for name in order:
                        fut = futures.get(name)
                        if not fut:
                            continue
                        try:
                            res = fut.result(timeout=300)  # Timeout de 5 minutos por tarea
                        except Exception as e:
                            msgs.append(f"{name} timeout/error: {e}")
                            continue
                        if name == "stripe":
                            msgs.append("Stripe OK" if not isinstance(res, Exception) else f"Stripe error: {res}")
                        elif name == "ga4":
                            msgs.append(f"GA4 {int(res or 0)} filas" if not isinstance(res, Exception) else f"GA4 error: {res}")
                        elif name == "ads":
                            msgs.append(f"Ads {int(res or 0)} filas" if not isinstance(res, Exception) else f"Ads error: {res}")
                        elif name == "kajabi":
                            if isinstance(res, Exception):
                                msgs.append(f"Kajabi API error: {res}")
                            elif isinstance(res, dict):
                                msgs.append(f"Kajabi: {res.get('detected', 0)} detectados, {res.get('inserted', 0)} insertados, {res.get('updated', 0)} actualizados")
                            else:
                                msgs.append(f"Kajabi API: {res}")
                        elif name == "hotmart":
                            if isinstance(res, Exception):
                                msgs.append(f"Hotmart API error: {res}")
                            elif isinstance(res, dict):
                                msgs.append(f"Hotmart: {res.get('detected', 0)} detectados, {res.get('inserted', 0)} insertados, {res.get('updated', 0)} actualizados")
                            else:
                                msgs.append(f"Hotmart API: {res}")
                        elif name == "kajabi_subs":
                            msgs.append(f"Kajabi Subs API: {res}" if not isinstance(res, Exception) else f"Kajabi Subs API error: {res}")
                        elif name == "meta_insights":
                            msgs.append("Meta Insights OK" if not isinstance(res, Exception) else f"Meta Insights error: {res}")
                        elif name == "google_ads_insights":
                            msgs.append("Google Ads Insights OK" if not isinstance(res, Exception) else f"Google Ads Insights error: {res}")
                        elif name == "kajabi_leads":
                            msgs.append(f"Kajabi Leads: {res}" if not isinstance(res, Exception) else f"Kajabi Leads error: {res}")
                        elif name == "sheets":
                            if isinstance(res, Exception):
                                msgs.append(f"Google Sheets error: {res}")
                            else:
                                if isinstance(res, dict) and len(res) > 0:
                                    msgs.append(f"Google Sheets OK ({len(res)} meses)")
                                else:
                                    msgs.append("Google Sheets sin datos")

                # Atribución al final (secuencial, depende de otras sincronizaciones)
                if run_attribution_sync is not None:
                    try:
                        res_attr = run_attribution_sync(days_back=30)
                        msgs.append(f"Attribution {res_attr}")
                    except Exception as e:
                        msgs.append(f"Attribution error: {e}")
                
                # Forzar recarga de Google Sheets limpiando su caché específico
                try:
                    from .data import load_economics_from_sheets
                    load_economics_from_sheets.clear()
                except Exception:
                    pass
            st.success(" | ".join(msgs) if msgs else "Sin cambios")

    if backend_error:
        st.warning("La ingesta de datos del backend presenta un error; usaré los importadores en texto/ruta/URL.")

    # Persistencia de avisos entre reruns en esta pestaña
    if "ingest_notices" not in st.session_state:
        st.session_state["ingest_notices"] = []

    def _add_notice(msg: str) -> None:
        st.session_state["ingest_notices"].append(str(msg))

    if st.session_state["ingest_notices"]:
        st.markdown("**Últimos resultados de importación:**")
        for _m in st.session_state["ingest_notices"]:
            st.info(_m)
        colc1, colc2 = st.columns([1,4])
        with colc1:
            if st.button("Limpiar avisos", key="btn_clear_ing_notices"):
                st.session_state["ingest_notices"] = []

    # --- Subida vía servidor HTTP embebido (alternativa a file_uploader) ---
    if ensure_server_running is not None:
        base_url = ensure_server_running()
        with st.expander("Subir archivo (vía HTTP, sin file_uploader)", expanded=False):
            st.markdown(
                f"""
                Puedes subir ficheros con curl o Postman a estos endpoints locales:
                - Kajabi pagos: `POST {base_url}/upload/kajabi_tx` (form-data campo `file`)
                - Kajabi suscripciones: `POST {base_url}/upload/kajabi_subs` (form-data campo `file`)
                - Hotmart pagos: `POST {base_url}/upload/hotmart` (form-data campo `file`)

                Ejemplo:
                ```bash
                curl -F file=@/ruta/transactions.csv {base_url}/upload/kajabi_tx
                ```
                """
            )
            # UI sencilla para usuarios no técnicos: formularios HTML con fetch
            components.html(
                f"""
                <style>
                  .uploader-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; font-family: sans-serif; }}
                  .card {{ border: 1px solid #ddd; border-radius: 8px; padding: 12px; }}
                  .card h4 {{ margin: 0 0 8px 0; font-size: 14px; }}
                  .row {{ display:flex; gap:8px; align-items:center; }}
                  .resp {{ margin-top:8px; font-size: 12px; white-space: pre-wrap; }}
                  button {{ padding: 6px 10px; }}
                </style>
                <div class="uploader-grid">
                  <div class="card">
                    <h4>Kajabi - transactions.csv</h4>
                    <div class="row">
                      <input type="file" id="f_tx" accept=".csv" />
                      <button onclick="upload('f_tx','{base_url}/upload/kajabi_tx','out_tx')">Subir</button>
                    </div>
                    <div id="out_tx" class="resp"></div>
                  </div>
                  <div class="card">
                    <h4>Kajabi - subscriptions.csv</h4>
                    <div class="row">
                      <input type="file" id="f_subs" accept=".csv" />
                      <button onclick="upload('f_subs','{base_url}/upload/kajabi_subs','out_subs')">Subir</button>
                    </div>
                    <div id="out_subs" class="resp"></div>
                  </div>
                  <div class="card">
                    <h4>Hotmart - hotmart_sales.csv</h4>
                    <div class="row">
                      <input type="file" id="f_hot" accept=".csv" />
                      <button onclick="upload('f_hot','{base_url}/upload/hotmart','out_hot')">Subir</button>
                    </div>
                    <div id="out_hot" class="resp"></div>
                  </div>
                </div>
                <script>
                  async function upload(inputId, url, outId) {{
                    const out = document.getElementById(outId);
                    const f = document.getElementById(inputId).files[0];
                    if (!f) {{ out.textContent = 'Selecciona un archivo'; return; }}
                    const fd = new FormData();
                    fd.append('file', f);
                    out.textContent = 'Subiendo...';
                    try {{
                      const res = await fetch(url, {{ method: 'POST', body: fd }});
                      const txt = await res.text();
                      out.textContent = (res.ok ? '✅ ' : '❌ ') + txt;
                    }} catch (e) {{
                      out.textContent = '❌ Error: ' + e;
                    }}
                  }}
                </script>
                """,
                height=280,
            )
    else:
        st.info("Subida HTTP desactivada (Flask no disponible). Usa texto/ruta/URL.")

    # --- Importadores sin file_uploader (texto, ruta, URL) ---
    st.markdown("**Kajabi - transactions.csv (pagos)**")
    col_t1, col_t2, col_t3 = st.columns(3)
    with col_t1:
        pasted = st.text_area("Pega CSV aquí", value="", height=160, key="kajabi_tx_paste")
        if st.button("Importar desde texto", key="btn_kajabi_tx_paste"):
            try:
                if not pasted.strip():
                    st.error("El texto está vacío.")
                else:
                    if kajabi_import_tx and not backend_error:
                        res = _import_via_backend_bytes(pasted.encode("utf-8"), kajabi_import_tx)
                        if isinstance(res, dict):
                            _add_notice(f"Kajabi pagos: procesadas {res.get('detected',0)} filas. Insertadas {res.get('inserted',0)}, actualizadas {res.get('updated',0)}.")
                            st.info(st.session_state["ingest_notices"][-1])
                        else:
                            detected = _detect_kajabi_tx_count_from_bytes(pasted.encode("utf-8"))
                            msg = f"Kajabi pagos (legacy): procesadas {detected} filas. Insertadas {int(res or 0)}, actualizadas 0."
                            _add_notice(msg)
                            st.info(st.session_state["ingest_notices"][-1])
                    else:
                        n = _inline_import_kajabi_transactions(pasted.encode("utf-8"))
                        _add_notice(f"Kajabi pagos (fallback): procesadas {len(pasted.splitlines())-1 if pasted else 0} filas. OK: {n}")
                        st.info(st.session_state["ingest_notices"][-1])
            except Exception as e:
                st.error(f"Error: {e}")
    with col_t2:
        local_path = st.text_input("Ruta local absoluta", value="", key="kajabi_tx_path")
        if st.button("Importar desde ruta", key="btn_kajabi_tx_path"):
            p = Path(local_path).expanduser()
            if not local_path.strip():
                st.error("Introduce una ruta.")
            elif not p.exists():
                st.error(f"No existe la ruta: {p}")
            else:
                with open(p, "rb") as fh:
                    content = fh.read()
                if kajabi_import_tx and not backend_error:
                    res = _import_via_backend_bytes(content, kajabi_import_tx)
                    if isinstance(res, dict):
                        _add_notice(f"Kajabi pagos: procesadas {res.get('detected',0)} filas. Insertadas {res.get('inserted',0)}, actualizadas {res.get('updated',0)}.")
                        st.info(st.session_state["ingest_notices"][-1])
                    else:
                        detected = _detect_kajabi_tx_count_from_bytes(content)
                        msg = f"Kajabi pagos (legacy): procesadas {detected} filas. Insertadas {int(res or 0)}, actualizadas 0."
                        _add_notice(msg)
                        st.info(st.session_state["ingest_notices"][-1])
                else:
                    n = _inline_import_kajabi_transactions(content)
                    detected = _detect_kajabi_tx_count_from_bytes(content)
                    _add_notice(f"Kajabi pagos (fallback): procesadas {detected} filas. OK: {n}")
                    st.info(st.session_state["ingest_notices"][-1])
    with col_t3:
        url = st.text_input("URL remota (CSV)", value="", key="kajabi_tx_url")
        if st.button("Importar desde URL", key="btn_kajabi_tx_url"):
            try:
                import requests
                r = requests.get(url, timeout=30)
                r.raise_for_status()
                content = r.content
                if kajabi_import_tx and not backend_error:
                    res = _import_via_backend_bytes(content, kajabi_import_tx)
                    if isinstance(res, dict):
                        _add_notice(f"Kajabi pagos: procesadas {res.get('detected',0)} filas. Insertadas {res.get('inserted',0)}, actualizadas {res.get('updated',0)}.")
                        st.info(st.session_state["ingest_notices"][-1])
                    else:
                        detected = _detect_kajabi_tx_count_from_bytes(content)
                        msg = f"Kajabi pagos (legacy): procesadas {detected} filas. Insertadas {int(res or 0)}, actualizadas 0."
                        _add_notice(msg)
                        st.info(st.session_state["ingest_notices"][-1])
                else:
                    n = _inline_import_kajabi_transactions(content)
                    detected = _detect_kajabi_tx_count_from_bytes(content)
                    _add_notice(f"Kajabi pagos (fallback): procesadas {detected} filas. OK: {n}")
                    st.info(st.session_state["ingest_notices"][-1])
            except Exception as e:
                st.error(f"Error descargando URL: {e}")

    st.divider()
    st.markdown("**Kajabi - subscriptions.csv (suscripciones)**")
    col_s1, col_s2, col_s3 = st.columns(3)
    with col_s1:
        pasted2 = st.text_area("Pega CSV aquí", value="", height=160, key="kajabi_subs_paste")
        if st.button("Importar desde texto", key="btn_kajabi_subs_paste"):
            if not pasted2.strip():
                st.error("El texto está vacío.")
            else:
                try:
                    if kajabi_import_subs and not backend_error:
                        res = _import_via_backend_bytes(pasted2.encode("utf-8"), kajabi_import_subs)
                        if isinstance(res, dict):
                            _add_notice(f"Kajabi suscripciones: procesadas {res.get('detected',0)} filas. Insertadas {res.get('inserted',0)}, actualizadas {res.get('updated',0)}.")
                            st.info(st.session_state["ingest_notices"][-1])
                        else:
                            ids = _extract_kajabi_sub_ids(pasted2.encode("utf-8"))
                            detected = len(ids)
                            try:
                                existing = _count_existing_kajabi_subscriptions(ids)
                            except Exception:
                                existing = 0
                            inserted_est = max(0, detected - existing)
                            msg = f"Kajabi suscripciones (legacy): procesadas {detected} filas. Insertadas {inserted_est}, actualizadas 0."
                            _add_notice(msg)
                            st.info(st.session_state["ingest_notices"][-1])
                    else:
                        n = _inline_import_kajabi_subscriptions(pasted2.encode("utf-8"))
                        _add_notice(f"Kajabi suscripciones (fallback): OK: {n}")
                        st.info(st.session_state["ingest_notices"][-1])
                except Exception as e:
                    st.error(f"Error importando: {e}")
    with col_s2:
        local_path2 = st.text_input("Ruta local absoluta", value="", key="kajabi_subs_path")
        if st.button("Importar desde ruta", key="btn_kajabi_subs_path"):
            p = Path(local_path2).expanduser()
            if not local_path2.strip():
                st.error("Introduce una ruta.")
            elif not p.exists():
                st.error(f"No existe la ruta: {p}")
            else:
                with open(p, "rb") as fh:
                    content = fh.read()
                if kajabi_import_subs and not backend_error:
                    res = _import_via_backend_bytes(content, kajabi_import_subs)
                    if isinstance(res, dict):
                        _add_notice(f"Kajabi suscripciones: procesadas {res.get('detected',0)} filas. Insertadas {res.get('inserted',0)}, actualizadas {res.get('updated',0)}.")
                        st.info(st.session_state["ingest_notices"][-1])
                    else:
                        ids = _extract_kajabi_sub_ids(content)
                        detected = len(ids)
                        try:
                            existing = _count_existing_kajabi_subscriptions(ids)
                        except Exception:
                            existing = 0
                        inserted_est = max(0, detected - existing)
                        msg = f"Kajabi suscripciones (legacy): procesadas {detected} filas. Insertadas {inserted_est}, actualizadas 0."
                        _add_notice(msg)
                        st.info(st.session_state["ingest_notices"][-1])
                else:
                    n = _inline_import_kajabi_subscriptions(content)
                    ids = _extract_kajabi_sub_ids(content)
                    detected = len(ids)
                    _add_notice(f"Kajabi suscripciones (fallback): procesadas {detected} filas. OK: {n}")
                    st.info(st.session_state["ingest_notices"][-1])
    with col_s3:
        url2 = st.text_input("URL remota (CSV)", value="", key="kajabi_subs_url")
        if st.button("Importar desde URL", key="btn_kajabi_subs_url"):
            if not url2.strip():
                st.error("Introduce una URL.")
            else:
                try:
                    import requests
                    r = requests.get(url2, timeout=30)
                    r.raise_for_status()
                    content = r.content
                except Exception as e:
                    st.error(f"Error descargando URL: {e}")
                else:
                    if kajabi_import_subs and not backend_error:
                        res = _import_via_backend_bytes(content, kajabi_import_subs)
                        if isinstance(res, dict):
                            _add_notice(f"Kajabi suscripciones: procesadas {res.get('detected',0)} filas. Insertadas {res.get('inserted',0)}, actualizadas {res.get('updated',0)}.")
                            st.info(st.session_state["ingest_notices"][-1])
                        else:
                            ids = _extract_kajabi_sub_ids(content)
                            detected = len(ids)
                            try:
                                existing = _count_existing_kajabi_subscriptions(ids)
                            except Exception:
                                existing = 0
                            inserted_est = max(0, detected - existing)
                            msg = f"Kajabi suscripciones (legacy): procesadas {detected} filas. Insertadas {inserted_est}, actualizadas 0."
                            _add_notice(msg)
                            st.info(st.session_state["ingest_notices"][-1])
                    else:
                        n = _inline_import_kajabi_subscriptions(content)
                        ids = _extract_kajabi_sub_ids(content)
                        detected = len(ids)
                        _add_notice(f"Kajabi suscripciones (fallback): procesadas {detected} filas. OK: {n}")
                        st.info(st.session_state["ingest_notices"][-1])

    st.divider()
    st.markdown("**Hotmart - hotmart_sales.csv (pagos)**")
    col_h1, col_h2, col_h3 = st.columns(3)
    with col_h1:
        pasted3 = st.text_area("Pega CSV aquí", value="", height=160, key="hotmart_paste")
        if st.button("Importar desde texto", key="btn_hotmart_paste"):
            if not pasted3.strip():
                st.error("El texto está vacío.")
            elif hotmart_import:
                try:
                    # Guardar a fichero temporal para que el importador backend pueda parsear bien ';'
                    content = pasted3.encode("utf-8")
                    res = _import_via_backend_bytes(content, hotmart_import)
                    if isinstance(res, dict):
                        _add_notice(f"Hotmart pagos: procesadas {res.get('detected',0)} filas. Insertadas {res.get('inserted',0)}, actualizadas {res.get('updated',0)}.")
                        st.info(st.session_state["ingest_notices"][-1])
                    else:
                        ids = _extract_hotmart_tx_ids(content)
                        detected = len(ids)
                        try:
                            existing = _count_existing_hotmart_transactions(ids)
                        except Exception:
                            existing = 0
                        inserted_est = max(0, detected - existing)
                        msg = f"Hotmart (legacy): procesadas {detected} filas. Insertadas {inserted_est}, actualizadas 0."
                        _add_notice(msg)
                        st.info(st.session_state["ingest_notices"][-1])
                except Exception as e:
                    st.error(f"Error importando: {e}")
            else:
                st.warning("Importador backend de Hotmart no disponible.")
    with col_h2:
        local_path3 = st.text_input("Ruta local absoluta", value="", key="hotmart_path")
        if st.button("Importar desde ruta", key="btn_hotmart_path"):
            p = Path(local_path3).expanduser()
            if not local_path3.strip():
                st.error("Introduce una ruta.")
            elif not p.exists():
                st.error(f"No existe la ruta: {p}")
            elif hotmart_import:
                with open(p, "rb") as fh:
                    content = fh.read()
                res = _import_via_backend_bytes(content, hotmart_import)
                if isinstance(res, dict):
                    _add_notice(f"Hotmart pagos: procesadas {res.get('detected',0)} filas. Insertadas {res.get('inserted',0)}, actualizadas {res.get('updated',0)}.")
                    st.info(st.session_state["ingest_notices"][-1])
                else:
                    ids = _extract_hotmart_tx_ids(content)
                    detected = len(ids)
                    try:
                        existing = _count_existing_hotmart_transactions(ids)
                    except Exception:
                        existing = 0
                    inserted_est = max(0, detected - existing)
                    msg = f"Hotmart (legacy): procesadas {detected} filas. Insertadas {inserted_est}, actualizadas 0."
                    _add_notice(msg)
                    st.info(st.session_state["ingest_notices"][-1])
            else:
                st.warning("Importador backend de Hotmart no disponible.")
    with col_h3:
        url3 = st.text_input("URL remota (CSV)", value="", key="hotmart_url")
        if st.button("Importar desde URL", key="btn_hotmart_url"):
            if not url3.strip():
                st.error("Introduce una URL.")
            elif hotmart_import:
                try:
                    import requests
                    r = requests.get(url3, timeout=30)
                    r.raise_for_status()
                    res = _import_via_backend_bytes(r.content, hotmart_import)
                    if isinstance(res, dict):
                        _add_notice(f"Hotmart pagos: procesadas {res.get('detected',0)} filas. Insertadas {res.get('inserted',0)}, actualizadas {res.get('updated',0)}.")
                        st.info(st.session_state["ingest_notices"][-1])
                    else:
                        ids = _extract_hotmart_tx_ids(r.content)
                        detected = len(ids)
                        try:
                            existing = _count_existing_hotmart_transactions(ids)
                        except Exception:
                            existing = 0
                        inserted_est = max(0, detected - existing)
                        msg = f"Hotmart (legacy): procesadas {detected} filas. Insertadas {inserted_est}, actualizadas 0."
                        _add_notice(msg)
                        st.info(st.session_state["ingest_notices"][-1])
                except Exception as e:
                    st.error(f"Error descargando URL: {e}")
            else:
                st.warning("Importador backend de Hotmart no disponible.")

    # Se han eliminado los botones individuales de sincronización.
    st.divider()
    st.markdown("**Kajabi - leads.csv (contactos con UTMs)**")
    col_l1, col_l2, col_l3 = st.columns(3)
    with col_l1:
        pasted_leads = st.text_area("Pega CSV de leads aquí", value="", height=160, key="kajabi_leads_paste")
        if st.button("Importar leads desde texto", key="btn_kajabi_leads_paste"):
            if not pasted_leads.strip():
                st.error("El texto está vacío.")
            else:
                try:
                    from backend.etl.kajabi_csv_leads import process_kajabi_leads_csv
                    result = process_kajabi_leads_csv(pasted_leads)
                    if result['success']:
                        _add_notice(f"Leads Kajabi: {result['message']} (procesados: {result['processed']}, errores: {result['errors']})")
                        st.success(st.session_state["ingest_notices"][-1])
                    else:
                        _add_notice(f"Error leads Kajabi: {result['message']}")
                        st.error(st.session_state["ingest_notices"][-1])
                except Exception as e:
                    st.error(f"Error procesando CSV de leads: {e}")
    with col_l2:
        local_path_leads = st.text_input("Ruta local absoluta", value="", key="kajabi_leads_path")
        if st.button("Importar leads desde ruta", key="btn_kajabi_leads_path"):
            p = Path(local_path_leads).expanduser()
            if not local_path_leads.strip():
                st.error("Introduce una ruta.")
            elif not p.exists():
                st.error(f"No existe la ruta: {p}")
            else:
                try:
                    with open(p, "r", encoding="utf-8") as fh:
                        content = fh.read()
                    from backend.etl.kajabi_csv_leads import process_kajabi_leads_csv
                    result = process_kajabi_leads_csv(content)
                    if result['success']:
                        _add_notice(f"Leads Kajabi: {result['message']} (procesados: {result['processed']}, errores: {result['errors']})")
                        st.success(st.session_state["ingest_notices"][-1])
                    else:
                        _add_notice(f"Error leads Kajabi: {result['message']}")
                        st.error(st.session_state["ingest_notices"][-1])
                except Exception as e:
                    st.error(f"Error procesando CSV de leads: {e}")
    with col_l3:
        url_leads = st.text_input("URL remota (CSV)", value="", key="kajabi_leads_url")
        if st.button("Importar leads desde URL", key="btn_kajabi_leads_url"):
            if not url_leads.strip():
                st.error("Introduce una URL.")
            else:
                try:
                    import requests
                    r = requests.get(url_leads, timeout=30)
                    r.raise_for_status()
                    content = r.text
                    from backend.etl.kajabi_csv_leads import process_kajabi_leads_csv
                    result = process_kajabi_leads_csv(content)
                    if result['success']:
                        _add_notice(f"Leads Kajabi: {result['message']} (procesados: {result['processed']}, errores: {result['errors']})")
                        st.success(st.session_state["ingest_notices"][-1])
                    else:
                        _add_notice(f"Error leads Kajabi: {result['message']}")
                        st.error(st.session_state["ingest_notices"][-1])
                except Exception as e:
                    st.error(f"Error descargando URL: {e}")

    st.divider()
    st.markdown("**Webhook de Kajabi**")
    col_webhook1, col_webhook2 = st.columns([1,1])
    with col_webhook1:
        if st.button("Iniciar Webhook Kajabi", key="btn_start_webhook"):
            try:
                import subprocess
                import threading
                import time
                
                # Iniciar webhook en background
                def start_webhook():
                    subprocess.run([
                        "python3", "start_webhook.py"
                    ], cwd="/Users/JoseSanchis/Projects/phil_hugo/dashboard")
                
                # Ejecutar en thread separado
                webhook_thread = threading.Thread(target=start_webhook, daemon=True)
                webhook_thread.start()
                
                # Esperar un poco para que inicie
                time.sleep(2)
                
                st.success("Webhook iniciado en puerto 5001")
                st.info("Endpoint: http://localhost:5001/webhook/kajabi/contact")
                st.info("Estado: http://localhost:5001/webhook/kajabi/contact (GET)")
                
            except Exception as e:
                st.error(f"Error iniciando webhook: {e}")
    
    with col_webhook2:
        if st.button("Verificar Webhook", key="btn_check_webhook"):
            try:
                import requests
                response = requests.get("http://localhost:5001/webhook/kajabi/contact", timeout=5)
                if response.status_code == 200:
                    st.success("Webhook funcionando correctamente")
                    st.json(response.json())
                else:
                    st.error(f"Webhook no responde: {response.status_code}")
            except Exception as e:
                st.error(f"Webhook no disponible: {e}")

    st.divider()
    st.markdown("**Acciones avanzadas (API)**")
    col_api1, col_api2 = st.columns([1,1])
    with col_api1:
        # Botón eliminado - ahora está en "Sincronizar dashboard"
        st.info("Los leads de Kajabi ahora se sincronizan con el botón 'Sincronizar dashboard'.")
    with col_api2:
        if st.button("Calcular LTV global", key="btn_compute_ltv"):
            try:
                import importlib
                mod2 = importlib.import_module("backend.etl.ltv_compute")
                try:
                    mod2 = importlib.reload(mod2)
                except Exception:
                    pass
                _compute_ltv = getattr(mod2, "compute_and_persist_ltv_global", None)
                if _compute_ltv is None:
                    raise RuntimeError("compute_and_persist_ltv_global no está disponible")
                res = _compute_ltv()
                st.success(f"LTV actualizado: {res}")
            except Exception as e:
                st.error(f"Error calculando LTV: {e}")

    st.divider()
    st.markdown("### Nota sobre Sincronización de Conversiones")
    st.info("Las conversiones de Meta Ads y Google Ads ahora se sincronizan automáticamente con el botón 'Sincronizar dashboard' (últimos 30 días).")
