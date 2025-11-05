from __future__ import annotations

import streamlit as st

from datetime import date
import pandas as pd
from sqlalchemy import text, inspect as sqla_inspect
import logging

from backend.db.config import engine, init_db
from .fx import get_fx_timeseries, FALLBACK_FX_RATES
from .utils import normalize_series, date_trunc_alias

# Importar para conversión de moneda
from datetime import date as date_type


def _to_float_es(val) -> float:
    """Convierte string con formato español (coma decimal) a float"""
    if val is None:
        return 0.0
    try:
        s = str(val).strip().replace(".", "").replace(",", ".")
        return float(s)
    except Exception:
        return 0.0


def _safe_upper(s: str | None) -> str:
    """Uppercase seguro que maneja None"""
    return (s or "").strip().upper()


@st.cache_data(ttl=3600)
def load_economics_from_sheets() -> dict:
    """Carga datos económicos desde Google Sheets"""
    logger = logging.getLogger(__name__)
    
    # Verificar que gspread esté disponible
    try:
        import gspread
        from google.oauth2.credentials import Credentials
    except ImportError as e:
        logger.error(f"gspread no está instalado: {e}. Instala con: pip install gspread google-auth")
        return {"_error": f"Módulo gspread no disponible: {e}"}
    
    try:
        import os
        from dotenv import load_dotenv
        load_dotenv()
        
        # Intentar primero con Service Account JSON (nuevo método)
        creds_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON")
        sheet_id = os.getenv("GOOGLE_SHEETS_ECONOMICS_ID") or os.getenv("GSHEETS_SHEET_ID")
        
        # Si no hay Service Account, usar OAuth (método alternativo)
        if not creds_json:
            # Intentar con OAuth credentials
            client_id = os.getenv("GSHEETS_OAUTH_CLIENT_ID") or os.getenv("GOOGLE_SHEETS_OAUTH_CLIENT_ID")
            client_secret = os.getenv("GSHEETS_OAUTH_CLIENT_SECRET") or os.getenv("GOOGLE_SHEETS_OAUTH_CLIENT_SECRET")
            refresh_token = os.getenv("GSHEETS_OAUTH_REFRESH_TOKEN") or os.getenv("GOOGLE_SHEETS_OAUTH_REFRESH_TOKEN")
            
            if client_id and client_secret and refresh_token:
                # Usar OAuth
                try:
                    creds = Credentials(
                        None,
                        refresh_token=refresh_token,
                        token_uri="https://oauth2.googleapis.com/token",
                        client_id=client_id,
                        client_secret=client_secret,
                        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
                    )
                    gc = gspread.authorize(creds)
                except Exception as e:
                    logger.error(f"Error autenticando con OAuth: {e}")
                    return {"_error": f"Error de autenticación OAuth: {e}"}
            else:
                logger.warning("No se encontraron credenciales (ni Service Account JSON ni OAuth)")
                return {"_error": "Faltan credenciales. Configura GOOGLE_SHEETS_CREDENTIALS_JSON o GSHEETS_OAUTH_*"}
        else:
            # Usar Service Account JSON
            try:
                import json
                creds_dict = json.loads(creds_json)
                from google.oauth2.service_account import Credentials as SACredentials
                creds = SACredentials.from_service_account_info(creds_dict, scopes=[
                    "https://www.googleapis.com/auth/spreadsheets.readonly"
                ])
                gc = gspread.authorize(creds)
            except json.JSONDecodeError as e:
                logger.error(f"Error parseando GOOGLE_SHEETS_CREDENTIALS_JSON: {e}")
                return {"_error": f"Error en formato de credenciales JSON: {e}"}
            except Exception as e:
                logger.error(f"Error autenticando con Service Account: {e}")
                return {"_error": f"Error de autenticación Service Account: {e}"}

        if not sheet_id:
            logger.warning("GOOGLE_SHEETS_ECONOMICS_ID o GSHEETS_SHEET_ID no está configurado")
            return {"_error": "Falta el ID de la hoja (GOOGLE_SHEETS_ECONOMICS_ID o GSHEETS_SHEET_ID)"}
        
        # Abrir hoja (intentar sheet específica o sheet1 por defecto)
        try:
            spreadsheet = gc.open_by_key(sheet_id)
            tab_name = os.getenv("GSHEETS_TAB") or os.getenv("GOOGLE_SHEETS_TAB")
            if tab_name:
                try:
                    sheet = spreadsheet.worksheet(tab_name)
                except gspread.WorksheetNotFound:
                    logger.warning(f"Tab '{tab_name}' no encontrada, usando sheet1")
                    sheet = spreadsheet.sheet1
            else:
                sheet = spreadsheet.sheet1
        except gspread.SpreadsheetNotFound:
            logger.error(f"Hoja no encontrada con ID: {sheet_id}")
            return {"_error": f"Hoja no encontrada: {sheet_id}"}
        except Exception as e:
            logger.error(f"Error abriendo hoja: {e}")
            return {"_error": f"Error abriendo hoja: {e}"}
        
        # Leer datos
        try:
            rows = sheet.get_all_records()
        except Exception as e:
            logger.error(f"Error leyendo datos de la hoja: {e}")
            return {"_error": f"Error leyendo datos: {e}"}
        
        if not rows:
            logger.warning("La hoja está vacía o no tiene datos")
            return {"_error": "Hoja vacía"}
        
        # Detectar formato: columnas por mes (ENE, FEB...) o formato tradicional (Mes, EBITDA, Margen)
        first_row = rows[0] if rows else {}
        available_cols = list(first_row.keys())
        
        # Mapeo de meses abreviados a números
        month_map = {
            "ENE": 1, "JAN": 1, "ENERO": 1,
            "FEB": 2, "FEBRERO": 2,
            "MAR": 3, "MARZO": 3,
            "ABR": 4, "APR": 4, "ABRIL": 4,
            "MAY": 5, "MAYO": 5,
            "JUN": 6, "JUNIO": 6,
            "JUL": 7, "JULIO": 7,
            "AGO": 8, "AUG": 8, "AGOSTO": 8,
            "SEPT": 9, "SEP": 9, "SEPTIEMBRE": 9,
            "OCT": 10, "OCTUBRE": 10,
            "NOV": 11, "NOVIEMBRE": 11,
            "DIC": 12, "DEC": 12, "DICIEMBRE": 12,
        }
        
        # Detectar si es formato "concepto por fila, meses por columna"
        month_cols = [col for col in available_cols if col.upper() in month_map or col.upper() in [m.upper() for m in month_map.keys()]]
        has_concept_col = any(col.upper() in ["BALANCE", "CONCEPTO", "CONCEPT", "DESCRIPCION", "DESCRIPTION"] for col in available_cols)
        
        result = {}
        
        if month_cols and has_concept_col:
            # FORMATO 1: Conceptos en filas, meses en columnas (formato BALANCE)
            logger.info("Detectado formato: conceptos por fila, meses por columnas")
            concept_col = next((col for col in available_cols if col.upper() in ["BALANCE", "CONCEPTO", "CONCEPT", "DESCRIPCION", "DESCRIPTION"]), available_cols[0])
            
            ebitda_row = None
            margen_row = None
            
            # Buscar filas de EBITDA y Margen
            for row in rows:
                concept = str(row.get(concept_col, "")).strip()
                concept_upper = concept.upper()
                
                # Buscar fila de EBITDA (sin porcentaje)
                if ebitda_row is None:
                    if "EBITDA" in concept_upper and "%" not in concept and "MARGEN" not in concept_upper and "MARGIN" not in concept_upper:
                        ebitda_row = row
                
                # Buscar fila de Margen (puede ser "MARGEN", "MARGIN", "EBITDA (%)", o cualquier fila con %)
                if margen_row is None:
                    if ("MARGEN" in concept_upper or "MARGIN" in concept_upper) and "%" in concept:
                        margen_row = row
                    elif "EBITDA" in concept_upper and "%" in concept:
                        # "EBITDA (%)" es el margen
                        margen_row = row
                    elif "%" in concept and ("MARGEN" in concept_upper or "MARGIN" in concept_upper or "EBITDA" in concept_upper):
                        margen_row = row
            
            if not ebitda_row:
                return {"_error": "No se encontró fila con EBITDA en la columna de concepto"}
            
            # Leer valores por mes desde las columnas
            for col in month_cols:
                month_name = col.upper()
                month_num = None
                
                # Buscar mes en el mapa
                for key, num in month_map.items():
                    if month_name.startswith(key) or key in month_name:
                        month_num = num
                        break
                
                if month_num and 1 <= month_num <= 12:
                    ebitda_val = ebitda_row.get(col, 0)
                    ebitda_float = _to_float_es(ebitda_val)
                    
                    margen_float = 0.0
                    if margen_row:
                        margen_val = margen_row.get(col, 0)
                        margen_float = _to_float_es(margen_val)
                        # Los valores vienen en porcentaje (ej: 2358 = 23.58%)
                        # Para guardar como decimal (0.2358) que luego se multiplica por 100 en la UI: dividir entre 10000
                        margen_float = margen_float / 10000.0
                    
                    result[month_num] = {
                        "ebitda_eur": ebitda_float,
                        "margin": margen_float,
                    }
        else:
            # FORMATO 2: Formato tradicional (Mes, EBITDA, Margen)
            logger.info("Detectado formato tradicional: Mes, EBITDA, Margen")
            
            def find_column(target_names, available_cols):
                target_lower = [t.lower() for t in target_names]
                for col in available_cols:
                    col_lower = col.lower().strip()
                    if col_lower in target_lower:
                        return col
                    for target in target_lower:
                        if target in col_lower or col_lower in target:
                            return col
                return None
            
            month_col = find_column(["Mes", "Month", "MES", "mes"], available_cols)
            ebitda_col = find_column(["EBITDA", "ebitda", "Ebitda"], available_cols)
            margen_col = find_column(["Margen", "margen", "Margin", "margin", "MARGEN"], available_cols)
            
            missing = []
            if not month_col:
                missing.append("Mes/Month")
            if not ebitda_col:
                missing.append("EBITDA")
            
            if missing:
                logger.error(f"Faltan columnas en la hoja: {missing}")
                logger.info(f"Columnas disponibles: {', '.join(available_cols)}")
                return {"_error": f"Faltan columnas: {', '.join(missing)}. Columnas disponibles: {', '.join(available_cols[:10])}"}
            
            # Procesar filas usando las columnas encontradas
            for idx, row in enumerate(rows, start=2):
                try:
                    month_str = str(row.get(month_col, "")).strip()
                    if not month_str:
                        continue
                    
                    try:
                        month = int(month_str)
                    except ValueError:
                        logger.warning(f"Fila {idx}: '{month_col}' no es un número válido: {month_str}")
                        continue
                    
                    if 1 <= month <= 12:
                        ebitda_val = row.get(ebitda_col, 0)
                        margen_val = row.get(margen_col, 0) if margen_col else 0
                        
                        ebitda_float = _to_float_es(ebitda_val)
                        margen_float = _to_float_es(margen_val)
                        
                        if margen_float > 1.0:
                            margen_float = margen_float / 100.0
                        
                        result[month] = {
                            "ebitda_eur": ebitda_float,
                            "margin": margen_float,
                        }
                    else:
                        logger.warning(f"Fila {idx}: Mes fuera de rango (1-12): {month}")
                except Exception as e:
                    logger.warning(f"Error procesando fila {idx}: {e}")
                    continue
        
        if not result:
            logger.warning("No se encontraron datos válidos en la hoja")
            return {"_error": "No hay datos válidos"}
        
        logger.info(f"Cargados {len(result)} meses desde Google Sheets")
        return result
        
    except Exception as e:
        logger.error(f"Error cargando Google Sheets: {e}", exc_info=True)
        return {"_error": f"Error general: {e}"}


@st.cache_data(ttl=300)
def load_global_range() -> tuple[date | None, date | None]:
    """Devuelve el rango global de fechas disponible en payments"""
    try:
        q = text("""
            SELECT MIN(paid_at::date) AS min_d, MAX(paid_at::date) AS max_d
            FROM payments
            WHERE paid_at IS NOT NULL
        """)
        with engine.begin() as conn:
            row = conn.execute(q).mappings().first()
        if row and row.get("min_d") and row.get("max_d"):
            return row["min_d"], row["max_d"]
    except Exception:
        pass
    return None, None


@st.cache_data(ttl=300)
def load_all_converted_sales(
    start: date,
    end: date,
    product_filter: str | None = None,
    source_filter: str | None = None,
    grain: str = "day",
    group_by_category: bool = False,
    status_opt: str = "completed"
) -> pd.DataFrame:
    """Carga todas las ventas convertidas a EUR con filtros - una fila por pago individual"""
    try:
        # Cargar pagos individuales directamente (no agregados)
        # Incluir amount_original_minor para poder convertir monedas no-EUR
        q = text("""
        SELECT
                p.paid_at::date AS date,
                p.source,
            p.currency_original,
                COALESCE(p.net_eur, p.amount_eur,
                    CASE WHEN UPPER(p.currency_original)='EUR' 
                         THEN p.amount_original_minor/100.0 
                         ELSE NULL END) AS amount_eur,
                p.amount_original_minor,
                COALESCE(pr.name, 'Sin producto') AS product_name,
                'Sin categoría' AS category
        FROM payments p
        LEFT JOIN orders o ON o.id = p.order_id
            LEFT JOIN order_items oi ON oi.order_id = o.id
            LEFT JOIN products pr ON pr.id = oi.product_id
        WHERE p.paid_at IS NOT NULL
          AND p.paid_at::date BETWEEN :start AND :end
              AND (:source_filter IS NULL OR p.source = :source_filter)
              AND (:product_filter IS NULL OR pr.name ILIKE :product_filter)
              AND (
                CASE 
                  WHEN :status_opt = 'Completas + Aprobadas' THEN
                    LOWER(COALESCE(p.status, '')) IN ('completed', 'succeeded', 'approved', 'paid')
                  WHEN :status_opt = 'Solo Completas' THEN
                    LOWER(COALESCE(p.status, '')) IN ('completed', 'succeeded')
                  WHEN :status_opt = 'Todos (excepto canceladas/reembolsadas)' THEN
                    LOWER(COALESCE(p.status, '')) NOT IN ('cancelled', 'canceled', 'refunded', 'reembolsado', 'reembolsada', 'chargeback')
                  WHEN :status_opt = 'Todos (incluyendo pendientes)' THEN
                    LOWER(COALESCE(p.status, '')) NOT IN ('cancelled', 'canceled', 'refunded', 'reembolsado', 'reembolsada', 'chargeback')
                  ELSE
                    LOWER(COALESCE(p.status, '')) IN ('completed', 'succeeded', 'approved', 'paid')
                END
              )
        """)
        
        params = {
            "start": start,
            "end": end,
            "source_filter": source_filter,
            "product_filter": f"%{product_filter}%" if product_filter else None,
            "status_opt": status_opt  # Mantener el texto completo para el CASE
        }
        
        with engine.begin() as conn:
            df = pd.read_sql(q, conn, params=params)

        if df.empty:
            return pd.DataFrame()
        
        # Aplicar FX para monedas no-EUR
        # Obtener todos los días únicos para calcular FX por día
        all_dates = df["date"].unique()
        all_currencies = df["currency_original"].dropna().unique()
        
        # Crear mapa de FX por (fecha, moneda)
        fx_map = {}
        for currency in all_currencies:
            currency_upper = str(currency).upper() if currency else 'EUR'
            if currency_upper == 'EUR':
                for d in all_dates:
                    fx_map[(d, currency)] = 1.0
            else:
                rates = get_fx_timeseries(min(all_dates), max(all_dates), currency_upper)
                for d in all_dates:
                    # Buscar rate para esta fecha específica, o el más cercano
                    if d in rates:
                        fx_map[(d, currency)] = rates[d]
                    elif rates:
                        # Usar el rate más reciente disponible
                        sorted_rates = sorted(rates.items())
                        fx_map[(d, currency)] = sorted_rates[-1][1] if sorted_rates else FALLBACK_FX_RATES.get(currency_upper, 1.0)
                    else:
                        fx_map[(d, currency)] = FALLBACK_FX_RATES.get(currency_upper, 1.0)
        
        # Convertir a EUR
        def convert_to_eur(row):
            # Si ya tenemos amount_eur, usarlo
            if pd.notna(row.get('amount_eur')):
                return float(row.get('amount_eur', 0.0))
            
            # Si no, calcular desde amount_original_minor
            currency = str(row.get('currency_original', 'EUR')).upper()
            amount_minor = int(row.get('amount_original_minor', 0) or 0)
            amount_major = amount_minor / 100.0
            
            if currency == 'EUR':
                return amount_major
            
            # Buscar FX rate para esta fecha y moneda
            date_val = row.get('date')
            rate = fx_map.get((date_val, row.get('currency_original')), FALLBACK_FX_RATES.get(currency, 1.0))
            return amount_major * rate
        
        df['gross_amount_eur'] = df.apply(convert_to_eur, axis=1)
        df['day_agg'] = df['date']  # Para el grain, será transformado después
        df['series'] = df.apply(
            lambda r: r['product_name'] if not group_by_category else r.get('category', 'Sin categoría'),
            axis=1
        )
        
        # Asegurar que currency_original esté en mayúsculas
        df['currency_original'] = df['currency_original'].fillna('EUR').str.upper()
        
        return df
    except Exception as e:
        logging.getLogger(__name__).warning(f"Error cargando ventas: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_ads_costs(start: date, end: date, platform: str) -> pd.DataFrame:
    """Carga costos de ads por plataforma"""
    try:
        # Obtener campaign name de ad_campaigns si existe (columna 'name', no 'campaign_name')
        # La tabla solo tiene: cost_major, impressions, clicks (sin cost_eur ni columnas adicionales)
        q = text("""
            SELECT
                acd.date::date AS day,
                COALESCE(ac.name, acd.campaign_id::text) AS campaign_name,
               acd.campaign_id,
               acd.adset_id,
               acd.ad_id,
                acd.cost_major AS cost_eur,
               acd.impressions,
                acd.clicks
        FROM ad_costs_daily acd
            LEFT JOIN ad_campaigns ac ON ac.platform = acd.platform AND ac.campaign_id = acd.campaign_id
        WHERE acd.date::date BETWEEN :start AND :end
              AND acd.platform = :platform
        """)
        
        with engine.begin() as conn:
            df = pd.read_sql(q, conn, params={"start": start, "end": end, "platform": platform})

        if df.empty:
            return pd.DataFrame()
        
        # Asegurar tipos correctos
        for col in ["cost_eur", "impressions", "clicks"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        
        return df
    except Exception as e:
        logging.getLogger(__name__).warning(f"Error cargando costos de ads: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_leads_by_utm(start: date, end: date, platform: str) -> pd.DataFrame:
    """Carga leads agregados por UTM campaign, atribuidos a plataforma"""
    try:
        # Usar la columna platform si está disponible (más confiable)
        # Si no está disponible, usar filtros de UTM como fallback más permisivos
        logger = logging.getLogger(__name__)
        if platform == "google_ads":
            platform_condition = """
                AND (
                    l.platform = 'google_ads' 
                    OR (l.platform IS NULL AND (
                        l.utm_source ILIKE '%google%'
                        OR l.utm_medium IN ('cpc', 'ppc')
                    ))
                )
            """
        elif platform == "meta":
            platform_condition = """
                AND (
                    l.platform = 'meta'
                    OR (l.platform IS NULL AND (
                        l.utm_source IN ('facebook', 'instagram')
                        OR (l.utm_source IN ('facebook', 'instagram') AND l.utm_medium IN ('cpc', 'ppc'))
                    ))
                )
            """
        else:
            platform_condition = ""
        
        q = text(f"""
        SELECT
                l.utm_campaign AS campaign_name,
                l.created_at::date AS day,
                COUNT(*) AS leads
            FROM leads_kajabi l
            WHERE l.created_at::date BETWEEN :start AND :end
              AND l.utm_campaign IS NOT NULL
              AND l.utm_campaign != ''
              {platform_condition}
            GROUP BY l.utm_campaign, l.created_at::date
        """)
        
        with engine.begin() as conn:
            df = pd.read_sql(q, conn, params={"start": start, "end": end})
        
        if df.empty:
            logger.debug(f"No se encontraron leads para {platform} entre {start} y {end}")
            return pd.DataFrame()
        
        # Agregar por campaign_name (sin día) para matching con costos
        df_agg = df.groupby("campaign_name", as_index=False).agg({
            "leads": "sum"
        })
        
        total_leads = df_agg["leads"].sum() if not df_agg.empty else 0
        logger.debug(f"Cargados {len(df_agg)} campañas con {total_leads} leads totales para {platform}")
        
        return df_agg
    except Exception as e:
        logger.error(f"Error cargando leads para {platform}: {e}", exc_info=True)
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_ads_event_revenue(start: date, end: date, platform: str | None = None) -> pd.DataFrame:
    """Carga revenue de eventos de ads (conversiones/purchases)"""
    try:
        platform_cond = ""
        params = {"start": start, "end": end}
        
        if platform == "google_ads":
            q = text("""
                SELECT
                    gai.date::date AS day,
                    gai.campaign_id,
                    COALESCE(ac.name, gai.campaign_id::text) AS campaign_name,
                    gai.adset_id,
                    gai.ad_id,
                    SUM(gai.conversions) AS purchases,
                    SUM(gai.conversions_value) AS revenue_eur
                FROM google_ads_insights_daily gai
                LEFT JOIN ad_campaigns ac ON ac.platform = 'google_ads' AND ac.campaign_id = gai.campaign_id
                WHERE gai.date::date BETWEEN :start AND :end
                GROUP BY gai.date::date, gai.campaign_id, ac.name, gai.adset_id, gai.ad_id
            """)
        elif platform == "meta":
            # Primero obtener los datos con currency para poder convertir después
            q = text("""
                SELECT
                    mi.date::date AS day,
                    mi.campaign_id,
                    COALESCE(ac.name, mi.campaign_id::text) AS campaign_name,
                    mi.adset_id,
                    mi.ad_id,
                    SUM(mi.purchases) AS purchases,
                    SUM(mi.purchase_value) AS purchase_value_raw,
                    MAX(mi.currency) AS currency  -- Tomar la moneda más común (asumiendo que todas las de un grupo son iguales)
                FROM meta_insights_daily mi
                LEFT JOIN ad_campaigns ac ON ac.platform = 'meta' AND ac.campaign_id = mi.campaign_id
                WHERE mi.date::date BETWEEN :start AND :end
                GROUP BY mi.date::date, mi.campaign_id, ac.name, mi.adset_id, mi.ad_id
            """)
            
            with engine.begin() as conn:
                df = pd.read_sql(q, conn, params=params)
            
            if df.empty:
                return pd.DataFrame()
            
            # Convertir purchase_value a EUR usando FX rates
            def convert_to_eur(row):
                currency = (row.get('currency') or 'EUR').upper()
                value = float(row.get('purchase_value_raw', 0) or 0)
                
                if currency == 'EUR' or not currency:
                    return value
                
                # Obtener FX rate para la fecha
                date_val = row.get('day')
                if date_val:
                    rates = get_fx_timeseries(date_val, date_val, currency)
                    if rates and date_val in rates:
                        return value * rates[date_val]
                    else:
                        # Usar fallback rate
                        fallback = FALLBACK_FX_RATES.get(currency, 1.0)
                        return value * fallback
                
                return value
            
            df['revenue_eur'] = df.apply(convert_to_eur, axis=1)
            df = df.drop(columns=['purchase_value_raw', 'currency'], errors='ignore')
            
            return df
        else:
            # Ambos
            q = text("""
                SELECT
                    COALESCE(gai.date::date, mi.date::date) AS day,
                    COALESCE(gai.campaign_id, mi.campaign_id) AS campaign_id,
                    COALESCE(ac1.name, ac2.name, 
                             COALESCE(gai.campaign_id, mi.campaign_id)::text) AS campaign_name,
                    COALESCE(gai.adset_id, mi.adset_id) AS adset_id,
                    COALESCE(gai.ad_id, mi.ad_id) AS ad_id,
                    COALESCE(SUM(gai.conversions), 0) + COALESCE(SUM(mi.purchases), 0) AS purchases,
                    COALESCE(SUM(gai.conversions_value), 0) + COALESCE(SUM(mi.purchase_value), 0) AS revenue_eur
                FROM (
                    SELECT date::date, campaign_id, adset_id, ad_id, 
                           SUM(conversions) AS conversions, SUM(conversions_value) AS conversions_value
                    FROM google_ads_insights_daily
                    WHERE date::date BETWEEN :start AND :end
                    GROUP BY date::date, campaign_id, adset_id, ad_id
                ) gai
                FULL OUTER JOIN (
                    SELECT date::date, campaign_id, adset_id, ad_id,
                           SUM(purchases) AS purchases, SUM(purchase_value) AS purchase_value
                    FROM meta_insights_daily
                    WHERE date::date BETWEEN :start AND :end
                    GROUP BY date::date, campaign_id, adset_id, ad_id
                ) mi ON gai.date::date = mi.date::date 
                    AND gai.campaign_id = mi.campaign_id 
                    AND COALESCE(gai.adset_id, '') = COALESCE(mi.adset_id, '')
                    AND COALESCE(gai.ad_id, '') = COALESCE(mi.ad_id, '')
                LEFT JOIN ad_campaigns ac1 ON ac1.platform = 'google_ads' AND ac1.campaign_id = COALESCE(gai.campaign_id, mi.campaign_id)
                LEFT JOIN ad_campaigns ac2 ON ac2.platform = 'meta' AND ac2.campaign_id = COALESCE(mi.campaign_id, gai.campaign_id)
                GROUP BY COALESCE(gai.date::date, mi.date::date), 
                         COALESCE(gai.campaign_id, mi.campaign_id),
                         ac1.name, ac2.name,
                         COALESCE(gai.adset_id, mi.adset_id),
                         COALESCE(gai.ad_id, mi.ad_id)
            """)
        
        with engine.begin() as conn:
            df = pd.read_sql(q, conn, params=params)
        
        if df.empty:
            return pd.DataFrame()
        
        # Asegurar tipos
        for col in ["purchases", "revenue_eur"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        
        return df
    except Exception as e:
        logging.getLogger(__name__).warning(f"Error cargando revenue de ads: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_ga_sessions(start: date, end: date) -> pd.DataFrame:
    """Carga sesiones de GA4"""
    try:
        q = text("""
            SELECT 
                date::date AS day,
                source,
                medium,
                campaign,
                SUM(sessions) AS sessions,
                SUM(users) AS users,
                SUM(conversions) AS conversions
            FROM ga_sessions_daily 
        WHERE date::date BETWEEN :start AND :end
            GROUP BY date::date, source, medium, campaign
        """)
        
        with engine.begin() as conn:
            df = pd.read_sql(q, conn, params={"start": start, "end": end})
        
        return df if not df.empty else pd.DataFrame()
    except Exception as e:
        logging.getLogger(__name__).warning(f"Error cargando sesiones GA4: {e}")
        return pd.DataFrame()
        

@st.cache_data(ttl=3600)
def load_sales_global_total(start: date, end: date) -> float:
    """Suma total de ventas en EUR en el rango"""
    try:
        q = text("""
            SELECT SUM(COALESCE(p.net_eur, p.amount_eur,
                CASE WHEN UPPER(p.currency_original)='EUR' 
                     THEN p.amount_original_minor/100.0 
                     ELSE NULL END)) AS total
        FROM payments p
        WHERE p.paid_at IS NOT NULL
          AND p.paid_at::date BETWEEN :start AND :end
              AND LOWER(COALESCE(p.status, '')) = 'completed'
        """)

        with engine.begin() as conn:
            row = conn.execute(q, {"start": start, "end": end}).mappings().first()
        
        return float(row["total"] or 0.0) if row else 0.0
    except Exception as e:
        logging.getLogger(__name__).warning(f"Error calculando ventas globales: {e}")
        return 0.0


@st.cache_data(ttl=3600)
def load_ltv_global() -> float:
    """Carga LTV global desde customer_ltv"""
    try:
        q = text("SELECT SUM(ltv_eur) AS total FROM customer_ltv")
        
        with engine.begin() as conn:
            row = conn.execute(q).mappings().first()
        
        return float(row["total"] or 0.0) if row else 0.0
    except Exception as e:
        logging.getLogger(__name__).warning(f"Error cargando LTV global: {e}")
        return 0.0


# Añadir función para cargar métricas de GA4 relacionadas con checkouts y engagement
@st.cache_data(ttl=300)
def load_ga4_engagement_metrics(start: date, end: date, platform: str | None = None) -> pd.DataFrame:
    """
    Carga métricas de engagement de GA4: checkout iniciado, tiempo de sesión, bounce rate.
    Segmentado por source/medium/campaign para poder atribuir a plataformas de ads.
    """
    try:
        q = text("""
            SELECT
                date::date AS day,
                source,
                medium,
                campaign,
                SUM(sessions) as sessions,
                SUM(users) as users,
                SUM(conversions) as conversions
            FROM ga_sessions_daily 
            WHERE date::date BETWEEN :start AND :end
                AND (:platform IS NULL OR (
                    (:platform = 'google_ads' AND (medium = 'cpc' OR medium = 'ppc' OR source = 'google')) OR
                    (:platform = 'meta' AND (source IN ('facebook', 'instagram') AND medium IN ('cpc', 'ppc')))
                ))
            GROUP BY date::date, source, medium, campaign
        """)
        
        with engine.begin() as conn:
            df = pd.read_sql(q, conn, params={"start": start, "end": end, "platform": platform})
        
        if df.empty:
            return pd.DataFrame()
        
        # Detectar plataforma
        def detect_platform_from_row(row):
            source_lower = str(row.get('source', '')).lower()
            medium_lower = str(row.get('medium', '')).lower()
            if medium_lower in ['cpc', 'ppc'] or 'google' in source_lower:
                return 'google_ads'
            elif source_lower in ['facebook', 'instagram']:
                return 'meta'
            else:
                return 'other'
        
        if platform:
            df['platform'] = platform
        else:
            df['platform'] = df.apply(detect_platform_from_row, axis=1)
        
        # Calcular métricas derivadas
        df['engagement_rate'] = (df['sessions'] / df['sessions'] * 100) if not df['sessions'].empty else 0  # Placeholder
        df['bounce_rate'] = 0  # Placeholder - necesitaríamos bounce_sessions de GA4
        
        return df
    except Exception as e:
        logging.getLogger(__name__).warning(f"Error cargando métricas de engagement GA4: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_ga4_checkout_funnel(start: date, end: date, platform: str | None = None) -> pd.DataFrame:
    """
    Carga datos del funnel de checkout de GA4.
    Por ahora devuelve un placeholder hasta que se añadan las métricas específicas a ga_sessions_daily.
    """
    try:
        # Por ahora solo devolvemos sesiones como placeholder
        df = load_ga_sessions(start, end)
        
        if df.empty:
            return pd.DataFrame()
        
        # Placeholder: cuando tengamos las métricas específicas, las añadiremos aquí
        df['checkout_initiated'] = 0
        df['add_to_cart'] = 0
        
        return df
    except Exception as e:
        logging.getLogger(__name__).warning(f"Error cargando funnel de checkout GA4: {e}")
        return pd.DataFrame()
