"""Cliente Meta Ads (Facebook Marketing API) para costes diarios por campaña.
Variables entorno requeridas:
 - META_ACCESS_TOKEN (System User long-lived token)
 - META_AD_ACCOUNT_ID (formato act_XXXXXXXX)
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable, Dict, Any
import os
import requests
from dotenv import load_dotenv
import json
import logging
from datetime import date, datetime

# Inicialización del logger
logger = logging.getLogger(__name__)

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount


def fetch_costs_daily(start: date, end: date) -> Iterable[Dict[str, Any]]:
    load_dotenv()
    token = os.getenv("META_ACCESS_TOKEN")
    account = os.getenv("META_AD_ACCOUNT_ID")
    if not token or not account:
        raise RuntimeError("Faltan META_ACCESS_TOKEN o META_AD_ACCOUNT_ID")

    # Meta usa YYYY-MM-DD y time_increment=1 para dividir por días
    params = {
        "access_token": token,
        # Pedimos al nivel más granular (anuncio) para segmentar por adset/ad
        "level": "ad",
        # Graph API requiere el objeto JSON, no un dict plano en querystring
        "time_range": json.dumps({
            "since": start.isoformat(),
            "until": end.isoformat(),
        }),
        "time_increment": 1,
        # Añadimos nombres y jerarquía completa
        "fields": "date_start,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,spend,impressions,clicks,account_currency",
        "limit": 5000,
    }
    url = f"https://graph.facebook.com/v18.0/{account}/insights"
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json().get("data", [])
    out: list[Dict[str, Any]] = []
    for row in data:
        try:
            d = date.fromisoformat(row.get("date_start"))
        except Exception:
            continue
        out.append({
            "date": d,
            "platform": "meta",
            "account_id": account,
            "campaign_id": row.get("campaign_id"),
            "campaign_name": row.get("campaign_name"),
            "adset_id": row.get("adset_id"),
            "adset_name": row.get("adset_name"),
            "ad_id": row.get("ad_id"),
            "ad_name": row.get("ad_name"),
            "currency": (row.get("account_currency") or "EUR").upper(),
            "cost_major": float(row.get("spend") or 0.0),
            "impressions": int(row.get("impressions") or 0),
            "clicks": int(row.get("clicks") or 0),
            "reach": int(row.get("reach") or 0),
            "frequency": float(row.get("frequency") or 0.0),
            "ctr": float(row.get("ctr") or 0.0),
            "cpm": float(row.get("cpm") or 0.0),
            "cpc": float(row.get("cpc") or 0.0),
            "quality_score": row.get("quality_score"),
        })
    return out


def fetch_purchases_daily(start: date, end: date) -> Iterable[Dict[str, Any]]:
    """Devuelve compras/valor por día a nivel anuncio con ventanas de atribución consistentes.

    Estrategia estable: fijamos `action_attribution_windows` a "7d_click,1d_view" (configurable por env
    META_ATTR_WINDOWS) y NO usamos breakdowns para que Meta ya agregue sin duplicados.
    Suma de tipos que contengan 'purchase' en `actions` y `action_values`.
    """
    load_dotenv()
    token = os.getenv("META_ACCESS_TOKEN")
    account = os.getenv("META_AD_ACCOUNT_ID")
    if not token or not account:
        raise RuntimeError("Faltan META_ACCESS_TOKEN o META_AD_ACCOUNT_ID")

    params = {
        "access_token": token,
        "level": "ad",
        "time_range": json.dumps({
            "since": start.isoformat(),
            "until": end.isoformat(),
        }),
        "time_increment": 1,
        # Pedimos acciones/valores agregadas por la(s) ventana(s) solicitadas
        "fields": (
            "date_start,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,"
            "actions,action_values,account_currency,attribution_setting"
        ),
        # Ventanas de atribución agregadas (sin breakdown) para evitar doble conteo
        "action_attribution_windows": os.getenv("META_ATTR_WINDOWS", "7d_click,1d_view"),
        "limit": 5000,
    }
    url = f"https://graph.facebook.com/v18.0/{account}/insights"
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json().get("data", [])
    out: list[Dict[str, Any]] = []
    for row in data:
        try:
            d = date.fromisoformat(row.get("date_start"))
        except Exception:
            continue
        actions = row.get("actions") or []
        action_values = row.get("action_values") or []
        purchases = 0
        value = 0.0
        for a in actions:
            atype = (a.get("action_type") or "").lower()
            if "purchase" in atype:
                try:
                    purchases += int(float(a.get("value") or 0))
                except Exception:
                    pass
        for av in action_values:
            atype = (av.get("action_type") or "").lower()
            if "purchase" in atype:
                try:
                    value += float(av.get("value") or 0.0)
                except Exception:
                    pass
        out.append({
            "date": d,
            "platform": "meta",
            "account_id": account,
            "campaign_id": row.get("campaign_id"),
            "campaign_name": row.get("campaign_name"),
            "adset_id": row.get("adset_id"),
            "adset_name": row.get("adset_name"),
            "ad_id": row.get("ad_id"),
            "ad_name": row.get("ad_name"),
            "currency": (row.get("account_currency") or "EUR").upper(),
            "purchases": purchases,
            "value": value,
            "attribution_setting": row.get("attribution_setting"),
            # nota de ventana aplicada a toda la consulta
            "_attr_windows": params.get("action_attribution_windows"),
        })
    return out


def sync_meta_insights(start_date: date, end_date: date):
    """
    Sincroniza los datos de rendimiento (insights) de Meta Ads a nivel de anuncio y día.
    Obtiene compras y valor de compra y los guarda en la tabla MetaInsightsDaily.
    """
    from backend.db.config import engine
    from backend.db.models import MetaInsightsDaily
    from sqlalchemy.dialects.postgresql import insert
    from facebook_business.exceptions import FacebookRequestError

    logger.info("Iniciando sincronización de insights de Meta Ads...")
    
    load_dotenv()
    token = os.getenv("META_ACCESS_TOKEN")
    account = os.getenv("META_AD_ACCOUNT_ID")
    if not token or not account:
        raise RuntimeError("Faltan META_ACCESS_TOKEN o META_AD_ACCOUNT_ID")
    
    try:
        # Inicializar API de Facebook
        FacebookAdsApi.init(access_token=token)
        ad_account = AdAccount(fbid=account, api=FacebookAdsApi.get_default_api())
        
        insights_data = []
        
        # Obtener insights directamente desde el ad account
        params = {
            "level": "ad",
            "action_breakdowns": ["action_type"],
            "fields": [
                "ad_id",
                "campaign_id",
                "adset_id",
                "date_start",
                "actions",
                "action_values",
                "account_currency"
            ],
            "time_range": {
                "since": start_date.strftime("%Y-%m-%d"),
                "until": end_date.strftime("%Y-%m-%d"),
            },
            "time_increment": 1,
        }
        
        insights = ad_account.get_insights(params=params)
        
        for insight in insights:
            try:
                purchases = 0
                purchase_value = 0.0
                
                # Capturar TODOS los tipos de purchase que Meta puede tener
                # Incluye: purchase, omni_purchase, web_in_store_purchase, 
                # onsite_web_purchase, offsite_conversion.fb_pixel_purchase, etc.
                
                if "actions" in insight:
                    for action in insight["actions"]:
                        action_type = action.get("action_type", "").lower()
                        # Capturar cualquier acción que contenga "purchase"
                        if "purchase" in action_type:
                            try:
                                purchases += int(float(action.get("value", 0)))
                            except (ValueError, TypeError):
                                pass
                
                if "action_values" in insight:
                    # IMPORTANTE: Meta puede reportar múltiples tipos de purchase con valores que se solapan
                    # Por ejemplo: purchase, omni_purchase, web_in_store_purchase pueden duplicar valores
                    # Estrategia: usar solo el valor más específico o el mayor, evitando duplicados
                    purchase_values_by_type = {}
                    for action_value in insight["action_values"]:
                        action_type = action_value.get("action_type", "").lower()
                        if "purchase" in action_type:
                            try:
                                value = float(action_value.get("value", 0.0))
                                # Guardar el valor más alto por tipo de purchase
                                if action_type not in purchase_values_by_type or value > purchase_values_by_type[action_type]:
                                    purchase_values_by_type[action_type] = value
                            except (ValueError, TypeError):
                                pass
                    
                    # Si hay múltiples tipos de purchase, usar solo el mayor para evitar duplicados
                    # O mejor: usar solo 'purchase' si está disponible, sino el mayor
                    if purchase_values_by_type:
                        if "purchase" in purchase_values_by_type:
                            purchase_value = purchase_values_by_type["purchase"]
                        else:
                            # Usar el mayor valor
                            purchase_value = max(purchase_values_by_type.values())
                
                # Incluir TODOS los registros (incluso con 0) para tener datos completos
                # El filtro de solo purchases > 0 se puede hacer después si es necesario
                insights_data.append({
                    "date": datetime.strptime(insight["date_start"], "%Y-%m-%d"),
                    "account_id": account,
                    "campaign_id": insight.get("campaign_id", ""),
                    "adset_id": insight.get("adset_id", ""),
                    "ad_id": insight.get("ad_id", ""),
                    "purchases": purchases,
                    "purchase_value": purchase_value,
                    "currency": insight.get("account_currency", "EUR"),
                })
            except Exception as e:
                logger.warning(f"Error procesando insight individual: {e}")
                continue
        
        if not insights_data:
            logger.info("No se encontraron insights de Meta Ads para sincronizar.")
            return

        # Upsert en la base de datos
        with engine.begin() as conn:
            stmt = insert(MetaInsightsDaily).values(insights_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['date', 'ad_id'],
                set_={
                    'purchases': stmt.excluded.purchases,
                    'purchase_value': stmt.excluded.purchase_value,
                }
            )
            conn.execute(stmt)
        
        logger.info(f"Sincronización de Meta Ads completada. Se procesaron {len(insights_data)} registros de insights.")

    except FacebookRequestError as e:
        logger.error(f"Error en la API de Facebook: {e}")
        raise
    except Exception as e:
        logger.error(f"Error inesperado durante la sincronización de insights de Meta: {e}", exc_info=True)
        raise

