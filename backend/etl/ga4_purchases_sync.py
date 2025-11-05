"""
Sincronización de purchases y revenue desde GA4 Data API.
Guarda en ga4_purchases_daily como fuente de verdad para revenue total.
"""
import os
import logging
from datetime import date, datetime
from typing import Dict, Any
from dotenv import load_dotenv

from backend.db.config import engine
try:
    from backend.db.models import GA4PurchasesDaily
except ImportError:
    # Si el modelo no está disponible, crear la referencia directamente
    GA4PurchasesDaily = None
from backend.etl.ga4_client import _build_client
from google.analytics.data_v1beta import RunReportRequest, DateRange, Dimension, Metric, FilterExpression, Filter
from sqlalchemy.dialects.postgresql import insert

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def detect_platform(source: str | None, medium: str | None, campaign: str | None) -> str:
    """Detecta la plataforma basándose en source/medium/campaign de GA4."""
    source_lower = (source or "").lower()
    medium_lower = (medium or "").lower()
    campaign_lower = (campaign or "").lower()
    
    # Google Ads
    if medium_lower in ('cpc', 'ppc') or 'gclid' in campaign_lower:
        if 'google' in source_lower or 'youtube' in source_lower:
            return 'google_ads'
        return 'google_ads'  # Por defecto si medium es CPC
    
    # Meta Ads
    if medium_lower in ('cpc', 'ppc') or 'fbclid' in campaign_lower:
        if any(s in source_lower for s in ('facebook', 'instagram', 'meta')):
            return 'meta'
        if any(c in campaign_lower for c in ('facebook', 'meta', 'fb', 'instagram')):
            return 'meta'
        return 'meta'  # Por defecto si medium es CPC y no es Google
    
    # Organic
    if medium_lower in ('organic', 'referral', 'none', ''):
        return 'organic'
    
    # Otros paid (si hay campaign pero no es CPC conocido)
    if campaign_lower and medium_lower not in ('organic', 'referral', 'none', ''):
        return 'other_paid'
    
    return 'organic'


def sync_ga4_purchases(start_date: date, end_date: date):
    """
    Sincroniza purchases y revenue desde GA4 Data API.
    Segmenta por date, source, medium, campaign, item_name.
    """
    logger.info(f"Iniciando sincronización de purchases GA4 desde {start_date} hasta {end_date}...")
    
    load_dotenv()
    property_id = os.getenv("GA4_PROPERTY_ID")
    if not property_id:
        raise RuntimeError("Falta GA4_PROPERTY_ID en el entorno")
    
    try:
        client = _build_client()
        
        # Filtro por evento 'purchase'
        event_filter = FilterExpression(
            filter=Filter(
                field_name="eventName",
                string_filter=Filter.StringFilter(value="purchase")
            )
        )
        
        req = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[
                Dimension(name="date"),
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium"),
                Dimension(name="sessionCampaignName"),
                Dimension(name="itemName"),  # Para granularidad de productos
            ],
            metrics=[
                Metric(name="eventCount"),  # Número de purchases
                Metric(name="purchaseRevenue"),  # Revenue en la moneda de GA4
            ],
            date_ranges=[DateRange(start_date=start_date.isoformat(), end_date=end_date.isoformat())],
            dimension_filter=event_filter,
        )
        
        resp = client.run_report(req)
        purchases_data = []
        
        for row in resp.rows:
            d = row.dimension_values
            m = row.metric_values
            
            date_str = d[0].value
            source = d[1].value or None
            medium = d[2].value or None
            campaign = d[3].value or None
            item_name = d[4].value or None
            
            # Obtener métricas
            purchases = int(float(m[0].value or 0))
            revenue_raw = m[1].value or "0"
            try:
                revenue = float(revenue_raw)
            except (ValueError, TypeError):
                revenue = 0.0
            
            # Detectar plataforma
            platform = detect_platform(source, medium, campaign)
            
            # Convertir revenue a EUR (asumiendo que GA4 devuelve en la moneda configurada)
            # TODO: Si GA4 devuelve en otra moneda, necesitaríamos un conversor
            revenue_eur = revenue
            
            purchases_data.append({
                "date": datetime.strptime(date_str, "%Y%m%d"),
                "source": source,
                "medium": medium,
                "campaign": campaign,
                "item_name": item_name,
                "purchases": purchases,
                "revenue_eur": revenue_eur,
                "platform_detected": platform,
            })
        
        if not purchases_data:
            logger.info("No se encontraron purchases en GA4 para el rango de fechas.")
            return
        
        # Upsert en la base de datos
        # Para manejar NULLs en item_name, normalizamos a '' si es None
        for row in purchases_data:
            if row["item_name"] is None:
                row["item_name"] = ""
        
        with engine.begin() as conn:
            stmt = insert(GA4PurchasesDaily).values(purchases_data)
            stmt = stmt.on_conflict_do_update(
                constraint='uq_ga4_purchases_daily',
                set_={
                    'purchases': stmt.excluded.purchases,
                    'revenue_eur': stmt.excluded.revenue_eur,
                    'platform_detected': stmt.excluded.platform_detected,
                }
            )
            conn.execute(stmt)
        
        logger.info(f"Sincronización GA4 completada. Se procesaron {len(purchases_data)} registros.")
        
    except Exception as e:
        logger.error(f"Error durante la sincronización de purchases GA4: {e}", exc_info=True)
        raise

