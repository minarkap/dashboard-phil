"""
Módulo para la sincronización de datos de rendimiento de Google Ads.
"""
import os
from datetime import date, datetime
import logging
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv

from backend.db.config import engine
from backend.db.models import GoogleAdsInsightsDaily

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_google_ads_client():
    """Inicializa y devuelve el cliente de la API de Google Ads usando variables de entorno."""
    try:
        from backend.etl.google_ads_client import _build_client
        load_dotenv()
        client = _build_client()
        return client
    except Exception as e:
        logger.error(f"Error al inicializar el cliente de Google Ads: {e}", exc_info=True)
        return None

def sync_google_ads_insights(start_date: date, end_date: date):
    """
    Sincroniza los datos de rendimiento de Google Ads.
    El customer_id se carga desde las variables de entorno.
    """
    load_dotenv()
    customer_id = os.getenv("GOOGLE_ADS_CUSTOMER_ID")
    if not customer_id:
        raise ValueError("La variable de entorno GOOGLE_ADS_CUSTOMER_ID no está definida en el .env")
    
    # Quitar guiones si los tuviera
    customer_id = customer_id.replace("-", "")

    client = get_google_ads_client()
    if not client:
        return

    ga_service = client.get_service("GoogleAdsService")

    query = f"""
        SELECT
            campaign.id,
            ad_group.id,
            ad_group_ad.ad.id,
            customer.currency_code,
            metrics.conversions,
            metrics.conversions_value,
            segments.date
        FROM ad_group_ad
        WHERE
            segments.date BETWEEN '{start_date.strftime("%Y-%m-%d")}' AND '{end_date.strftime("%Y-%m-%d")}'
    """

    try:
        stream = ga_service.search_stream(customer_id=customer_id, query=query)
        insights_data = []

        for batch in stream:
            for row in batch.results:
                conversions = float(row.metrics.conversions or 0)
                conversions_value = float(row.metrics.conversions_value or 0.0)
                
                # Solo incluir si hay conversiones o valor
                if conversions > 0 or conversions_value > 0:
                    insights_data.append({
                        "date": datetime.strptime(row.segments.date, "%Y-%m-%d"),
                        "account_id": customer_id,
                        "campaign_id": str(row.campaign.id),
                        "adgroup_id": str(row.ad_group.id),
                        "ad_id": str(row.ad_group_ad.ad.id),
                        "conversions": conversions,
                        "conversions_value": conversions_value,
                        "currency": row.customer.currency_code or "EUR",
                    })

        if not insights_data:
            logger.info(f"No se encontraron insights de Google Ads para el cliente {customer_id}.")
            return

        # Upsert en la base de datos
        with engine.begin() as conn:
            stmt = insert(GoogleAdsInsightsDaily).values(insights_data)
            # Necesitamos un índice único para el upsert
            # Lo añadiremos al modelo
            stmt = stmt.on_conflict_do_update(
                index_elements=['date', 'ad_id'],
                set_={
                    'conversions': stmt.excluded.conversions,
                    'conversions_value': stmt.excluded.conversions_value,
                }
            )
            conn.execute(stmt)

        logger.info(f"Sincronizados {len(insights_data)} registros de insights de Google Ads para el cliente {customer_id}.")

    except GoogleAdsException as ex:
        logger.error(f"Error en la petición a la API de Google Ads: {ex}")
    except Exception as e:
        logger.error(f"Error inesperado en la sincronización de Google Ads: {e}")
