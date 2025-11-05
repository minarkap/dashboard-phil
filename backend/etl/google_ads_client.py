"""Cliente Google Ads usando OAuth Desktop + refresh token.
Variables esperadas en .env:
 - GOOGLE_ADS_DEVELOPER_TOKEN
 - GOOGLE_ADS_LOGIN_CUSTOMER_ID
 - GOOGLE_ADS_CUSTOMER_ID
 - GOOGLE_ADS_CLIENT_ID / GOOGLE_ADS_CLIENT_SECRET (pueden ser los de GA4)
 - GOOGLE_ADS_REFRESH_TOKEN
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Iterable, Dict, Any
import os
from google.ads.googleads.client import GoogleAdsClient
from google.oauth2.credentials import Credentials
from dotenv import load_dotenv


def _build_client() -> GoogleAdsClient:
    load_dotenv()
    devtoken = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN")
    # Si el usuario del refresh token no está en la MCC, usar el propio CUSTOMER_ID como login header
    login = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID") or os.getenv("GOOGLE_ADS_CUSTOMER_ID")
    # Acepta variantes de nombres en .env
    client_id = (
        os.getenv("GOOGLE_ADS_CLIENT_ID")
        or os.getenv("GOOGLE_ADS_OAUTH_CLIENT_ID")
        or os.getenv("GA4_OAUTH_CLIENT_ID")
        or os.getenv("OAUTH_CLIENT_ID")
    )
    client_secret = (
        os.getenv("GOOGLE_ADS_CLIENT_SECRET")
        or os.getenv("GOOGLE_ADS_OAUTH_CLIENT_SECRET")
        or os.getenv("GA4_OAUTH_CLIENT_SECRET")
        or os.getenv("OAUTH_CLIENT_SECRET")
    )
    refresh_token = os.getenv("GOOGLE_ADS_REFRESH_TOKEN")
    if not all([devtoken, client_id, client_secret, refresh_token]):
        raise RuntimeError("Faltan credenciales de Google Ads en .env")
    creds = Credentials(
        None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=["https://www.googleapis.com/auth/adwords"],
    )
    return GoogleAdsClient(credentials=creds, developer_token=devtoken, login_customer_id=login)


def fetch_costs_daily(start: date, end: date) -> Iterable[Dict[str, Any]]:
    load_dotenv()
    customer_id = os.getenv("GOOGLE_ADS_CUSTOMER_ID")
    if not customer_id:
        raise RuntimeError("Falta GOOGLE_ADS_CUSTOMER_ID en .env")

    client = _build_client()
    ga_service = client.get_service("GoogleAdsService")

    query = f"""
        SELECT
          segments.date,
          customer.currency_code,
          campaign.id,
          campaign.name,
          ad_group.id,
          ad_group.name,
          ad_group_ad.ad.id,
          ad_group_ad.ad.name,
          metrics.cost_micros,
          metrics.clicks,
          metrics.impressions,
          metrics.average_cpc,
          ad_group_ad.ad.type
        FROM ad_group_ad
        WHERE segments.date BETWEEN '{start.isoformat()}' AND '{end.isoformat()}'
    """
    stream = ga_service.search_stream(customer_id=customer_id, query=query)
    out: list[Dict[str, Any]] = []
    for batch in stream:
        for row in batch.results:
            d = row.segments.date
            
            out.append({
                "date": date.fromisoformat(d),
                "platform": "google_ads",
                "account_id": customer_id,
                "campaign_id": str(row.campaign.id),
                "campaign_name": row.campaign.name,
                "adset_id": str(row.ad_group.id),
                "adset_name": row.ad_group.name,
                "ad_id": str(row.ad_group_ad.ad.id),
                "ad_name": row.ad_group_ad.ad.name,
                "ad_type": str(getattr(row.ad_group_ad.ad, 'type_', None) or 'UNKNOWN'),
                "currency": row.customer.currency_code,
                "cost_major": float(row.metrics.cost_micros) / 1_000_000.0,
                "impressions": int(row.metrics.impressions),
                "clicks": int(row.metrics.clicks),
                "average_cpc": float(getattr(row.metrics, 'average_cpc', 0) or 0) / 1_000_000.0,
            })
    return out


def fetch_conversions_daily(start: date, end: date) -> Iterable[Dict[str, Any]]:
    """Devuelve conversiones y valor por día a nivel anuncio, desglosado por acción de conversión.

    Incluye detalles de la acción (nombre, tipo, categoría) para entender el origen del tracking
    (importadas de GA4, etiqueta web, offline, etc. según configuración de la cuenta).
    """
    load_dotenv()
    customer_id = os.getenv("GOOGLE_ADS_CUSTOMER_ID")
    if not customer_id:
        raise RuntimeError("Falta GOOGLE_ADS_CUSTOMER_ID en .env")

    client = _build_client()
    ga_service = client.get_service("GoogleAdsService")

    query = f"""
        SELECT
          segments.date,
          customer.currency_code,
          campaign.id,
          campaign.name,
          ad_group.id,
          ad_group.name,
          ad_group_ad.ad.id,
          ad_group_ad.ad.name,
          segments.conversion_action,
          conversion_action.name,
          conversion_action.category,
          conversion_action.type,
          metrics.conversions,
          metrics.conversions_value
        FROM ad_group_ad
        WHERE segments.date BETWEEN '{start.isoformat()}' AND '{end.isoformat()}'
          AND conversion_action.category IN (PURCHASE)
    """
    stream = ga_service.search_stream(customer_id=customer_id, query=query)
    out: list[Dict[str, Any]] = []
    for batch in stream:
        for row in batch.results:
            d = row.segments.date
            out.append({
                "date": date.fromisoformat(d),
                "platform": "google_ads",
                "account_id": customer_id,
                "campaign_id": str(row.campaign.id),
                "campaign_name": row.campaign.name,
                "adset_id": str(row.ad_group.id),
                "adset_name": row.ad_group.name,
                "ad_id": str(row.ad_group_ad.ad.id),
                "ad_name": row.ad_group_ad.ad.name,
                "currency": row.customer.currency_code,
                "conversion_action": row.segments.conversion_action,
                "conversion_action_name": getattr(row.conversion_action, "name", None),
                "conversion_action_category": getattr(row.conversion_action, "category", None),
                "conversion_action_type": getattr(row.conversion_action, "type", None),
                "conversions": float(row.metrics.conversions or 0),
                "conversions_value": float(row.metrics.conversions_value or 0.0),
            })
    return out

