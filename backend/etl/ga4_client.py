"""Cliente GA4 (Analytics Data API) con OAuth (Desktop app + refresh token).
Variables esperadas en entorno (o .env):
 - GA4_PROPERTY_ID
 - GA4_OAUTH_CLIENT_ID (o OAUTH_CLIENT_ID)
 - GA4_OAUTH_CLIENT_SECRET (o OAUTH_CLIENT_SECRET)
 - GA4_OAUTH_REFRESH_TOKEN
"""
from __future__ import annotations

from datetime import date
from typing import Iterable, Dict, Any
import os
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    FilterExpression,
    Filter,
)
from google.oauth2.credentials import Credentials
from dotenv import load_dotenv


def _build_client() -> BetaAnalyticsDataClient:
    load_dotenv()
    # Acepta múltiples variantes (token combinado de Ads/GA4 o específicos)
    client_id = (
        os.getenv("GA4_OAUTH_CLIENT_ID")
        or os.getenv("OAUTH_CLIENT_ID")
        or os.getenv("GOOGLE_ADS_CLIENT_ID")
        or os.getenv("GOOGLE_ADS_OAUTH_CLIENT_ID")
    )
    client_secret = (
        os.getenv("GA4_OAUTH_CLIENT_SECRET")
        or os.getenv("OAUTH_CLIENT_SECRET")
        or os.getenv("GOOGLE_ADS_CLIENT_SECRET")
        or os.getenv("GOOGLE_ADS_OAUTH_CLIENT_SECRET")
    )
    refresh_token = (
        os.getenv("GA4_OAUTH_REFRESH_TOKEN")
        or os.getenv("OAUTH_REFRESH_TOKEN")
        or os.getenv("GOOGLE_ADS_REFRESH_TOKEN")
    )
    if not (client_id and client_secret and refresh_token):
        raise RuntimeError("Faltan credenciales OAuth de GA4 en el entorno")
    creds = Credentials(
        None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    return BetaAnalyticsDataClient(credentials=creds)


def fetch_sessions_daily(start: date, end: date) -> Iterable[Dict[str, Any]]:
    load_dotenv()
    property_id = os.getenv("GA4_PROPERTY_ID")
    if not property_id:
        raise RuntimeError("Falta GA4_PROPERTY_ID en el entorno")

    client = _build_client()
    # Importante: no incluir sessionDefaultChannelGroup para evitar duplicados
    # cuando agregamos por (date, source, medium, campaign) que es lo que
    # persistimos en la tabla ga_sessions_daily.
    req = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="date"),
            Dimension(name="source"),
            Dimension(name="medium"),
            Dimension(name="sessionCampaignName"),
        ],
        metrics=[
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="conversions"),
        ],
        date_ranges=[DateRange(start_date=start.isoformat(), end_date=end.isoformat())],
    )
    resp = client.run_report(req)
    out: list[Dict[str, Any]] = []
    for row in resp.rows:
        d = row.dimension_values
        m = row.metric_values
        conv_raw = m[2].value or "0"
        try:
            conv_val = int(float(conv_raw))
        except Exception:
            conv_val = 0
        out.append({
            "date": date(int(d[0].value[0:4]), int(d[0].value[4:6]), int(d[0].value[6:8])),
            "source": d[1].value or None,
            "medium": d[2].value or None,
            "campaign": d[3].value or None,
            "sessions": int(m[0].value or 0),
            "users": int(m[1].value or 0),
            "conversions": conv_val,
        })
    return out



def fetch_purchases_by_day_item(start: date, end: date) -> Iterable[Dict[str, Any]]:
    """Devuelve compras (eventos GA4 'purchase') agregadas por día e item.

    Dimensiones: date, itemName
    Métrica: itemPurchaseQuantity (nº de unidades compradas)
    """
    load_dotenv()
    property_id = os.getenv("GA4_PROPERTY_ID")
    if not property_id:
        raise RuntimeError("Falta GA4_PROPERTY_ID en el entorno")

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
            Dimension(name="itemName"),
        ],
        metrics=[
            Metric(name="itemPurchaseQuantity"),
        ],
        date_ranges=[DateRange(start_date=start.isoformat(), end_date=end.isoformat())],
        dimension_filter=event_filter,
    )

    resp = client.run_report(req)
    out: list[Dict[str, Any]] = []
    for row in resp.rows:
        d = row.dimension_values
        m = row.metric_values
        out.append({
            "date": date(int(d[0].value[0:4]), int(d[0].value[4:6]), int(d[0].value[6:8])),
            "item_name": d[1].value or None,
            "purchases": int(float(m[0].value or 0)),
        })
    return out


def fetch_purchases_by_day_tx(start: date, end: date) -> Iterable[Dict[str, Any]]:
    """Devuelve compras (purchase) agregadas por día y campaña.
    Nota: Se omite transactionId para compatibilidad de métricas/dimensiones con eventCount.
    Dimensiones: date, source, medium, sessionCampaignName
    Métrica: eventCount (nº de eventos 'purchase')
    """
    load_dotenv()
    property_id = os.getenv("GA4_PROPERTY_ID")
    if not property_id:
        raise RuntimeError("Falta GA4_PROPERTY_ID en el entorno")

    client = _build_client()
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
        ],
        metrics=[
            Metric(name="eventCount"),
        ],
        date_ranges=[DateRange(start_date=start.isoformat(), end_date=end.isoformat())],
        dimension_filter=event_filter,
    )
    resp = client.run_report(req)
    out: list[Dict[str, Any]] = []
    for row in resp.rows:
        d = row.dimension_values
        m = row.metric_values
        out.append({
            "date": date(int(d[0].value[0:4]), int(d[0].value[4:6]), int(d[0].value[6:8])),
            "source": d[1].value or None,
            "medium": d[2].value or None,
            "campaign": d[3].value or None,
            "purchases": int(m[0].value or 0),
        })
    return out
