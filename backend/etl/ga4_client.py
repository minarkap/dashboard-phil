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
    OrderBy,
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



def _try_run_report(client: BetaAnalyticsDataClient, request: RunReportRequest):
    """Ejecuta un RunReport con manejo genérico de errores."""
    try:
        return client.run_report(request)
    except Exception as e:  # noqa: BLE001
        raise e


def _metric_or_fallback(primary_metric: str, fallback_metric: str, available_metrics: list[str]) -> str:
    """Devuelve la métrica disponible entre primaria y fallback."""
    # Cuando no tenemos el catálogo, intentaremos primero la primaria y, si falla
    # la llamada, reintentaremos con fallback. Esta función queda por simetría.
    return primary_metric if primary_metric in available_metrics else fallback_metric


def fetch_pages_screens(start: date, end: date, limit: int = 1000):
    """
    Devuelve métricas de 'Páginas y pantallas' (GA4) para el rango dado.
    Columnas: pagePath, screenPageViews, screenPageViewsPerUser, userEngagementDuration, keyEvents|conversions
    """
    load_dotenv()
    property_id = os.getenv("GA4_PROPERTY_ID")
    if not property_id:
        raise RuntimeError("Falta GA4_PROPERTY_ID en el entorno")

    client = _build_client()

    def _build_request(use_key_events: bool) -> RunReportRequest:
        metrics = [
            Metric(name="screenPageViews"),
            Metric(name="activeUsers"),
            Metric(name="screenPageViewsPerUser"),
            # No existe averageEngagementTime en API → usar userEngagementDuration (segundos totales)
            Metric(name="userEngagementDuration"),
            Metric(name="keyEvents" if use_key_events else "conversions"),
        ]
        return RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="pagePath")],
            metrics=metrics,
            date_ranges=[DateRange(start_date=start.isoformat(), end_date=end.isoformat())],
            order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="screenPageViews"), desc=True)],
            limit=limit,
        )

    # 1º intento con keyEvents
    try:
        resp = _try_run_report(client, _build_request(use_key_events=True))
        use_key_events = True
    except Exception:
        # 2º intento con conversions (propiedades antiguas sin key events)
        resp = _try_run_report(client, _build_request(use_key_events=False))
        use_key_events = False

    out: list[dict] = []
    for row in resp.rows:
        d = row.dimension_values
        m = row.metric_values
        # Orden de métricas según _build_request
        views = float(m[0].value or 0)
        active_users = float(m[1].value or 0)
        views_per_user = float(m[2].value or 0)
        user_eng_dur = float(m[3].value or 0)  # en segundos totales
        key_events = float(m[4].value or 0)
        out.append(
            {
                "page_path": d[0].value or "/",
                "views": views,
                "active_users": active_users,
                "views_per_user": views_per_user,
                "user_engagement_duration_sec": user_eng_dur,
                "key_events" if use_key_events else "conversions": key_events,
            }
        )
    return out


def fetch_acquisition_channels(start: date, end: date, limit: int = 1000):
    """
    Devuelve métricas de 'Adquisición de tráfico' agrupadas por sessionDefaultChannelGroup.
    Columnas: channel, sessions, engagementRate, keyEvents|conversions
    """
    load_dotenv()
    property_id = os.getenv("GA4_PROPERTY_ID")
    if not property_id:
        raise RuntimeError("Falta GA4_PROPERTY_ID en el entorno")

    client = _build_client()

    def _build_request(use_key_events: bool) -> RunReportRequest:
        metrics = [
            Metric(name="sessions"),
            Metric(name="engagementRate"),
            Metric(name="keyEvents" if use_key_events else "conversions"),
        ]
        return RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="sessionDefaultChannelGroup")],
            metrics=metrics,
            date_ranges=[DateRange(start_date=start.isoformat(), end_date=end.isoformat())],
            order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)],
            limit=limit,
        )

    # 1º intento con keyEvents
    try:
        resp = _try_run_report(client, _build_request(use_key_events=True))
        use_key_events = True
    except Exception:
        # 2º intento con conversions
        resp = _try_run_report(client, _build_request(use_key_events=False))
        use_key_events = False

    out: list[dict] = []
    for row in resp.rows:
        d = row.dimension_values
        m = row.metric_values
        sessions = float(m[0].value or 0)
        engagement_rate = float(m[1].value or 0)  # proporción [0..1]
        key_events = float(m[2].value or 0)
        out.append(
            {
                "channel": d[0].value or "(Unassigned)",
                "sessions": sessions,
                "engagement_rate": engagement_rate,
                "key_events" if use_key_events else "conversions": key_events,
            }
        )
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


def fetch_funnel_metrics(start: date, end: date) -> Dict[str, Any]:
    """
    Devuelve métricas agregadas del funnel de conversión:
    - Sesiones totales
    - Vistas de página totales
    - Eventos clave/conversiones totales
    - Tasa de conversión
    """
    load_dotenv()
    property_id = os.getenv("GA4_PROPERTY_ID")
    if not property_id:
        raise RuntimeError("Falta GA4_PROPERTY_ID en el entorno")

    client = _build_client()

    def _build_request(use_key_events: bool) -> RunReportRequest:
        metrics = [
            Metric(name="sessions"),
            Metric(name="screenPageViews"),
            Metric(name="keyEvents" if use_key_events else "conversions"),
        ]
        return RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[],
            metrics=metrics,
            date_ranges=[DateRange(start_date=start.isoformat(), end_date=end.isoformat())],
        )

    try:
        resp = _try_run_report(client, _build_request(use_key_events=True))
        use_key_events = True
    except Exception:
        resp = _try_run_report(client, _build_request(use_key_events=False))
        use_key_events = False

    if not resp.rows:
        return {
            "sessions": 0,
            "views": 0,
            "key_events": 0,
            "conversion_rate": 0.0,
        }

    row = resp.rows[0]
    m = row.metric_values
    sessions = float(m[0].value or 0)
    views = float(m[1].value or 0)
    key_events = float(m[2].value or 0)
    conversion_rate = (key_events / sessions * 100.0) if sessions > 0 else 0.0

    return {
        "sessions": sessions,
        "views": views,
        "key_events": key_events,
        "conversion_rate": conversion_rate,
    }


def fetch_trends_daily(start: date, end: date) -> Iterable[Dict[str, Any]]:
    """
    Devuelve tendencias diarias de métricas clave:
    date, sessions, views, key_events, engagement_rate
    """
    load_dotenv()
    property_id = os.getenv("GA4_PROPERTY_ID")
    if not property_id:
        raise RuntimeError("Falta GA4_PROPERTY_ID en el entorno")

    client = _build_client()

    def _build_request(use_key_events: bool) -> RunReportRequest:
        metrics = [
            Metric(name="sessions"),
            Metric(name="screenPageViews"),
            Metric(name="engagementRate"),
            Metric(name="keyEvents" if use_key_events else "conversions"),
        ]
        return RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="date")],
            metrics=metrics,
            date_ranges=[DateRange(start_date=start.isoformat(), end_date=end.isoformat())],
            order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"))],
        )

    try:
        resp = _try_run_report(client, _build_request(use_key_events=True))
        use_key_events = True
    except Exception:
        resp = _try_run_report(client, _build_request(use_key_events=False))
        use_key_events = False

    out: list[Dict[str, Any]] = []
    for row in resp.rows:
        d = row.dimension_values
        m = row.metric_values
        out.append({
            "date": date(int(d[0].value[0:4]), int(d[0].value[4:6]), int(d[0].value[6:8])),
            "sessions": float(m[0].value or 0),
            "views": float(m[1].value or 0),
            "engagement_rate": float(m[2].value or 0),
            "key_events" if use_key_events else "conversions": float(m[3].value or 0),
        })
    return out


def fetch_landing_pages(start: date, end: date, limit: int = 50) -> Iterable[Dict[str, Any]]:
    """
    Devuelve landing pages con métricas de conversión:
    landingPage, sessions, bounceRate, keyEvents, conversionRate
    """
    load_dotenv()
    property_id = os.getenv("GA4_PROPERTY_ID")
    if not property_id:
        raise RuntimeError("Falta GA4_PROPERTY_ID en el entorno")

    client = _build_client()

    def _build_request(use_key_events: bool) -> RunReportRequest:
        metrics = [
            Metric(name="sessions"),
            Metric(name="bounceRate"),
            Metric(name="keyEvents" if use_key_events else "conversions"),
        ]
        return RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="landingPage")],
            metrics=metrics,
            date_ranges=[DateRange(start_date=start.isoformat(), end_date=end.isoformat())],
            order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)],
            limit=limit,
        )

    try:
        resp = _try_run_report(client, _build_request(use_key_events=True))
        use_key_events = True
    except Exception:
        resp = _try_run_report(client, _build_request(use_key_events=False))
        use_key_events = False

    out: list[Dict[str, Any]] = []
    for row in resp.rows:
        d = row.dimension_values
        m = row.metric_values
        sessions = float(m[0].value or 0)
        bounce_rate = float(m[1].value or 0)
        key_events = float(m[2].value or 0)
        conversion_rate = (key_events / sessions * 100.0) if sessions > 0 else 0.0

        out.append({
            "landing_page": d[0].value or "/",
            "sessions": sessions,
            "bounce_rate": bounce_rate,
            "key_events" if use_key_events else "conversions": key_events,
            "conversion_rate": conversion_rate,
        })
    return out
