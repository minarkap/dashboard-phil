from __future__ import annotations

from datetime import datetime, timedelta, date
import os
from typing import Dict, Any, Optional

from backend.db.config import db_session, init_db
from backend.db.models import (
    Payment,
    Customer,
    Order,
    OrderItem,
    Product,
    SyncState,
    Refund,
    Subscription,
    LeadsKajabi,
)
from .kajabi_client import KajabiClient


def _parse_iso(dt: Optional[str]) -> Optional[datetime]:
    if not dt:
        return None
    try:
        return datetime.fromisoformat(dt.replace('Z', '+00:00'))
    except Exception:
        return None


def _get_state(key: str) -> Optional[str]:
    with db_session() as s:
        st = (
            s.query(SyncState)
            .filter(SyncState.key == key)
            .order_by(SyncState.id.desc())
            .first()
        )
        return st.value if st else None


def _set_state(key: str, value: str) -> None:
    with db_session() as s:
        st = (
            s.query(SyncState)
            .filter(SyncState.key == key)
            .order_by(SyncState.id.desc())
            .first()
        )
        if st:
            st.value = value
        else:
            s.add(SyncState(key=key, value=value))


def _to_int(val: Any) -> int:
    try:
        if val is None:
            return 0
        if isinstance(val, (int,)):
            return int(val)
        if isinstance(val, float):
            return int(round(val))
        s = str(val).strip()
        if s == "":
            return 0
        return int(round(float(s)))
    except Exception:
        return 0


def run_kajabi_sync(days_back: int = 365, insert_only: bool = True) -> Dict[str, int]:
    """Sincroniza purchases desde Kajabi y upserta en payments y entidades relacionadas.
    Usa cursor en sync_state para incremental; si no hay cursor, usa days_back.
    Si insert_only=False, fuerza backfill de al menos 90 días desde hoy para capturar datos recientes.
    """
    client = KajabiClient()
    cursor_key = "kajabi_purchases_cursor"
    state = _get_state(cursor_key)

    # Ventana por defecto
    start: Optional[date] = None
    end: Optional[date] = None
    
    # Si insert_only=False, forzar backfill de al menos 90 días desde hoy
    # para asegurar que no se pierdan datos recientes
    if not insert_only:
        end = date.today()
        # Usar el máximo entre days_back y 90 días para asegurar datos recientes
        backfill_days = max(days_back, 90)
        start = end - timedelta(days=backfill_days)
    elif not state:
        # Si no hay cursor y es insert_only, usar days_back
        end = date.today()
        start = end - timedelta(days=days_back)

    inserted = 0
    updated = 0
    detected = 0
    max_seen: Optional[datetime] = None

    # Paginación: por defecto recorre TODAS las páginas.
    # Puedes limitar con KAJABI_MAX_PAGES=<n> si necesitas acelerar.
    fast_pages_env = os.getenv("KAJABI_MAX_PAGES")
    max_pages = None
    try:
        if fast_pages_env:
            max_pages = max(1, int(fast_pages_env))
        else:
            max_pages = None  # sin límite por defecto
    except Exception:
        max_pages = None

    for item in client.iter_purchases(start=start, end=end, include="customer,offer", max_pages=max_pages):
        purchase = item.get("purchase") or {}
        attrs = purchase.get("attributes") or {}
        detected += 1

        tx_id = str(purchase.get("id") or "").strip()
        # Calcula importe NETO en minor units teniendo en cuenta posibles descuentos
        gross_candidates = [
            attrs.get("paid_in_cents"),
            attrs.get("grand_total_in_cents"),
            attrs.get("total_in_cents"),
            attrs.get("amount_in_cents"),
            attrs.get("price_in_cents"),
            attrs.get("subtotal_in_cents"),
        ]
        gross_cents = next(( _to_int(v) for v in gross_candidates if _to_int(v) ), 0)
        discount_candidates = [
            attrs.get("discount_total_in_cents"),
            attrs.get("coupon_total_in_cents"),
            attrs.get("discount_in_cents"),
            attrs.get("total_discount_in_cents"),
            attrs.get("promotion_discount_in_cents"),
        ]
        discount_cents = next(( _to_int(v) for v in discount_candidates if _to_int(v) ), 0)
        # Si la API ya da 'paid_in_cents' úsalo como neto. Si no, resta descuentos si existen.
        paid_in_cents = _to_int(attrs.get("paid_in_cents"))
        amount_minor = paid_in_cents if paid_in_cents else max(0, gross_cents - discount_cents)
        currency = (attrs.get("currency") or "EUR").upper()
        paid_at = _parse_iso(attrs.get("effective_start_at") or attrs.get("created_at"))

        # Estado: sólo consideramos "completed/paid/approved" como ventas; resto cancelado/pending/failed
        state_candidates = [
            attrs.get("state"),
            attrs.get("status"),
            attrs.get("payment_status"),
            attrs.get("payment_state"),
        ]
        status_raw = (next((s for s in state_candidates if s), "") or "").lower()
        status = "pending"
        if any(k in status_raw for k in ("paid", "succeeded", "completed", "captured", "approved")):
            status = "completed"
        elif any(k in status_raw for k in ("cancel", "void")):
            status = "cancelled"
        elif "fail" in status_raw:
            status = "failed"
        elif "refund" in status_raw or "refunded" in status_raw:
            status = "refunded"

        customer = item.get("customer") or {}
        cust_attrs = customer.get("attributes") or {}
        email = (cust_attrs.get("email") or "").lower() or None

        offer = item.get("offer") or {}
        offer_attrs = offer.get("attributes") or {}
        product_name = offer_attrs.get("title") or offer_attrs.get("internal_title") or None
        product_sid = str(offer.get("id") or product_name or "").strip()
        # Payload crudo útil para fallback en reporting (nombre de producto, importes, etc.)
        raw_payload: dict[str, Any] = {
            "offer_id": product_sid or None,
            "offer_title": product_name or None,
            "gross_cents": gross_cents,
            "discount_cents": discount_cents,
            "paid_in_cents": paid_in_cents or amount_minor,
            "currency": currency,
            "status_raw": (attrs.get("state") or attrs.get("status") or attrs.get("payment_status") or attrs.get("payment_state") or None),
        }

        if paid_at and (max_seen is None or paid_at > max_seen):
            max_seen = paid_at

        with db_session() as s:
            cust_obj = None
            if email:
                cust_obj = (
                    s.query(Customer)
                    .filter((Customer.source == "kajabi") & (Customer.source_id == email))
                    .order_by(Customer.id.desc())
                    .first()
                )
                if not cust_obj:
                    cust_obj = Customer(source="kajabi", source_id=email, email=email)
                    s.add(cust_obj)

            order_obj = (
                s.query(Order)
                .filter((Order.source == "kajabi") & (Order.source_id == tx_id))
                .order_by(Order.id.desc())
                .first()
            )
            if not order_obj:
                order_obj = Order(source="kajabi", source_id=tx_id, customer_id=cust_obj.id if cust_obj else None, status=status)
                s.add(order_obj)
                s.flush()

            prod_obj = None
            if product_sid:
                prod_obj = (
                    s.query(Product)
                    .filter((Product.source == "kajabi") & (Product.source_id == product_sid))
                    .order_by(Product.id.desc())
                    .first()
                )
                if not prod_obj:
                    prod_obj = Product(source="kajabi", source_id=product_sid, name=product_name or product_sid)
                    s.add(prod_obj)
                    s.flush()

            pay = (
                s.query(Payment)
                .filter((Payment.source == "kajabi") & (Payment.source_payment_id == tx_id))
                .order_by(Payment.id.desc())
                .first()
            )
            if not pay:
                pay = Payment(
                    order_id=order_obj.id,
                    source="kajabi",
                    source_payment_id=tx_id,
                    status=status,
                    amount_original_minor=amount_minor,
                    currency_original=currency,
                    amount_eur=None,
                    net_eur=None,
                    paid_at=paid_at,
                    raw=raw_payload,
                )
                s.add(pay)
                inserted += 1
            else:
                if not insert_only:
                    prev = (
                        pay.status,
                        pay.amount_original_minor,
                        pay.currency_original,
                        pay.paid_at,
                    )
                    pay.status = status or pay.status
                    if amount_minor:
                        pay.amount_original_minor = amount_minor
                    pay.currency_original = currency or pay.currency_original
                    pay.paid_at = paid_at or pay.paid_at
                    # Fusiona raw para conservar trazabilidad mínima (sin sobreescribir datos anteriores útiles)
                    try:
                        merged_raw = dict(pay.raw or {})
                        for k, v in raw_payload.items():
                            if v is not None:
                                merged_raw[k] = v
                        pay.raw = merged_raw
                    except Exception:
                        pay.raw = raw_payload
                    if prev != (pay.status, pay.amount_original_minor, pay.currency_original, pay.paid_at):
                        updated += 1

                # Asegura el enlace del Payment al Order aunque estemos en modo insert_only
                if not pay.order_id and order_obj and order_obj.id:
                    pay.order_id = order_obj.id

            if prod_obj:
                oi = (
                    s.query(OrderItem)
                    .filter((OrderItem.order_id == order_obj.id) & (OrderItem.product_id == prod_obj.id))
                    .order_by(OrderItem.id.desc())
                    .first()
                )
                if not oi:
                    s.add(
                        OrderItem(
                            order_id=order_obj.id,
                            product_id=prod_obj.id,
                            quantity=1,
                            unit_price_original_minor=amount_minor,
                            currency_original=currency,
                            unit_price_eur=None,
                        )
                    )

            # Registrar refund parcial si viene información explícita de reembolso
            refund_candidates = [
                attrs.get("refund_total_in_cents"),
                attrs.get("refunded_total_in_cents"),
                attrs.get("refunded_in_cents"),
            ]
            refund_cents = next(( _to_int(v) for v in refund_candidates if _to_int(v) ), 0)
            if refund_cents and pay:
                # Guarda la pista del importe reembolsado en el raw del pago
                try:
                    rraw = dict(pay.raw or {})
                    rraw["refund_cents"] = refund_cents
                    pay.raw = rraw
                except Exception:
                    pass
                exists_ref = (
                    s.query(Refund)
                    .filter(Refund.payment_id == pay.id, Refund.amount_original_minor == refund_cents)
                    .first()
                )
                if not exists_ref:
                    s.add(
                        Refund(
                            payment_id=pay.id,
                            amount_original_minor=refund_cents,
                            currency_original=currency,
                            amount_eur=None,
                            reason=(attrs.get("refund_reason") or None),
                            refunded_at=paid_at,
                            raw=None,
                        )
                    )
                    # Si hay refund total del importe, marca el pago como 'refunded'
                    if refund_cents >= (pay.amount_original_minor or 0):
                        pay.status = "refunded"

    if max_seen:
        _set_state(cursor_key, max_seen.isoformat())

    return {"detected": detected, "inserted": inserted, "updated": updated}



def run_kajabi_subs_sync() -> Dict[str, int]:
    """Sincroniza suscripciones desde Kajabi API y upserta en subscriptions."""
    client = KajabiClient()
    detected = 0
    inserted = 0
    updated = 0

    for item in client.iter_subscriptions(include="customer,offer"):
        sub = item.get("subscription") or {}
        attrs = sub.get("attributes") or {}
        detected += 1

        sub_id = str(sub.get("id") or "").strip()
        status = (attrs.get("state") or attrs.get("status") or "").lower()
        interval = (attrs.get("interval") or attrs.get("billing_interval") or "").lower()
        currency = (attrs.get("currency") or "EUR").upper()
        amount_minor = _to_int(attrs.get("price_in_cents") or attrs.get("amount_in_cents") or attrs.get("amount_cents"))

        trial_ends_on = _parse_iso(attrs.get("trial_ends_at") or attrs.get("trial_end_at"))
        canceled_on = _parse_iso(attrs.get("canceled_at") or attrs.get("canceled_on"))
        next_payment_date = _parse_iso(attrs.get("next_payment_at") or attrs.get("next_charge_at"))
        created_at = _parse_iso(attrs.get("created_at"))

        customer = item.get("customer") or {}
        cust_attrs = customer.get("attributes") or {}
        email = (cust_attrs.get("email") or "").lower() or None

        with db_session() as s:
            cust_obj = None
            if email:
                cust_obj = (
                    s.query(Customer)
                    .filter((Customer.source == "kajabi") & (Customer.source_id == email))
                    .order_by(Customer.id.desc())
                    .first()
                )
                if not cust_obj:
                    cust_obj = Customer(source="kajabi", source_id=email, email=email)
                    s.add(cust_obj)
                    s.flush()

            subs_obj = (
                s.query(Subscription)
                .filter((Subscription.source == "kajabi") & (Subscription.source_id == sub_id))
                .order_by(Subscription.id.desc())
                .first()
            )
            if not subs_obj:
                s.add(
                    Subscription(
                        source="kajabi",
                        source_id=sub_id,
                        customer_id=cust_obj.id if cust_obj else None,
                        status=status or None,
                        interval=interval or None,
                        amount_original_minor=amount_minor if amount_minor else None,
                        currency_original=currency,
                        trial_ends_on=trial_ends_on,
                        canceled_on=canceled_on,
                        next_payment_date=next_payment_date,
                        created_at=created_at or datetime.utcnow(),
                    )
                )
                inserted += 1
            else:
                prev = (
                    subs_obj.customer_id,
                    subs_obj.status,
                    subs_obj.interval,
                    subs_obj.amount_original_minor,
                    subs_obj.currency_original,
                    subs_obj.trial_ends_on,
                    subs_obj.canceled_on,
                    subs_obj.next_payment_date,
                    subs_obj.created_at,
                )
                subs_obj.customer_id = cust_obj.id if cust_obj else subs_obj.customer_id
                subs_obj.status = status or subs_obj.status
                subs_obj.interval = interval or subs_obj.interval
                subs_obj.amount_original_minor = amount_minor if amount_minor else subs_obj.amount_original_minor
                subs_obj.currency_original = currency or subs_obj.currency_original
                subs_obj.trial_ends_on = trial_ends_on or subs_obj.trial_ends_on
                subs_obj.canceled_on = canceled_on or subs_obj.canceled_on
                subs_obj.next_payment_date = next_payment_date or subs_obj.next_payment_date
                subs_obj.created_at = created_at or subs_obj.created_at
                newv = (
                    subs_obj.customer_id,
                    subs_obj.status,
                    subs_obj.interval,
                    subs_obj.amount_original_minor,
                    subs_obj.currency_original,
                    subs_obj.trial_ends_on,
                    subs_obj.canceled_on,
                    subs_obj.next_payment_date,
                    subs_obj.created_at,
                )
                if newv != prev:
                    updated += 1

    return {"detected": detected, "inserted": inserted, "updated": updated}



def run_kajabi_leads_sync(days_back: int = 365) -> Dict[str, int]:
    """Sincroniza contactos/leads desde Kajabi y guarda UTMs básicos en leads_kajabi.
    Usa cursor en sync_state para incremental; si no hay cursor, usa days_back.
    """
    # Asegura que las tablas existen
    try:
        init_db()
    except Exception:
        pass
    client = KajabiClient()
    cursor_key = "kajabi_leads_cursor"
    state = _get_state(cursor_key)

    start: Optional[date] = None
    end: Optional[date] = None
    if not state:
        end = date.today()
        start = end - timedelta(days=days_back)

    detected = 0
    inserted = 0
    updated = 0
    max_seen: Optional[datetime] = None

    fast_pages_env = os.getenv("KAJABI_MAX_PAGES")
    max_pages = None
    try:
        if fast_pages_env:
            max_pages = max(1, int(fast_pages_env))
        else:
            max_pages = None
    except Exception:
        max_pages = None

    for row in client.iter_contacts(start=start, end=end, max_pages=max_pages):
        detected += 1
        attrs = row.get("attributes") or {}
        cid = str(row.get("id") or "").strip()
        email = (attrs.get("email") or "").lower() or None
        created_at = _parse_iso(attrs.get("created_at") or attrs.get("updated_at")) or datetime.utcnow()

        # Extrae UTMs desde attributes y, si no están, desde custom fields
        utm_source = attrs.get("utm_source") or None
        utm_medium = attrs.get("utm_medium") or None
        utm_campaign = attrs.get("utm_campaign") or None
        utm_content = attrs.get("utm_content") or None
        gclid = attrs.get("gclid") or None
        fbclid = attrs.get("fbclid") or None

        # Parseo genérico de estructuras anidadas típicas (properties/custom_fields)
        def _deep_get(d: Dict[str, Any], key: str) -> Any:
            try:
                if not isinstance(d, dict):
                    return None
                if key in d and d.get(key):
                    return d.get(key)
                for k, v in d.items():
                    if isinstance(v, dict):
                        r = _deep_get(v, key)
                        if r:
                            return r
                return None
            except Exception:
                return None

        for k in ("utm_source","utm_medium","utm_campaign","utm_content","gclid","fbclid"):
            val = _deep_get(attrs, k)
            if k == "utm_source" and not utm_source and val:
                utm_source = val
            elif k == "utm_medium" and not utm_medium and val:
                utm_medium = val
            elif k == "utm_campaign" and not utm_campaign and val:
                utm_campaign = val
            elif k == "utm_content" and not utm_content and val:
                utm_content = val
            elif k == "gclid" and not gclid and val:
                gclid = val
            elif k == "fbclid" and not fbclid and val:
                fbclid = val

        # Si siguen faltando, consulta custom fields por contacto
        if any(x is None for x in (utm_source, utm_medium, utm_campaign, utm_content, gclid, fbclid)):
            try:
                cf_map = client.get_contact_custom_fields(cid)
            except Exception:
                cf_map = {}
            if cf_map:
                utm_source = utm_source or cf_map.get("utm_source") or cf_map.get("UTM Source")
                utm_medium = utm_medium or cf_map.get("utm_medium") or cf_map.get("UTM Medium")
                utm_campaign = utm_campaign or cf_map.get("utm_campaign") or cf_map.get("UTM Campaign")
                utm_content = utm_content or cf_map.get("utm_content") or cf_map.get("UTM Content")
                gclid = gclid or cf_map.get("gclid") or cf_map.get("GCLID")
                fbclid = fbclid or cf_map.get("fbclid") or cf_map.get("FBCLID")

        # Deducción de plataforma a partir de UTM/clids
        platform: Optional[str] = None
        src_l = (utm_source or "").lower()
        if gclid or any(k in src_l for k in ("google", "adwords", "google_ads")):
            platform = "google_ads"
        elif fbclid or any(k in src_l for k in ("facebook", "meta", "instagram")):
            platform = "meta"

        if max_seen is None or created_at > max_seen:
            max_seen = created_at

        with db_session() as s:
            # Evita duplicados simples por email + created_at (si hay email)
            exists_q = s.query(LeadsKajabi).filter(LeadsKajabi.created_at == created_at)
            if email:
                exists_q = exists_q.filter(LeadsKajabi.email == email)
            exists = exists_q.first()
            if not exists:
                s.add(
                    LeadsKajabi(
                        created_at=created_at,
                        email=email,
                        utm_source=utm_source,
                        utm_medium=utm_medium,
                        utm_campaign=utm_campaign,
                        utm_content=utm_content,
                        gclid=gclid,
                        fbclid=fbclid,
                        platform=platform,
                        campaign_id=None,
                        adset_id=None,
                        ad_id=None,
                    )
                )
                inserted += 1
            else:
                # Actualiza campos UTM/plataforma si llegan ahora (sin sobreescribir con None)
                prev = (
                    exists.utm_source,
                    exists.utm_medium,
                    exists.utm_campaign,
                    exists.utm_content,
                    exists.gclid,
                    exists.fbclid,
                    exists.platform,
                )
                exists.utm_source = utm_source or exists.utm_source
                exists.utm_medium = utm_medium or exists.utm_medium
                exists.utm_campaign = utm_campaign or exists.utm_campaign
                exists.utm_content = utm_content or exists.utm_content
                exists.gclid = gclid or exists.gclid
                exists.fbclid = fbclid or exists.fbclid
                exists.platform = platform or exists.platform
                newv = (
                    exists.utm_source,
                    exists.utm_medium,
                    exists.utm_campaign,
                    exists.utm_content,
                    exists.gclid,
                    exists.fbclid,
                    exists.platform,
                )
                if newv != prev:
                    updated += 1

    if max_seen:
        _set_state(cursor_key, max_seen.isoformat())

    return {"detected": detected, "inserted": inserted, "updated": updated}
