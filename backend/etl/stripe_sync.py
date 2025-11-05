from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import select

from backend.db.config import db_session
from backend.db.models import Customer, Order, Payment, Refund, SourceEnum, SyncState, Subscription
from backend.etl.stripe_client import list_charges, list_refunds, list_subscriptions


def _minor_to_major(minor: int, currency: str) -> Decimal:
    # Stripe amounts are in minor units (cents). La conversión a EUR se hará en fase FX.
    return Decimal(minor) / Decimal(100)


def _get_sync_dt(key: str) -> Optional[datetime]:
    with db_session() as s:
        row = (
            s.query(SyncState)
            .filter(SyncState.key == key)
            .order_by(SyncState.id.desc())
            .first()
        )
        if not row:
            return None
        try:
            return datetime.fromisoformat(row.value)
        except Exception:
            return None


def _set_sync_dt(key: str, dt: datetime) -> None:
    with db_session() as s:
        row = (
            s.query(SyncState)
            .filter(SyncState.key == key)
            .order_by(SyncState.id.desc())
            .first()
        )
        if row:
            row.value = dt.isoformat()
        else:
            s.add(SyncState(key=key, value=dt.isoformat()))


def sync_stripe_charges(force_backfill_days: Optional[int] = None):
    """
    Sincroniza charges de Stripe.
    Si force_backfill_days está definido, busca desde esa cantidad de días atrás desde hoy
    en lugar de usar el cursor guardado.
    """
    from datetime import date, timedelta
    
    last = None
    if force_backfill_days is not None:
        # Forzar backfill: buscar desde hace N días
        last = datetime.now() - timedelta(days=force_backfill_days)
    else:
        last = _get_sync_dt("stripe_charges_last")
    
    latest_seen: Optional[datetime] = last

    for ch in list_charges(updated_after=last):
        charge_id = ch["id"]
        customer_id = ch.get("customer")
        email = ch.get("billing_details", {}).get("email") or ch.get("receipt_email")
        currency = (ch.get("currency") or "").upper()
        amount_minor = int(ch.get("amount", 0))
        status = ch.get("status") or "unknown"
        created = datetime.fromtimestamp(ch.get("created", 0))
        paid_at = datetime.fromtimestamp(ch.get("created", 0)) if ch.get("paid") else None

        with db_session() as s:
            customer = None
            customer_source_id = None
            if customer_id and customer_id.startswith("cus_"):
                customer_source_id = customer_id
            elif email:
                customer_source_id = email

            if customer_source_id:
                customer = s.query(Customer).filter(
                    (Customer.source == "stripe") & (Customer.source_id == customer_source_id)
                ).one_or_none()
                if not customer:
                    customer = Customer(
                        source="stripe",
                        source_id=customer_source_id,
                        email=email,
                    )
                    s.add(customer)

            # Order como contenedor lógico por charge
            order = s.query(Order).filter(
                (Order.source == "stripe") & (Order.source_id == charge_id)
            ).one_or_none()
            if not order:
                order = Order(source="stripe", source_id=charge_id, customer_id=customer.id if customer else None)
                s.add(order)
                s.flush()  # asegurar order.id para el Payment

            payment = s.query(Payment).filter(
                (Payment.source == "stripe") & (Payment.source_payment_id == charge_id)
            ).one_or_none()
            if not payment:
                payment = Payment(
                    order_id=order.id,
                    source="stripe",
                    source_payment_id=charge_id,
                    status=status,
                    amount_original_minor=amount_minor,
                    currency_original=currency,
                    amount_eur=None,
                    fee_eur=None,
                    net_eur=None,
                    paid_at=paid_at,
                    raw=ch,
                )
                s.add(payment)
            else:
                payment.status = status
                payment.paid_at = paid_at
                payment.raw = ch

        if not latest_seen or created > latest_seen:
            latest_seen = created

    if latest_seen:
        _set_sync_dt("stripe_charges_last", latest_seen)


def sync_stripe_refunds(force_backfill_days: Optional[int] = None):
    """
    Sincroniza refunds de Stripe.
    Si force_backfill_days está definido, busca desde esa cantidad de días atrás.
    """
    from datetime import timedelta
    
    last = None
    if force_backfill_days is not None:
        last = datetime.now() - timedelta(days=force_backfill_days)
    else:
        last = _get_sync_dt("stripe_refunds_last")
    
    latest_seen: Optional[datetime] = last

    for rf in list_refunds(updated_after=last):
        refund_id = rf["id"]
        charge_id = rf.get("charge")
        amount_minor = int(rf.get("amount", 0))
        currency = (rf.get("currency") or "").upper()
        created = datetime.fromtimestamp(rf.get("created", 0))

        with db_session() as s:
            payment = s.query(Payment).filter(
                (Payment.source == "stripe") & (Payment.source_payment_id == (charge_id or ""))
            ).one_or_none()
            if not payment:
                # Si no existe el charge, saltamos; el próximo ciclo lo recogerá
                continue

            exists = s.query(Refund).filter(Refund.payment_id == payment.id, Refund.amount_original_minor == amount_minor).first()
            if not exists:
                refund = Refund(
                    payment_id=payment.id,
                    amount_original_minor=amount_minor,
                    currency_original=currency,
                    amount_eur=None,
                    reason=rf.get("reason"),
                    refunded_at=created,
                    raw=rf,
                )
                s.add(refund)

        if not latest_seen or created > latest_seen:
            latest_seen = created

    if latest_seen:
        _set_sync_dt("stripe_refunds_last", latest_seen)


def run_stripe_sync(force_backfill_days: Optional[int] = None):
    """
    Ejecuta sincronización completa de Stripe.
    Si force_backfill_days está definido, fuerza un backfill de N días.
    """
    sync_stripe_charges(force_backfill_days)
    # También forzar backfill en refunds y subscriptions si se especifica
    if force_backfill_days:
        sync_stripe_refunds(force_backfill_days)
        sync_stripe_subscriptions(force_backfill_days)
    else:
        sync_stripe_refunds()
        sync_stripe_subscriptions()


def sync_stripe_subscriptions(force_backfill_days: Optional[int] = None):
    """
    Sincroniza subscriptions de Stripe.
    Si force_backfill_days está definido, busca desde esa cantidad de días atrás.
    """
    from datetime import timedelta
    
    last = None
    if force_backfill_days is not None:
        last = datetime.now() - timedelta(days=force_backfill_days)
    else:
        last = _get_sync_dt("stripe_subscriptions_last")
    
    latest_seen: Optional[datetime] = last

    for sub in list_subscriptions(updated_after=last):
        sub_id = sub["id"]
        status = (sub.get("status") or "").lower()
        created = datetime.fromtimestamp(sub.get("created", 0))
        cancel_at = sub.get("cancel_at")
        canceled_at = sub.get("canceled_at")
        next_billing = sub.get("current_period_end")
        customer_obj = sub.get("customer") or {}
        email = customer_obj.get("email") if isinstance(customer_obj, dict) else None

        # Precio principal (primer item) — sin expand, cae a plan/price según disponibilidad
        items = (sub.get("items", {}) or {}).get("data", [])
        unit_minor = None
        currency = None
        interval = None
        if items:
            price_obj = items[0].get("price")
            plan_obj = items[0].get("plan")
            if isinstance(price_obj, dict):
                unit_minor = price_obj.get("unit_amount")
                currency = (price_obj.get("currency") or "").upper() or None
                interval = (price_obj.get("recurring", {}) or {}).get("interval") or None
            elif isinstance(plan_obj, dict):
                unit_minor = plan_obj.get("amount")
                currency = (plan_obj.get("currency") or "").upper() or None
                interval = (plan_obj.get("interval") or None)

        # Tiempos a datetime
        canceled_dt = datetime.fromtimestamp(canceled_at) if canceled_at else None
        next_dt = datetime.fromtimestamp(next_billing) if next_billing else None

        with db_session() as s:
            cust = None
            cust_key = None

            if isinstance(customer_obj, dict):
                cust_key = customer_obj.get("id")
            elif isinstance(customer_obj, str) and customer_obj.startswith("cus_"):
                cust_key = customer_obj
            
            if not cust_key and email:
                cust_key = email

            if cust_key:
                cust = s.query(Customer).filter((Customer.source == "stripe") & (Customer.source_id == cust_key)).one_or_none()
                if not cust:
                    cust = Customer(source="stripe", source_id=cust_key, email=email)
                    s.add(cust)
                    s.flush()

            subs = s.query(Subscription).filter((Subscription.source == "stripe") & (Subscription.source_id == sub_id)).one_or_none()
            if not subs:
                s.add(
                    Subscription(
                        source="stripe",
                        source_id=sub_id,
                        customer_id=cust.id if cust else None,
                        status=status or None,
                        interval=interval or None,
                        amount_original_minor=int(unit_minor) if unit_minor is not None else None,
                        currency_original=currency,
                        trial_ends_on=None,
                        canceled_on=canceled_dt,
                        next_payment_date=next_dt,
                        created_at=created,
                    )
                )
            else:
                subs.customer_id = cust.id if cust else subs.customer_id
                subs.status = status or subs.status
                subs.interval = interval or subs.interval
                subs.amount_original_minor = int(unit_minor) if unit_minor is not None else subs.amount_original_minor
                subs.currency_original = currency or subs.currency_original
                subs.canceled_on = canceled_dt or subs.canceled_on
                subs.next_payment_date = next_dt or subs.next_payment_date

        if not latest_seen or created > latest_seen:
            latest_seen = created

    if latest_seen:
        _set_sync_dt("stripe_subscriptions_last", latest_seen)


