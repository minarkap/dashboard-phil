from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import select

from backend.db.config import db_session
from backend.db.models import Customer, Order, Payment, Refund, SyncState, Product, OrderItem
from backend.etl.hotmart_client import list_transactions


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


def _parse_dt_any(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    s = str(value).strip()
    # ISO con o sin Z
    for v in (s, s.replace("Z", "+00:00")):
        try:
            return datetime.fromisoformat(v)
        except Exception:
            pass
    # Epoch (ms o s)
    if s.isdigit():
        try:
            iv = int(s)
            if iv > 10_000_000_000:  # ms
                return datetime.fromtimestamp(iv / 1000, tz=timezone.utc)
            return datetime.fromtimestamp(iv, tz=timezone.utc)
        except Exception:
            return None
    return None


def _first(*vals):
    for v in vals:
        if v is not None and v != "":
            return v
    return None


def sync_hotmart_transactions(days_back: int = 365, insert_only: bool = True) -> dict:
    """
    Sincroniza transacciones de Hotmart. Si no hay cursor, hace backfill de days_back días.
    Si insert_only=False, fuerza backfill de al menos 90 días desde hoy para capturar datos recientes.
    """
    last = _get_sync_dt("hotmart_tx_last")
    # Si estamos en modo update, forzamos un backfill para asegurarnos de que todo se actualiza.
    if not insert_only:
        # Forzar backfill de al menos 90 días desde hoy para asegurar datos recientes
        backfill_days = max(days_back, 90)
        last = datetime.utcnow() - timedelta(days=backfill_days)
    elif last is None:
        # Backfill si es la primera vez
        last = datetime.utcnow() - timedelta(days=days_back)
    latest_seen: Optional[datetime] = last

    detected = 0
    inserted = 0
    updated = 0

    for tx in list_transactions(updated_after=last):
        detected += 1
        # Campos robustos
        tx_id = _first(
            tx.get("transaction"),
            (tx.get("purchase") or {}).get("transaction"),
            tx.get("id"),
            (tx.get("purchase") or {}).get("id"),
        )
        product_info = tx.get("product") or {}
        product_name = product_info.get("name")
        product_id = str(product_info.get("id", ""))
        
        # Estado
        status_raw = _first(
            tx.get("status"),
            tx.get("transaction_status"),
            (tx.get("purchase") or {}).get("status"),
        ) or ""
        status = str(status_raw).lower()

        # Email
        buyer = tx.get("buyer") or {}
        customer = tx.get("customer") or {}
        email = _first(
            buyer.get("email"),
            customer.get("email"),
            tx.get("customer_email"),
            tx.get("email"),
        )

        # Moneda
        currency = _first(
            (tx.get("currency") or "").upper() or None,
            (tx.get("currency_code") or "").upper() or None,
            ((tx.get("purchase") or {}).get("currency_code") or "").upper() or None,
            (((tx.get("price") or {}).get("currency_code")) or "").upper() if tx.get("price") else None,
        ) or "EUR"

        # Importe
        amount_candidate = _first(
            tx.get("value"),
            tx.get("amount"),
            (tx.get("price") or {}).get("value"),
            (tx.get("purchase") or {}).get("amount"),
        )
        try:
            amount_minor = int(round(float(amount_candidate or 0) * 100))
        except Exception:
            amount_minor = 0

        # Fechas
        created_str = _first(
            tx.get("approved_date"),
            tx.get("purchase_date"),
            tx.get("date_created"),
            tx.get("approvedDate"),
            tx.get("purchaseDate"),
            tx.get("creationDate"),
            tx.get("lastUpdateDate"),
            tx.get("approved_at"),
        )
        created = _parse_dt_any(created_str)

        with db_session() as s:
            customer_obj = None
            if email:
                customer_obj = (
                    s.query(Customer)
                    .filter((Customer.source == "hotmart") & (Customer.source_id == email))
                    .order_by(Customer.id.desc())
                    .first()
                )
                if not customer_obj:
                    customer_obj = Customer(source="hotmart", source_id=email, email=email)
                    s.add(customer_obj)

            order = (
                s.query(Order)
                .filter((Order.source == "hotmart") & (Order.source_id == tx_id))
                .order_by(Order.id.desc())
                .first()
            )
            if not order:
                order = Order(source="hotmart", source_id=tx_id, customer_id=customer_obj.id if customer_obj else None, status=status)
                s.add(order)
                s.flush()

            product_obj = None
            if product_name:
                product_obj = (
                    s.query(Product)
                    .filter_by(source="hotmart", source_id=product_id)
                    .first()
                )
                if not product_obj:
                    product_obj = Product(
                        source="hotmart", source_id=product_id, name=product_name
                    )
                    s.add(product_obj)
                    s.flush()

            if product_obj:
                order_item = (
                    s.query(OrderItem)
                    .filter_by(order_id=order.id, product_id=product_obj.id)
                    .first()
                )
                if not order_item:
                    order_item = OrderItem(
                        order_id=order.id,
                        product_id=product_obj.id,
                        quantity=1,
                        unit_price_original_minor=amount_minor,
                        currency_original=currency,
                    )
                    s.add(order_item)
                    s.flush()

            payment = (
                s.query(Payment)
                .filter((Payment.source == "hotmart") & (Payment.source_payment_id == tx_id))
                .order_by(Payment.id.desc())
                .first()
            )
            if not payment:
                payment = Payment(
                    order_id=order.id,
                    source="hotmart",
                    source_payment_id=tx_id,
                    status=status,
                    amount_original_minor=amount_minor,
                    currency_original=currency,
                    paid_at=created,
                    raw=tx,
                )
                s.add(payment)
                inserted += 1
            else:
                if not insert_only:
                    prev = (
                        payment.status,
                        payment.paid_at,
                        payment.amount_original_minor,
                        payment.currency_original,
                    )
                    payment.status = status or payment.status
                    payment.paid_at = created or payment.paid_at
                    if amount_minor:
                        payment.amount_original_minor = amount_minor
                    if currency:
                        payment.currency_original = currency
                    payment.raw = tx
                    if prev != (
                        payment.status,
                        payment.paid_at,
                        payment.amount_original_minor,
                        payment.currency_original,
                    ):
                        updated += 1

        if created and (not latest_seen or created > latest_seen):
            latest_seen = created

    if latest_seen:
        _set_sync_dt("hotmart_tx_last", latest_seen)

    return {"detected": detected, "inserted": inserted, "updated": updated}


def run_hotmart_sync(days_back: int = 365, insert_only: bool = True) -> dict:
    return sync_hotmart_transactions(days_back=days_back, insert_only=insert_only)


