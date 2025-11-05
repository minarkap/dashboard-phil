from __future__ import annotations

from datetime import date
from typing import Optional

from sqlalchemy import text
from datetime import datetime, timedelta

from backend.db.config import db_session
from backend.db.models import Payment, OrderItem, Product, Order

from backend.db.config import engine
from backend.db.config import init_db
from backend.etl.kajabi_sync import run_kajabi_sync


def get_kajabi_last_day() -> Optional[date]:
    q = text("""
        SELECT MAX(paid_at)::date AS d
        FROM payments
        WHERE source = 'kajabi'
    """)
    with engine.begin() as conn:
        row = conn.execute(q).mappings().first()
        return row["d"] if row and row["d"] else None


def purge_kajabi_day(day: date) -> dict:
    """Borra pagos/refunds y líneas de pedido de Kajabi para un día específico.
    Limpia también pedidos sin pagos tras la eliminación.
    """
    stats = {"refunds": 0, "payments": 0, "order_items": 0, "orders": 0}

    with engine.begin() as conn:
        # Identificar pagos de ese día
        q_pay_ids = text(
            """
            SELECT id, order_id FROM payments
            WHERE source = 'kajabi' AND paid_at::date = :d
            """
        )
        pay_rows = list(conn.execute(q_pay_ids, {"d": day}).mappings())
        if not pay_rows:
            return stats

        pay_ids = [r["id"] for r in pay_rows]
        order_ids = list({r["order_id"] for r in pay_rows if r["order_id"] is not None})

        # Refunds
        del_ref = conn.execute(text("DELETE FROM refunds WHERE payment_id = ANY(:ids)"), {"ids": pay_ids})
        stats["refunds"] = del_ref.rowcount or 0

        # Order items
        if order_ids:
            del_oi = conn.execute(text("DELETE FROM order_items WHERE order_id = ANY(:oids)"), {"oids": order_ids})
            stats["order_items"] = del_oi.rowcount or 0

        # Payments
        del_pay = conn.execute(text("DELETE FROM payments WHERE id = ANY(:ids)"), {"ids": pay_ids})
        stats["payments"] = del_pay.rowcount or 0

        # Orders de ese día que se queden sin pagos
        if order_ids:
            del_orders = conn.execute(text(
                """
                DELETE FROM orders o
                WHERE o.id = ANY(:oids)
                  AND o.source = 'kajabi'
                  AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id)
                """
            ), {"oids": order_ids})
            stats["orders"] = del_orders.rowcount or 0

    return stats


def backfill_kajabi_items_from_payment_raw(days_back: int = 30) -> dict:
    """Crea `order_items` para pedidos de Kajabi sin producto usando `Payment.raw`.

    - Busca pagos de Kajabi en los últimos `days_back` días cuyo `order_id` no tenga líneas.
    - Usa `payment.raw.offer_id` / `payment.raw.offer_title` para asociar/crear `Product`.
    - Crea una línea con `unit_price_original_minor = abs(payment.amount_original_minor)` y moneda del pago.
    """
    since = datetime.utcnow() - timedelta(days=days_back)
    stats = {"payments_checked": 0, "items_created": 0, "products_created": 0}
    with db_session() as s:
        q = (
            s.query(Payment)
            .join(Order, Order.id == Payment.order_id)
            .filter(Order.source == "kajabi")
            .filter(Payment.paid_at != None)  # noqa: E711
            .filter(Payment.paid_at >= since)
        )
        pays = q.all()
        # Incluir también pagos de Kajabi huérfanos (sin order_id)
        orphans = (
            s.query(Payment)
            .filter(Payment.order_id == None)  # noqa: E711
            .filter(Payment.source == "kajabi")
            .filter(Payment.paid_at != None)  # noqa: E711
            .filter(Payment.paid_at >= since)
            .all()
        )
        pays.extend(orphans)
        for pay in pays:
            stats["payments_checked"] += 1
            # Asegurar Order si falta
            if not pay.order_id:
                # Usa el propio source_payment_id como order.source_id
                ord = Order(
                    source="kajabi",
                    source_id=(pay.source_payment_id or f"kajabi_{pay.id}"),
                )
                s.add(ord)
                s.flush()
                pay.order_id = ord.id

            # Evitar duplicar líneas
            has_item = s.query(OrderItem).filter(OrderItem.order_id == pay.order_id).first()
            if has_item:
                continue
            raw = pay.raw or {}
            offer_id = (raw.get("offer_id") or "").strip() if isinstance(raw, dict) else ""
            offer_title = (raw.get("offer_title") or "").strip() if isinstance(raw, dict) else ""
            if not (offer_id or offer_title):
                continue
            prod_key = offer_id or offer_title
            prod = (
                s.query(Product)
                .filter((Product.source == "kajabi") & (Product.source_id == prod_key))
                .first()
            )
            if not prod:
                prod = Product(source="kajabi", source_id=prod_key, name=offer_title or prod_key)
                s.add(prod)
                s.flush()
                stats["products_created"] += 1
            s.add(
                OrderItem(
                    order_id=pay.order_id,
                    product_id=prod.id,
                    quantity=1,
                    unit_price_original_minor=abs(int(pay.amount_original_minor or 0)),
                    currency_original=pay.currency_original,
                    unit_price_eur=None,
                )
            )
            stats["items_created"] += 1
    return stats

def main():
    init_db()
    d = get_kajabi_last_day()
    if not d:
        print("No hay pagos de Kajabi para limpiar.")
        return
    stats = purge_kajabi_day(d)
    print(f"Eliminado último día {d}: {stats}")
    # Re-sync para reinsertar con lógica neta
    res = run_kajabi_sync(days_back=365)
    print(f"Re-sync Kajabi: {res}")


if __name__ == "__main__":
    main()


