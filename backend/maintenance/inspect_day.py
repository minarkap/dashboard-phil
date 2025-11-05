from __future__ import annotations

import sys
from datetime import date
from typing import Optional

from sqlalchemy import text
from dotenv import load_dotenv

from backend.db.config import engine, init_db


def _parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except Exception:
        return None


def main():
    load_dotenv()
    init_db()
    d = _parse_date(sys.argv[1]) if len(sys.argv) > 1 else None
    if not d:
        print("Uso: python -m backend.maintenance.inspect_day YYYY-MM-DD")
        return

    q_by_source = text(
        """
        WITH base AS (
            SELECT p.id, p.source, p.amount_original_minor, p.currency_original, p.paid_at::date AS day
            FROM payments p
            WHERE p.paid_at IS NOT NULL
              AND p.paid_at::date = :d
              AND COALESCE(LOWER(p.status),'') NOT IN (
                'refunded','reembolsado','reembolsada','chargeback',
                'cancelled','canceled','cancelado','cancelada',
                'expired','expirada','vencida','vencido',
                'in_analysis','en análisis','en analisis','analisis',
                'initiated','iniciada','pending','pendiente','failed','fallido','fallida'
              )
        ), rf AS (
            SELECT payment_id, SUM(amount_original_minor) AS refund_minor
            FROM refunds GROUP BY payment_id
        )
        SELECT b.source, b.currency_original AS cur,
               COUNT(*) AS n,
               SUM(GREATEST(0, b.amount_original_minor - COALESCE(rf.refund_minor,0))) AS net_minor
        FROM base b
        LEFT JOIN rf ON rf.payment_id = b.id
        GROUP BY b.source, b.currency_original
        ORDER BY b.source, b.currency_original
        """
    )

    q_by_product = text(
        """
        WITH base AS (
            SELECT p.id, p.source, p.amount_original_minor, p.currency_original, p.paid_at::date AS day, o.id AS order_id
            FROM payments p
            LEFT JOIN orders o ON o.id = p.order_id
            WHERE p.paid_at IS NOT NULL
              AND p.paid_at::date = :d
              AND COALESCE(LOWER(p.status),'') NOT IN (
                'refunded','reembolsado','reembolsada','chargeback',
                'cancelled','canceled','cancelado','cancelada',
                'expired','expirada','vencida','vencido',
                'in_analysis','en análisis','en analisis','analisis',
                'initiated','iniciada','pending','pendiente','failed','fallido','fallida'
              )
        ), rf AS (
            SELECT payment_id, SUM(amount_original_minor) AS refund_minor
            FROM refunds GROUP BY payment_id
        ), prod AS (
            SELECT oi.order_id, pr.name AS product_name
            FROM order_items oi JOIN products pr ON pr.id = oi.product_id
        )
        SELECT COALESCE(prod.product_name,'(Sin producto)') AS product,
               b.currency_original AS cur,
               COUNT(*) AS n,
               SUM(GREATEST(0, b.amount_original_minor - COALESCE(rf.refund_minor,0))) AS net_minor
        FROM base b
        LEFT JOIN rf ON rf.payment_id = b.id
        LEFT JOIN prod ON prod.order_id = b.order_id
        GROUP BY product, b.currency_original
        ORDER BY product, b.currency_original
        """
    )

    with engine.begin() as conn:
        src = [dict(r) for r in conn.execute(q_by_source, {"d": d}).mappings().all()]
        prd = [dict(r) for r in conn.execute(q_by_product, {"d": d}).mappings().all()]

    def _fmt(rows):
        return [
            {
                **r,
                "net_major": float(r.get("net_minor") or 0) / 100.0,
            }
            for r in rows
        ]

    print({"by_source": _fmt(src), "by_product": _fmt(prd)})


if __name__ == "__main__":
    main()



