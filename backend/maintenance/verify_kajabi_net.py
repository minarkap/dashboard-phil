from __future__ import annotations

import sys
from datetime import date
from typing import Optional

from dotenv import load_dotenv
from sqlalchemy import text

from backend.db.config import engine, init_db


def _parse_date_arg(arg: Optional[str]) -> Optional[date]:
    if not arg:
        return None
    try:
        return date.fromisoformat(arg)
    except Exception:
        return None


def get_last_kajabi_day() -> Optional[date]:
    q = text("SELECT MAX(paid_at)::date AS d FROM payments WHERE source='kajabi'")
    with engine.begin() as conn:
        row = conn.execute(q).mappings().first()
        return row["d"] if row and row["d"] else None


def compute_kajabi_net(target_day: date) -> dict:
    """Devuelve el neto por moneda para pagos de Kajabi en target_day restando refunds."""
    q = text(
        """
        WITH p AS (
            SELECT id, amount_original_minor, currency_original
            FROM payments
            WHERE source='kajabi' AND paid_at::date = :d AND COALESCE(LOWER(status),'') NOT IN (
                'refunded','reembolsado','reembolsada','chargeback',
                'cancelled','canceled','cancelado','cancelada',
                'expired','expirada','vencida','vencido',
                'in_analysis','en análisis','en analisis','analisis',
                'initiated','iniciada','pending','pendiente','failed','fallido','fallida'
            )
        ), rf AS (
            SELECT payment_id, SUM(amount_original_minor) AS refund_minor
            FROM refunds
            GROUP BY payment_id
        )
        SELECT
            p.currency_original AS cur,
            SUM(GREATEST(0, p.amount_original_minor - COALESCE(rf.refund_minor,0))) AS net_minor
        FROM p
        LEFT JOIN rf ON rf.payment_id = p.id
        GROUP BY p.currency_original
        """
    )
    with engine.begin() as conn:
        rows = list(conn.execute(q, {"d": target_day}).mappings())
    out: dict[str, float] = {}
    for r in rows:
        cur = str(r["cur"]).upper()
        net_major = float(r["net_minor"] or 0) / 100.0
        out[cur] = net_major
    return {"day": target_day.isoformat(), "by_currency": out, "total_major": sum(out.values())}


def main() -> None:
    load_dotenv()
    init_db()
    target = _parse_date_arg(sys.argv[1]) if len(sys.argv) > 1 else None
    if not target:
        target = get_last_kajabi_day()
    if not target:
        print("No hay pagos de Kajabi en BD")
        return
    res = compute_kajabi_net(target)
    print(res)


if __name__ == "__main__":
    main()



