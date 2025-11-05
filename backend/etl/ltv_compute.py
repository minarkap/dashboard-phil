from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from typing import Dict, Tuple

from backend.db.config import db_session
from backend.db.models import Payment, Refund, Order, Customer, CustomerLTV


# Tipos de cambio de respaldo hacia EUR (aprox.)
FALLBACK_FX_RATES: dict[str, float] = {
    "USD": 0.93,
    "GBP": 1.18,
    "MXN": 0.05,
    "COP": 0.00023,
    "CLP": 0.001,
    "CRC": 0.00188,
    "ARS": 0.001,
    "PEN": 0.25,
    "UYU": 0.023,
}


def _to_eur(amount_minor: int | None, currency: str | None) -> float:
    if not amount_minor:
        return 0.0
    cur = (currency or "EUR").upper()
    major = float(Decimal(int(amount_minor)) / Decimal(100))
    if cur == "EUR":
        return major
    rate = FALLBACK_FX_RATES.get(cur)
    if rate is None:
        return 0.0
    return major * rate


def compute_and_persist_ltv_global() -> dict:
    """Calcula LTV por (email, source) sumando pagos menos reembolsos, y persiste en customer_ltv.
    Usa tipos de cambio de respaldo cuando no es EUR.
    """
    # Acumular LTV por (email, source)
    ltv_map: Dict[Tuple[str, str], float] = defaultdict(float)

    with db_session() as s:
        # Join Payment -> Order -> Customer
        q = (
            s.query(Payment, Order, Customer)
            .join(Order, Payment.order_id == Order.id)
            .join(Customer, Order.customer_id == Customer.id)
        )
        for pay, order, cust in q:
            if not cust.email:
                continue
            src = str(pay.source)
            key = (cust.email.lower(), src)
            amt_eur = 0.0
            if pay.amount_eur is not None:
                try:
                    amt_eur = float(pay.amount_eur)
                except Exception:
                    amt_eur = 0.0
            else:
                amt_eur = _to_eur(pay.amount_original_minor, pay.currency_original)

            # Restar reembolsos
            ref_total_eur = 0.0
            for rf in s.query(Refund).filter(Refund.payment_id == pay.id):
                ref_total_eur += _to_eur(rf.amount_original_minor, rf.currency_original)

            net_eur = max(0.0, amt_eur - ref_total_eur)
            ltv_map[key] += net_eur

        # Upsert en customer_ltv
        upserted = 0
        for (email, source), ltv_eur in ltv_map.items():
            row = (
                s.query(CustomerLTV)
                .filter((CustomerLTV.email == email) & (CustomerLTV.source == source))
                .one_or_none()
            )
            if not row:
                s.add(CustomerLTV(email=email, source=source, ltv_eur=ltv_eur, updated_at=datetime.utcnow()))
                upserted += 1
            else:
                row.ltv_eur = ltv_eur
                row.updated_at = datetime.utcnow()
                upserted += 1

    return {"sources": len({src for (_, src) in ltv_map.keys()}), "customers": len({email for (email, _) in ltv_map.keys()}), "upserted": upserted}



