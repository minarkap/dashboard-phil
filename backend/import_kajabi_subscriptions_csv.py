import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

from backend.db.config import init_db, db_session
from backend.db.models import Customer, Subscription


def parse_amount(s: str) -> float:
    s = (s or "").replace("€", "").replace("$", "").replace(",", ".").strip()
    try:
        return float(s)
    except Exception:
        return 0.0


def import_subscriptions_csv(csv_path: Path, insert_only: bool = True) -> dict:
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    detected = 0
    inserted = 0
    updated = 0
    for _, row in df.iterrows():
        sub_id = (row.get("Kajabi Subscription ID") or "").strip()
        if not sub_id:
            continue
        detected += 1
        amount_major = parse_amount(row.get("Amount"))
        currency = (row.get("Currency") or "EUR").strip().upper()
        interval = (row.get("Interval") or "").strip().lower()
        status = (row.get("Status") or "").strip().lower()
        trial_ends_on = row.get("Trial Ends On") or ""
        canceled_on = row.get("Canceled On") or ""
        next_payment = row.get("Next Payment Date") or ""
        created_at_raw = row.get("Created At") or ""
        email = (row.get("Customer Email") or "").strip().lower() or None

        def to_dt(x: str) -> Optional[datetime]:
            if not x:
                return None
            try:
                return pd.to_datetime(x).to_pydatetime()
            except Exception:
                return None

        trial_dt = to_dt(trial_ends_on)
        canceled_dt = to_dt(canceled_on)
        next_dt = to_dt(next_payment)
        created_dt = to_dt(created_at_raw)

        with db_session() as s:
            customer = None
            if email:
                customer = s.query(Customer).filter((Customer.source == "kajabi") & (Customer.source_id == email)).one_or_none()
                if not customer:
                    customer = Customer(source="kajabi", source_id=email, email=email)
                    s.add(customer)
                    s.flush()

            sub = s.query(Subscription).filter((Subscription.source == "kajabi") & (Subscription.source_id == sub_id)).one_or_none()
            amount_minor = int(round(amount_major * 100)) if amount_major is not None else None
            if not sub:
                sub = Subscription(
                    source="kajabi",
                    source_id=sub_id,
                    customer_id=customer.id if customer else None,
                    status=status,
                    interval=interval,
                    amount_original_minor=amount_minor,
                    currency_original=currency,
                    trial_ends_on=trial_dt,
                    canceled_on=canceled_dt,
                    next_payment_date=next_dt,
                    created_at=created_dt or datetime.utcnow(),
                )
                s.add(sub)
                inserted += 1
            else:
                if not insert_only:
                    prev_vals = (
                        sub.customer_id,
                        sub.status,
                        sub.interval,
                        sub.amount_original_minor,
                        sub.currency_original,
                        sub.trial_ends_on,
                        sub.canceled_on,
                        sub.next_payment_date,
                        sub.created_at,
                    )
                    sub.customer_id = customer.id if customer else sub.customer_id
                    sub.status = status or sub.status
                    sub.interval = interval or sub.interval
                    sub.amount_original_minor = amount_minor if amount_minor is not None else sub.amount_original_minor
                    sub.currency_original = currency or sub.currency_original
                    sub.trial_ends_on = trial_dt or sub.trial_ends_on
                    sub.canceled_on = canceled_dt or sub.canceled_on
                    sub.next_payment_date = next_dt or sub.next_payment_date
                    sub.created_at = created_dt or sub.created_at
                    new_vals = (
                        sub.customer_id,
                        sub.status,
                        sub.interval,
                        sub.amount_original_minor,
                        sub.currency_original,
                        sub.trial_ends_on,
                        sub.canceled_on,
                        sub.next_payment_date,
                        sub.created_at,
                    )
                    if new_vals != prev_vals:
                        updated += 1
    return {"detected": detected, "inserted": inserted, "updated": updated}


def main():
    load_dotenv()
    if len(sys.argv) < 2:
        print("Uso: python -m backend.import_kajabi_subscriptions_csv RUTA_SUBSCRIPTIONS_CSV")
        sys.exit(1)
    p = Path(sys.argv[1]).expanduser().resolve()
    if not p.exists():
        print(f"No existe: {p}")
        sys.exit(1)
    init_db()
    res = import_subscriptions_csv(p)
    print(f"Procesadas {res['detected']} filas. Insertadas {res['inserted']}, actualizadas {res['updated']} suscripciones (Kajabi)")


if __name__ == "__main__":
    main()
