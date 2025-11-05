import sys
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

from backend.db.config import init_db, db_session
from backend.db.models import Customer, Order, Payment, Product, OrderItem


def parse_amount(s: str) -> float:
    s_clean = (s or "").replace("€", "").replace("$", "").strip()
    # Gestionar formato europeo (ej: "1.234,56") y americano (ej: "1,234.56")
    is_european_format = "," in s_clean and "." in s_clean and s_clean.rfind(",") > s_clean.rfind(".")
    is_us_format = "," in s_clean and "." in s_clean and s_clean.rfind(".") > s_clean.rfind(",")

    if is_european_format:
        s_clean = s_clean.replace(".", "").replace(",", ".")  # 1.234,56 -> 1234.56
    elif is_us_format:
        s_clean = s_clean.replace(",", "")  # 1,234.56 -> 1234.56
    else:
        # Casos simples como "19,99" o "19.99"
        s_clean = s_clean.replace(",", ".")

    try:
        return float(s_clean)
    except (ValueError, TypeError):
        return 0.0


def import_transactions_csv(csv_path: Path, insert_only: bool = True) -> dict:
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    detected = 0
    inserted = 0
    updated = 0
    for _, row in df.iterrows():
        tx_id = str(row.get("ID") or row.get("Order No.") or "").strip()
        if not tx_id:
            continue
        detected += 1
        amount_major = parse_amount(row.get("Amount"))
        is_refund = amount_major < 0
        abs_amount_major = abs(amount_major)
        currency = (row.get("Currency") or "EUR").strip().upper()
        email = (row.get("Customer Email") or "").strip().lower() or None
        created_at_raw = row.get("Created At") or ""
        try:
            created_at = pd.to_datetime(created_at_raw).to_pydatetime()
            # Normalizar a naive UTC para evitar falsas actualizaciones repetidas
            if created_at.tzinfo is not None:
                created_at = created_at.astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            created_at = None
        status = (row.get("Status") or row.get("Type") or "").strip().lower()
        if is_refund:
            status = "refunded"
        elif not status:
            status = "completed"

        with db_session() as s:
            with s.no_autoflush:
                # --- Lookup existing Payment (no creation todavía) ---
                existing_payment = (
                    s.query(Payment)
                    .filter((Payment.source == "kajabi") & (Payment.source_payment_id == tx_id))
                    .one_or_none()
                )
                amount_minor = int(round(amount_major * 100))

                # --- Find/Create Customer ---
                customer = None
                if email:
                    customer = (
                        s.query(Customer)
                        .filter((Customer.source == "kajabi") & (Customer.source_id == email))
                        .one_or_none()
                    )
                    if not customer:
                        customer = Customer(source="kajabi", source_id=email, email=email)
                        s.add(customer)
                        s.flush()  # Necesario para customer.id

                # --- Find/Create Order ---
                order = (
                    s.query(Order)
                    .filter((Order.source == "kajabi") & (Order.source_id == tx_id))
                    .one_or_none()
                )
                if not order:
                    order = Order(
                        source="kajabi",
                        source_id=tx_id,
                        customer_id=customer.id if customer else None,
                    )
                    s.add(order)
                    s.flush()  # Needed for order.id
                order.status = status
                # Cache ids para evitar accesos perezosos si el objeto se expira
                order_id = int(order.id)

        # --- Find/Create Product & OrderItem (también para refunds para evitar 'Sin producto') ---
        offer_id = (row.get("Offer ID") or "").strip()
        offer_title = (row.get("Offer Title") or "").strip()
        product = None
        if offer_id or offer_title:
            product_source_id = offer_id or offer_title
            product = (
                s.query(Product)
                .filter((Product.source == "kajabi") & (Product.source_id == product_source_id))
                .one_or_none()
            )
            if not product:
                product = Product(source="kajabi", source_id=product_source_id, name=offer_title)
                s.add(product)
                s.flush()  # Needed for product.id
            product_id = int(product.id)

            oi = (
                s.query(OrderItem)
                .filter((OrderItem.order_id == order_id) & (OrderItem.product_id == product_id))
                .one_or_none()
            )
            if not oi:
                oi = OrderItem(
                    order_id=order_id,
                    product_id=product_id,
                    quantity=1,
                    unit_price_original_minor=abs(amount_minor),
                    currency_original=currency,
                    unit_price_eur=(Decimal(abs(amount_minor)) / Decimal(100) if currency == "EUR" else None),
                )
                s.add(oi)

                # --- Create or Update Payment ---
                if existing_payment is None:
                    payment = Payment(
                        source="kajabi",
                        source_payment_id=tx_id,
                        order_id=order_id,
                        status=status or "completed",
                        amount_original_minor=amount_minor,
                        currency_original=currency or "EUR",
                        paid_at=created_at,
                    )
                    # Guardar parte útil del CSV en raw para facilitar trazabilidad
                    try:
                        payment.raw = {
                            "offer_id": offer_id,
                            "offer_title": offer_title,
                            "type": row.get("Type"),
                            "status": row.get("Status"),
                        }
                    except Exception:
                        pass
                    if (currency or "EUR") == "EUR":
                        signed_minor = -abs(amount_minor) if is_refund else abs(amount_minor)
                        signed_major_dec = Decimal(signed_minor) / Decimal(100)
                        payment.amount_eur = signed_major_dec
                        payment.net_eur = signed_major_dec
                    else:
                        payment.amount_eur = None
                        payment.net_eur = None
                    s.add(payment)
                    inserted += 1
                else:
                    if not insert_only:
                        prev = {
                            "status": existing_payment.status,
                            "amount_original_minor": existing_payment.amount_original_minor,
                            "currency_original": existing_payment.currency_original,
                            "paid_at": existing_payment.paid_at,
                            "amount_eur": existing_payment.amount_eur,
                            "net_eur": existing_payment.net_eur,
                        }
                        existing_payment.order_id = order_id
                        existing_payment.status = status or "completed"
                        existing_payment.amount_original_minor = amount_minor
                        existing_payment.currency_original = currency or "EUR"
                        existing_payment.paid_at = created_at
                        if (existing_payment.currency_original or "EUR") == "EUR":
                            signed_minor = -abs(amount_minor) if is_refund else abs(amount_minor)
                            signed_major_dec = Decimal(signed_minor) / Decimal(100)
                            existing_payment.amount_eur = signed_major_dec
                            existing_payment.net_eur = signed_major_dec
                        else:
                            existing_payment.amount_eur = None
                            existing_payment.net_eur = None
                        # Completar raw mínimo si falta
                        if not existing_payment.raw:
                            try:
                                existing_payment.raw = {
                                    "offer_id": offer_id,
                                    "offer_title": offer_title,
                                    "type": row.get("Type"),
                                    "status": row.get("Status"),
                                }
                            except Exception:
                                pass

                        # Comparación robusta: cantidades con misma base minor y fecha normalizada
                        def _eq_num(a, b):
                            if a is None and b is None:
                                return True
                            if a is None or b is None:
                                return False
                            try:
                                return Decimal(a) == Decimal(b)
                            except Exception:
                                return float(a) == float(b)

                        changed = (
                            prev["status"] != existing_payment.status
                            or prev["amount_original_minor"] != existing_payment.amount_original_minor
                            or prev["currency_original"] != existing_payment.currency_original
                            or (prev["paid_at"] or None) != (existing_payment.paid_at or None)
                            or not _eq_num(prev["amount_eur"], existing_payment.amount_eur)
                            or not _eq_num(prev["net_eur"], existing_payment.net_eur)
                        )
                        if changed:
                            updated += 1

    return {"detected": detected, "inserted": inserted, "updated": updated}


def main():
    load_dotenv()
    if len(sys.argv) < 2:
        print("Uso: python -m backend.import_kajabi_csv RUTA_TRANSACTIONS_CSV [--update]")
        sys.exit(1)
    p = Path(sys.argv[1]).expanduser().resolve()
    if not p.exists():
        print(f"No existe: {p}")
        sys.exit(1)
    init_db()
    insert_only = True
    if len(sys.argv) >= 3 and sys.argv[2].strip().lower() in ("--update", "--upsert", "--no-insert-only"):
        insert_only = False
    res = import_transactions_csv(p, insert_only=insert_only)
    print(f"Procesadas {res['detected']} filas. Insertadas {res['inserted']}, actualizadas {res['updated']} (Kajabi)")


if __name__ == "__main__":
    main()


