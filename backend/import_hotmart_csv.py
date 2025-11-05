import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

from backend.db.config import init_db, db_session
from backend.db.models import Customer, Order, Payment, Product, OrderItem


def _parse_dt(value: str) -> Optional[datetime]:
    if not value or pd.isna(value):
        return None
    # Formato típico: 17/09/2025 11:57:51
    for fmt in ("%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(value), fmt)
        except Exception:
            continue
    return None


def import_hotmart_csv(csv_path: Path, insert_only: bool = True) -> dict:
    # Informe extendido de Hotmart con separador ';' y cabeceras en español.
    df = pd.read_csv(csv_path, sep=";", dtype=str, keep_default_na=False)

    # Normalizar nombres de columnas para identificación robusta
    def norm(s: str) -> str:
        import unicodedata
        s2 = unicodedata.normalize("NFD", s or "").encode("ascii", "ignore").decode("ascii")
        return " ".join(s2.strip().lower().split())

    original_cols = list(df.columns)
    norm_cols = [norm(c) for c in original_cols]

    def find_exact(name: str) -> Optional[str]:
        n = norm(name)
        for raw, nrm in zip(original_cols, norm_cols):
            if nrm == n:
                return raw
        return None

    def find_first(prefix: str) -> Optional[str]:
        n = norm(prefix)
        for raw, nrm in zip(original_cols, norm_cols):
            if nrm.startswith(n):
                return raw
        return None

    # Columnas clave con múltiples variantes entre reportes antiguos y extendidos
    col_tx = (
        find_first("codigo de la transaccion")
        or find_first("transaccion")
        or "Transacción"
    )
    col_status = find_first("estatus") or find_first("estado") or find_first("estatus de la transaccion")
    col_email = find_first("email del comprador") or find_first("email")

    # Fechas
    col_fecha_conf = (
        find_first("confirmacion del pago")
        or find_exact("Fecha de Confirmación")
        or find_first("fecha de confirmacion")
    )
    col_fecha_venta = (
        find_first("fecha de la transaccion")
        or find_exact("Fecha de venta")
        or find_first("fecha de venta")
    )

    # Producto
    col_nombre_producto = (
        find_exact("Nombre del Producto")
        or find_first("nombre del producto")
        or find_first("producto")
    )
    col_codigo_producto = find_exact("Código del Producto") or find_first("codigo del producto")
    col_codigo_oferta = (
        find_exact("Código de la Oferta")
        or find_first("codigo de la oferta")
        or find_first("codigo del precio")
    )

    # Importes y monedas
    col_precio_total = (
        find_exact("Precio Total")
        or find_first("valor de compra con impuestos")
        or find_first("valor de compra sin impuestos")
    )
    col_moneda_compra = find_first("moneda de compra")
    col_precio_total_conv = find_exact("Precio Total Convertido")
    col_valor_recibido_conv = (
        find_exact("Valor que has recibido convertido")
        or find_first("facturacion bruta (sin impuestos)")
        or find_first("facturacion bruta")
    )
    col_moneda_recibir = (
        find_first("moneda en la que recibiras los valores")
        or find_first("moneda en la que recibiras los valores")
    )
    col_tasa_cambio_real = find_exact("Tasa de Cambio Real") or find_first("tasa de cambio real") or find_first("tasa de conversion (moneda de compra)")
    col_neto_productor = find_first("facturacion neta del productor") or find_first("faturacion neta del productor")

    # Detectar moneda del precio total: primera columna exacta 'Moneda' posterior a 'Precio Total' si existe
    headers = list(df.columns)
    moneda_cols = [i for i, c in enumerate(headers) if norm(c) == "moneda"]
    # Heurística: si existe 'Moneda en la que recibirás los valores ', úsala como fallback para converted
    col_moneda_recibir = find_first("moneda en la que recibiras") or find_first("moneda en la que recibiras los valores") or find_first("moneda en la que recibiras los valores")

    detected = 0
    inserted = 0
    updated = 0
    for _, row in df.iterrows():
        tx_id = (row.get(col_tx) or "").strip()
        if not tx_id:
            continue
        detected += 1
        status_raw = (row.get(col_status) or "").strip().lower()
        # Normaliza estado a un conjunto estable de valores para analítica
        if "complet" in status_raw:
            status = "completed"
        elif "aprob" in status_raw:
            status = "approved"
        elif "reembols" in status_raw:
            status = "refunded"
        elif "cancel" in status_raw:
            status = "cancelled"
        elif "charge" in status_raw:
            status = "chargeback"
        elif "anal" in status_raw:
            status = "in_analysis"
        elif "expir" in status_raw:
            status = "expired"
        elif "inici" in status_raw:
            status = "initiated"
        elif "reclam" in status_raw:
            status = "claimed"
        elif "solicitud" in status_raw or "pago generada" in status_raw:
            status = "payment_link_generated"
        elif "vencid" in status_raw:
            status = "overdue"
        else:
            status = status_raw
        email = (row.get(col_email) or "").strip().lower() or None

        paid_at = _parse_dt(row.get(col_fecha_conf)) or _parse_dt(row.get(col_fecha_venta))

        # Monto y moneda base inicial: Precio total y moneda de compra
        amount_str = (row.get(col_precio_total) or "").replace(",", ".").strip()
        try:
            amount_major = float(amount_str) if amount_str else 0.0
        except Exception:
            amount_major = 0.0

        # Moneda original de compra
        currency = None
        if col_moneda_compra:
            currency = (row.get(col_moneda_compra) or "").strip().upper() or None
        if not currency:
            # Heurística anterior por posición
            try:
                idx_total = headers.index(col_precio_total) if col_precio_total else -1
                prior_currency_idx = max([i for i in moneda_cols if i < idx_total]) if any(i < idx_total for i in moneda_cols) else None
                if prior_currency_idx is not None:
                    currency_col_name = headers[prior_currency_idx]
                    currency = (row.get(currency_col_name) or "").strip().upper() or None
            except Exception:
                currency = None
        if not currency:
            currency = "EUR"

        # amount_minor se recalculará al final, tras posibles overrides

        # Calcular amount_eur/net_eur en EUR si tenemos datos en EUR en el informe extendido
        amount_eur: Optional[float] = None
        net_eur: Optional[float] = None
        recibir_moneda = (row.get(col_moneda_recibir) or "").strip().upper() if col_moneda_recibir else None
        # Intentar con columnas de facturación bruta / neta cuando la moneda de recepción es EUR
        if recibir_moneda == "EUR":
            try:
                val_bruta_str = (row.get(col_valor_recibido_conv) or "").replace(",", ".").strip()
                amount_eur = float(val_bruta_str) if val_bruta_str else None
            except Exception:
                amount_eur = None
            try:
                val_neto_str = (row.get(col_neto_productor) or "").replace(",", ".").strip()
                net_eur = float(val_neto_str) if val_neto_str else None
            except Exception:
                net_eur = None
        # Fallbacks: si no hay columnas nuevas, usar heurísticas anteriores SOLO cuando están en EUR
        if amount_eur is None:
            try:
                if col_valor_recibido_conv:
                    idx_val = headers.index(col_valor_recibido_conv)
                    prior_currency_idx2 = max([i for i in moneda_cols if i < idx_val]) if any(i < idx_val for i in moneda_cols) else None
                    if prior_currency_idx2 is not None:
                        curr2 = (row.get(headers[prior_currency_idx2]) or "").strip().upper()
                        val_str = (row.get(col_valor_recibido_conv) or "").replace(",", ".").strip()
                        if curr2 == "EUR" and val_str:
                            amount_eur = float(val_str)
            except Exception:
                pass

        if amount_eur is None and col_precio_total_conv:
            try:
                idx_conv = headers.index(col_precio_total_conv)
                prior_currency_idx3 = max([i for i in moneda_cols if i < idx_conv]) if any(i < idx_conv for i in moneda_cols) else None
                if prior_currency_idx3 is not None:
                    curr3 = (row.get(headers[prior_currency_idx3]) or "").strip().upper()
                    val2_str = (row.get(col_precio_total_conv) or "").replace(",", ".").strip()
                    if curr3 == "EUR" and val2_str:
                        amount_eur = float(val2_str)
            except Exception:
                pass

        if amount_eur is None and currency == "EUR":
            amount_eur = amount_major

        # Si existe Facturación neta del Productor(a) y conocemos la moneda de recepción,
        # usaremos ese neto como amount_original y la moneda de recepción como currency.
        # Esto alinea nuestra analítica con los KPIs de Hotmart (payout por moneda).
        try:
            net_str_any = (row.get(col_neto_productor) or "").replace(",", ".").strip() if col_neto_productor else ""
            net_major_any = float(net_str_any) if net_str_any else None
        except Exception:
            net_major_any = None

        if net_major_any is not None and net_major_any != 0 and recibir_moneda:
            # Override a neto por moneda de recepción
            amount_major = net_major_any
            currency = recibir_moneda
            # Para EUR sí persistimos net_eur/amount_eur; para otras monedas, lo dejamos a NULL
            if recibir_moneda == "EUR":
                amount_eur = net_major_any
                net_eur = net_major_any
            else:
                amount_eur = None
                net_eur = None

        # Recalcular minor tras cualquier override
        amount_minor = int(round((amount_major or 0.0) * 100))

        with db_session() as s:
            customer = None
            if email:
                customer = s.query(Customer).filter((Customer.source == "hotmart") & (Customer.source_id == email)).one_or_none()
                if not customer:
                    customer = Customer(source="hotmart", source_id=email, email=email)
                    s.add(customer)

            order = s.query(Order).filter((Order.source == "hotmart") & (Order.source_id == tx_id)).one_or_none()
            if not order:
                order = Order(source="hotmart", source_id=tx_id, customer_id=customer.id if customer else None, status=status)
                s.add(order)
                s.flush()

            # Producto y línea de pedido
            product_name = (row.get(col_nombre_producto) or "").strip()
            product_source_id = (row.get(col_codigo_producto) or row.get(col_codigo_oferta) or product_name or "").strip()
            product_obj = None
            if product_source_id:
                product_obj = (
                    s.query(Product)
                    .filter((Product.source == "hotmart") & (Product.source_id == product_source_id))
                    .one_or_none()
                )
                if not product_obj:
                    product_obj = Product(source="hotmart", source_id=product_source_id, name=product_name or product_source_id)
                    s.add(product_obj)
                    s.flush()

            # Pago
            payment = s.query(Payment).filter((Payment.source == "hotmart") & (Payment.source_payment_id == tx_id)).one_or_none()
            if not payment:
                payment = Payment(
                    order_id=order.id,
                    source="hotmart",
                    source_payment_id=tx_id,
                    status=status or "completed",
                    amount_original_minor=amount_minor,
                    currency_original=currency,
                    amount_eur=amount_eur,
                    net_eur=net_eur if net_eur is not None else amount_eur,
                    paid_at=paid_at,
                    raw=None,
                )
                s.add(payment)
                inserted += 1
            else:
                if not insert_only:
                    prev = {
                        "status": payment.status,
                        "paid_at": payment.paid_at,
                        "currency_original": payment.currency_original,
                        "amount_original_minor": payment.amount_original_minor,
                        "amount_eur": payment.amount_eur,
                        "net_eur": payment.net_eur,
                    }
                    payment.status = status or payment.status
                    payment.paid_at = paid_at or payment.paid_at
                    # Actualiza moneda/monto si cambiaron (p.ej., importación previa errónea)
                    if currency and payment.currency_original != currency:
                        payment.currency_original = currency
                    if amount_minor and payment.amount_original_minor != amount_minor:
                        payment.amount_original_minor = amount_minor
                    if amount_eur is not None:
                        payment.amount_eur = amount_eur
                    if net_eur is not None:
                        payment.net_eur = net_eur
                    # Importante: si la moneda no es EUR, dejamos amount_eur/net_eur en NULL.
                    # La conversión a EUR se hará a nivel de analítica con series FX fiables.
                    changed = (
                        prev["status"] != payment.status or
                        (prev["paid_at"] or None) != (payment.paid_at or None) or
                        prev["currency_original"] != payment.currency_original or
                        prev["amount_original_minor"] != payment.amount_original_minor or
                        prev["amount_eur"] != payment.amount_eur or
                        prev["net_eur"] != payment.net_eur
                    )
                    if changed:
                        updated += 1

            # Crear OrderItem si falta
            if product_obj:
                oi = (
                    s.query(OrderItem)
                    .filter((OrderItem.order_id == order.id) & (OrderItem.product_id == product_obj.id))
                    .one_or_none()
                )
                if not oi:
                    oi = OrderItem(
                        order_id=order.id,
                        product_id=product_obj.id,
                        quantity=1,
                        unit_price_original_minor=amount_minor,
                        currency_original=currency,
                        unit_price_eur=amount_eur if currency == "EUR" and amount_eur is not None else None,
                    )
                    s.add(oi)

    return {"detected": detected, "inserted": inserted, "updated": updated}


def main():
    load_dotenv()
    if len(sys.argv) < 2:
        print("Uso: python -m backend.import_hotmart_csv RUTA_DEL_CSV")
        sys.exit(1)
    csv_path = Path(sys.argv[1]).expanduser().resolve()
    if not csv_path.exists():
        print(f"No existe: {csv_path}")
        sys.exit(1)
    init_db()
    res = import_hotmart_csv(csv_path)
    print(f"Procesadas {res['detected']} filas. Insertadas {res['inserted']}, actualizadas {res['updated']} pagos (Hotmart)")


if __name__ == "__main__":
    main()


