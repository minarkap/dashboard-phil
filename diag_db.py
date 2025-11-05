import os
import sys

from sqlalchemy import text


def main() -> None:
    sys.path.insert(0, "/Users/JoseSanchis/Projects/phil_hugo/dashboard")
    try:
        from backend.db.config import engine
    except Exception as e:  # noqa: BLE001
        print("Import error:", e)
        return

    print("DATABASE_URL env:", os.getenv("DATABASE_URL"))
    print("Engine URL:", engine.url)

    try:
        with engine.begin() as conn:
            print("Connected OK")

            # Conteos por tabla
            counts: dict[str, object] = {}
            for t in [
                "customers",
                "products",
                "orders",
                "order_items",
                "payments",
                "refunds",
                "subscriptions",
                "ga_sessions_daily",
                "ad_costs_daily",
            ]:
                try:
                    n = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
                    counts[t] = int(n or 0)
                except Exception as e:  # noqa: BLE001
                    counts[t] = f"error: {e}"
            print("Counts:", counts)

            # Rango de pagos con paid_at
            try:
                row = (
                    conn.execute(
                        text(
                            """
                            SELECT COUNT(*) AS n, MIN(paid_at) AS min_p, MAX(paid_at) AS max_p
                            FROM payments
                            WHERE paid_at IS NOT NULL
                            """
                        )
                    )
                    .mappings()
                    .first()
                )
                print("payments paid_at not null:", dict(row) if row else row)
            except Exception as e:  # noqa: BLE001
                print("payments paid_at range error:", e)

            # Pagos que pasarían el filtro del dashboard
            try:
                q = text(
                    """
                    SELECT COUNT(*)
                    FROM payments p
                    WHERE p.paid_at IS NOT NULL
                      AND COALESCE(LOWER(p.status), '') NOT IN (
                        'refunded','reembolsado','reembolsada','chargeback',
                        'cancelled','canceled','cancelado','cancelada',
                        'expired','expirada','vencida','vencido',
                        'in_analysis','en análisis','en analisis','analisis',
                        'initiated','iniciada',
                        'claimed','reclamado','reclamada',
                        'payment_link_generated','solicitud de pago generada',
                        'pending','pendiente',
                        'failed','fallido','fallida','fallidas'
                      )
                    """
                )
                n_ok = conn.execute(q).scalar()
                print("payments passing dashboard filter:", int(n_ok or 0))
            except Exception as e:  # noqa: BLE001
                print("filter count error:", e)
    except Exception as e:  # noqa: BLE001
        print("DB connection error:", e)


if __name__ == "__main__":
    main()
