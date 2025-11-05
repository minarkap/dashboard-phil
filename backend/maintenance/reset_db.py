from __future__ import annotations

"""Herramienta para resetear completamente la base de datos del dashboard.

Borra TODAS las tablas definidas en SQLAlchemy y las vuelve a crear.
"""

from typing import Dict

from backend.db.config import Base, engine, init_db


def reset_database() -> Dict[str, int]:
    stats = {"dropped": 0, "created": 0}
    # PostgreSQL: DROP SCHEMA public CASCADE para asegurar borrado total
    with engine.begin() as conn:
        backend = engine.url.get_backend_name()
        if str(backend).startswith("postgresql"):
            conn.exec_driver_sql("DROP SCHEMA IF EXISTS public CASCADE;")
            conn.exec_driver_sql("CREATE SCHEMA public;")
            stats["dropped"] = 1
        else:
            Base.metadata.drop_all(bind=conn)
            stats["dropped"] = 1
    # Re-crear todo el esquema
    init_db()
    stats["created"] = 1
    return stats


def main() -> None:
    res = reset_database()
    print(f"DB reseteada: {res}")


if __name__ == "__main__":
    main()


