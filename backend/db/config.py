import os
from contextlib import contextmanager
import logging

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from dotenv import load_dotenv, find_dotenv

# Configura el logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Carga las variables de entorno desde el archivo .env
load_dotenv(find_dotenv())


class Base(DeclarativeBase):
    pass


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://dashboard:dashboard@localhost:5432/dashboard")

# Timeout corto para evitar bloqueos de la UI si la BD no responde
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args={"connect_timeout": 5},
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


@contextmanager
def db_session():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db():
    """Inicializa la BD, asegurando que todas las tablas (incluidas las nuevas) existan."""
    
    # Simplemente importa los modelos para que SQLAlchemy los 'vea'.
    # La recarga puede causar problemas de metadatos duplicados.
    from . import models  # noqa: F401

    Base.metadata.create_all(bind=engine, checkfirst=True)
    logger.info("Tablas de la BD verificadas/creadas.")


