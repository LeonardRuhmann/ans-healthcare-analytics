"""
Database configuration — SQLAlchemy connection to MySQL.

Reads from the same MySQL container defined in docker-compose.yml:
  - Host: localhost:3306
  - User: root / Password: root
  - Database: ans_test
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

import os

# 1. Tenta pegar a URL do Render. Se não existir (rodando localmente), usa o seu Docker de fallback.
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "mysql+pymysql://root:root@localhost:3306/ans_test"
)

# 2. A "Trava de Segurança": Se a string vier do Aiven sem o pymysql, o código corrige sozinho!
if DATABASE_URL.startswith("mysql://"):
    DATABASE_URL = DATABASE_URL.replace("mysql://", "mysql+pymysql://", 1)

engine = create_engine(DATABASE_URL, echo=False)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


class Base(DeclarativeBase):
    pass


def get_db():
    """FastAPI dependency: yields a DB session and closes it after the request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
