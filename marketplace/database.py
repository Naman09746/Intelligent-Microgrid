"""
Database configuration for the P2P Energy Marketplace.
Uses SQLite for zero-setup deployment. Swap DATABASE_URL for PostgreSQL in production.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import os

# SQLite file stored at project root
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "sqlite:///./marketplace.db"
)

# For SQLite, we need check_same_thread=False for FastAPI's async access
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False, "timeout": 30} if "sqlite" in DATABASE_URL else {},
    echo=False,           # Set True for SQL debug logging
    pool_pre_ping=True,   # Reconnect on stale connections
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    """FastAPI dependency — yields a DB session, auto-closes after request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Create all tables if they don't exist. Called on app startup."""
    Base.metadata.create_all(bind=engine)
