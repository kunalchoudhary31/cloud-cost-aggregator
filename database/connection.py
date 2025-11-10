"""
Database connection management and session handling
"""
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool
import logging

from database.models import Base

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Manages database connections and sessions
    """

    def __init__(self, db_url: str):
        """
        Initialize database manager with connection URL

        Args:
            db_url: PostgreSQL connection URL (e.g., postgresql://user:pass@host:port/db)
        """
        self.db_url = db_url
        self.engine = None
        self.SessionLocal = None

    def initialize(self):
        """
        Create database engine and session factory
        """
        logger.info(f"Initializing database connection...")

        # Create engine with connection pooling
        self.engine = create_engine(
            self.db_url,
            pool_pre_ping=True,  # Verify connections before using them
            pool_size=5,
            max_overflow=10,
            echo=False  # Set to True for SQL query logging
        )

        # Create session factory
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )

        logger.info("Database connection initialized successfully")

    def create_tables(self):
        """
        Create all tables defined in models
        """
        logger.info("Creating database tables...")
        Base.metadata.create_all(bind=self.engine)
        logger.info("Database tables created successfully")

    @contextmanager
    def get_session(self) -> Session:
        """
        Context manager for database sessions

        Yields:
            SQLAlchemy session

        Example:
            with db_manager.get_session() as session:
                session.add(record)
                session.commit()
        """
        if not self.SessionLocal:
            raise RuntimeError("Database not initialized. Call initialize() first.")

        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()

    def test_connection(self) -> bool:
        """
        Test database connection

        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection test successful")
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False

    def close(self):
        """
        Close database connections
        """
        if self.engine:
            self.engine.dispose()
            logger.info("Database connections closed")


def build_database_url(host: str, port: int, database: str, user: str, password: str) -> str:
    """
    Build PostgreSQL connection URL

    Args:
        host: Database host
        port: Database port
        database: Database name
        user: Database user
        password: Database password

    Returns:
        PostgreSQL connection URL
    """
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"
