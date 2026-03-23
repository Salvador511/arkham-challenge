"""
PostgreSQL State Driver - Store extraction state (last extraction dates) in database.

Provides abstraction for storing extraction metadata in PostgreSQL instead of filesystem.
Ensures single source of truth for extraction state across server restarts.
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class PostgresStateDriver:
    """
    Store and retrieve extraction state in PostgreSQL.

    Tracks last_extraction_date for each dataset to support incremental extractions.
    """

    def __init__(self, database_url: str):
        """
        Initialize driver with database connection string.

        Args:
            database_url: PostgreSQL connection URL
        """
        self.database_url = database_url
        self._connection = None
        self._create_table()

    def _get_connection(self):
        """Get or create database connection."""
        if self._connection is None:
            import psycopg2

            self._connection = psycopg2.connect(self.database_url)
        return self._connection

    def _create_table(self) -> None:
        """Create extraction_state table if it doesn't exist."""
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS extraction_state (
                        id SERIAL PRIMARY KEY,
                        dataset VARCHAR(50) NOT NULL UNIQUE,
                        last_extraction_date VARCHAR(10),
                        updated_at TIMESTAMP DEFAULT NOW()
                    );
                    CREATE INDEX IF NOT EXISTS idx_extraction_dataset
                        ON extraction_state(dataset);
                """)
                conn.commit()
            logger.info("✅ extraction_state table ready")
        except Exception as exc:
            logger.error("❌ Failed to create extraction_state table: %s", exc)
            raise

    def load_state(self) -> dict:
        """
        Load extraction state from database.

        Returns:
            Dictionary with last extraction dates for each dataset:
            {
                'facility_outages': {'last_extraction_date': '2026-03-22'},
                'us_outages': {'last_extraction_date': '2026-03-22'},
                'plants': {'last_extraction_date': '2026-03-22'}
            }
        """
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT dataset, last_extraction_date FROM extraction_state")
                rows = cur.fetchall()

            # Convert to state format
            state = {
                "facility_outages": {"last_extraction_date": None},
                "us_outages": {"last_extraction_date": None},
                "plants": {"last_extraction_date": None},
            }

            for dataset, last_date in rows:
                if dataset in state:
                    state[dataset]["last_extraction_date"] = last_date

            logger.info("📖 Loaded extraction state from database")
            return state

        except Exception as exc:
            logger.error("❌ Failed to load state from database: %s", exc)
            raise

    def save_state(self, state: dict) -> None:
        """
        Save extraction state to database (UPSERT).

        Args:
            state: Dictionary with extraction state
        """
        try:
            conn = self._get_connection()

            for dataset, metadata in state.items():
                last_extraction_date = metadata.get("last_extraction_date")

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO extraction_state (dataset, last_extraction_date, updated_at)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (dataset) DO UPDATE SET
                            last_extraction_date = EXCLUDED.last_extraction_date,
                            updated_at = NOW()
                    """,
                        (dataset, last_extraction_date, datetime.now()),
                    )

                logger.debug("Saved state for %s: %s", dataset, last_extraction_date)

            conn.commit()
            logger.info("💾 Saved extraction state to database")

        except Exception as exc:
            logger.error("❌ Failed to save state to database: %s", exc)
            raise

    def clear_cache(self) -> None:
        """Clear any connection caches (for testing)."""
        if self._connection:
            self._connection.close()
            self._connection = None
        logger.debug("State driver connection cleared")

    def close(self) -> None:
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("State driver connection closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
