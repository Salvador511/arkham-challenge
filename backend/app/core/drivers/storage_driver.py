"""
Database Parquet Storage Driver - Serialize/deserialize parquets to PostgreSQL BYTEA.

Provides abstraction for storing parquet data in PostgreSQL instead of filesystem.
Useful for cloud deployments (Render, Heroku) where filesystem is ephemeral.
"""

import logging
from datetime import datetime
from io import BytesIO

import pandas as pd

logger = logging.getLogger(__name__)


class DatabaseParquetDriver:
    """
    Store and retrieve parquet data as BYTEA in PostgreSQL.

    500k rows → ~15MB serialized + compressed with snappy

    Features:
    - Automatic BYTEA serialization/deserialization
    - DataFrame caching in memory with TTL
    - Efficient querying with filtering and pagination
    """

    def __init__(self, database_url: str, cache_ttl_seconds: int = 3600):
        """
        Initialize driver with database connection string.

        Args:
            database_url: PostgreSQL connection URL (e.g., postgresql://user:pass@host/db)
            cache_ttl_seconds: Cache duration in seconds (default 1 hour)
        """
        self.database_url = database_url
        self.cache_ttl_seconds = cache_ttl_seconds
        self._connection = None

        # In-memory cache for DataFrames: {dataset_name: (df, timestamp)}
        self._dataframe_cache = {}

        self._create_table()

    def _get_connection(self):
        """Get or create database connection."""
        if self._connection is None:
            import psycopg2

            self._connection = psycopg2.connect(self.database_url)
        return self._connection

    def _create_table(self) -> None:
        """Create data_cache table if it doesn't exist."""
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS data_cache (
                        id SERIAL PRIMARY KEY,
                        dataset VARCHAR(50) NOT NULL UNIQUE,
                        parquet_data BYTEA NOT NULL,
                        updated_at TIMESTAMP DEFAULT NOW()
                    );
                    CREATE INDEX IF NOT EXISTS idx_dataset ON data_cache(dataset);
                """)
                conn.commit()
            logger.info("✅ data_cache table ready")
        except Exception as exc:
            logger.error("❌ Failed to create data_cache table: %s", exc)
            raise

    def save(self, dataset_name: str, df: pd.DataFrame) -> None:
        """
        Serialize DataFrame to parquet and save as BYTEA in PostgreSQL.
        Invalidates cache for this dataset.

        Args:
            dataset_name: Name of dataset (e.g., 'facility_outages')
            df: DataFrame to save

        Raises:
            Exception: If save fails
        """
        try:
            # Serialize DataFrame to parquet bytes (snappy compression)
            buffer = BytesIO()
            df.to_parquet(buffer, compression="snappy", index=False)
            parquet_bytes = buffer.getvalue()

            size_mb = len(parquet_bytes) / 1024 / 1024
            logger.info(
                "💾 Serialized %s: %d rows, %.1f MB (compressed)", dataset_name, len(df), size_mb
            )

            # Insert or update in database (UPSERT)
            conn = self._get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO data_cache (dataset, parquet_data, updated_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (dataset) DO UPDATE SET
                        parquet_data = EXCLUDED.parquet_data,
                        updated_at = NOW()
                """,
                    (dataset_name, parquet_bytes, datetime.now()),
                )
                conn.commit()

            # Invalidate cache for this dataset (will be reloaded next time)
            if dataset_name in self._dataframe_cache:
                del self._dataframe_cache[dataset_name]
                logger.debug("Cache invalidated for %s", dataset_name)

            logger.info("✅ Saved %s to data_cache (upsert)", dataset_name)

        except Exception as exc:
            logger.error("❌ Failed to save %s: %s", dataset_name, exc)
            raise

    def load(self, dataset_name: str) -> pd.DataFrame:
        """
        Load parquet from PostgreSQL BYTEA and deserialize to DataFrame.
        Uses in-memory cache to avoid repeated deserialization.

        Args:
            dataset_name: Name of dataset (e.g., 'facility_outages')

        Returns:
            DataFrame with the loaded data

        Raises:
            FileNotFoundError: If dataset not found in database
        """
        # Check cache first
        if dataset_name in self._dataframe_cache:
            df, timestamp = self._dataframe_cache[dataset_name]
            age = (datetime.now() - timestamp).total_seconds()

            if age < self.cache_ttl_seconds:
                logger.info(
                    "📖 Loaded %s: %d rows (from CACHE, age=%.1fs)", dataset_name, len(df), age
                )
                return df.copy()  # Return copy to avoid accidental mutations
            else:
                # Cache expired, remove it
                del self._dataframe_cache[dataset_name]
                logger.debug("Cache expired for %s, reloading from DB", dataset_name)

        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT parquet_data FROM data_cache WHERE dataset = %s", (dataset_name,)
                )
                row = cur.fetchone()

                if not row:
                    raise FileNotFoundError(f"Dataset '{dataset_name}' not found in data_cache")

                parquet_bytes = row[0]

            # Deserialize
            buffer = BytesIO(parquet_bytes)
            df = pd.read_parquet(buffer)

            # Cache the DataFrame
            self._dataframe_cache[dataset_name] = (df, datetime.now())

            logger.info("📖 Loaded %s: %d rows (from database → CACHED)", dataset_name, len(df))
            return df

        except FileNotFoundError:
            raise
        except Exception as exc:
            logger.error("❌ Failed to load %s: %s", dataset_name, exc)
            raise

    def query(
        self,
        dataset_name: str,
        filters: dict | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> pd.DataFrame:
        """
        Load dataset with filtering, limit, and offset applied efficiently.

        Loads parquet from DB, applies filters in memory, returns only needed rows.
        Much more efficient than loading full dataset and filtering client-side.

        Args:
            dataset_name: Name of dataset (e.g., 'facility_outages')
            filters: Dict of column -> value for WHERE conditions
                    e.g., {'facility_id': 123, 'date_range': ('2026-01-01', '2026-03-22')}
            limit: Max rows to return
            offset: Skip this many rows before returning

        Returns:
            Filtered and paginated DataFrame

        Example:
            driver.query('facility_outages',
                        filters={'facility_id': 8907},
                        limit=100,
                        offset=0)
        """
        try:
            # Load full dataset
            df = self.load(dataset_name)

            # Apply filters if provided
            if filters:
                for column, value in filters.items():
                    if isinstance(value, tuple) and len(value) == 2:
                        # Range filter: (min, max)
                        df = df[(df[column] >= value[0]) & (df[column] <= value[1])]
                    else:
                        # Equality filter
                        df = df[df[column] == value]

            total_count = len(df)

            # Apply pagination
            df = df.iloc[offset : offset + (limit if limit else len(df))]

            rows_returned = len(df)
            logger.debug(
                "🔍 Query %s: filters=%s, limit=%s, offset=%s → %d/%d rows",
                dataset_name,
                filters,
                limit,
                offset,
                rows_returned,
                total_count,
            )

            return df

        except Exception as exc:
            logger.error("❌ Failed to query %s: %s", dataset_name, exc)
            raise

    def exists(self, dataset_name: str) -> bool:
        """
        Check if dataset exists in database.

        Args:
            dataset_name: Name of dataset

        Returns:
            True if dataset exists, False otherwise
        """
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM data_cache WHERE dataset = %s LIMIT 1", (dataset_name,))
                exists = cur.fetchone() is not None

            logger.debug("Dataset '%s' exists: %s", dataset_name, exists)
            return exists

        except Exception as exc:
            logger.error("❌ Failed to check existence of %s: %s", dataset_name, exc)
            return False

    def has_any_data(self) -> bool:
        """
        Check if database has ANY data in data_cache table.
        Useful for detecting recovery scenarios (Delta tables deleted but DB has data).

        Returns:
            True if at least one dataset exists in database, False otherwise
        """
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM data_cache")
                count = cur.fetchone()[0]

            has_data = count > 0
            logger.debug("Database has data: %s (%d datasets)", has_data, count)
            return has_data

        except Exception as exc:
            logger.error("❌ Failed to check if database has data: %s", exc)
            return False

    def close(self) -> None:
        """Close database connection."""
        if self._connection:
            self._connection.close()
            logger.info("Database connection closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
