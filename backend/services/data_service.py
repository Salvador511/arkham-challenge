"""Data Service - Load and filter parquet files"""

import logging
from typing import Any

import pandas as pd

from app.config import settings
from exceptions import DataNotFoundError, ProcessingError, ValidationError

logger = logging.getLogger(__name__)

# Cache driver instance to avoid recreating table on every request
_storage_driver_cache = None


def get_storage_driver():
    """Get or create cached storage driver instance."""
    global _storage_driver_cache
    if _storage_driver_cache is None and settings.database_url:
        from app.core.drivers.storage_driver import DatabaseParquetDriver

        _storage_driver_cache = DatabaseParquetDriver(settings.database_url)
    return _storage_driver_cache


class DataService:
    """Service for loading and filtering parquet data"""

    # Map public dataset names to internal storage names
    # Public API uses: 'facility', 'us', 'plants'
    # Storage uses: 'facility_outages', 'us_outages', 'plants'
    DATASET_NAME_MAP = {
        "facility": "facility_outages",
        "us": "us_outages",
        "plants": "plants",
    }

    @staticmethod
    def _load_dataframe(dataset: str) -> pd.DataFrame:
        """
        Load DataFrame from database or filesystem.

        If DATABASE_URL is configured, loads from PostgreSQL.
        Otherwise, loads from filesystem (backward compatible).

        Args:
            dataset: Public dataset name ('facility', 'us', 'plants')
        """
        # Map public name to storage name
        storage_name = DataService.DATASET_NAME_MAP.get(dataset)
        if not storage_name:
            raise ValidationError(f"Unknown dataset: {dataset}")

        if settings.database_url:
            # Load from PostgreSQL (use cached driver)
            try:
                driver = get_storage_driver()
                df = driver.load(storage_name)  # ← Use storage name, not public name
                logger.debug("Loaded %s from PostgreSQL (%d rows)", dataset, len(df))
                return df

            except FileNotFoundError:
                raise DataNotFoundError(f"Dataset '{dataset}' not found in database") from None
            except Exception as e:
                logger.error(f"Error loading {dataset} from database: {e}")
                raise ProcessingError("Failed to read dataset from database") from e

        else:
            # Load from filesystem (default behavior)
            dataset_filepaths = {
                "facility": settings.facility_outages_file,
                "us": settings.us_outages_file,
                "plants": settings.plants_file,
            }
            filepath = dataset_filepaths.get(dataset)

            try:
                df = pd.read_parquet(filepath)
                logger.debug("Loaded %s from filesystem (%d rows)", dataset, len(df))
                return df
            except FileNotFoundError:
                raise DataNotFoundError(f"Dataset '{dataset}' not found") from None
            except Exception as e:
                logger.error(f"Error reading {filepath}: {e}")
                raise ProcessingError("Failed to read dataset") from e

    @staticmethod
    def get_dataset(
        dataset: str,
        date_from: str | None = None,
        date_to: str | None = None,
        facility_id: str | None = None,
        offset: int = 0,
        limit: int = 100,
    ) -> dict[str, Any]:
        """
        Load dataset from parquet and apply filters + pagination.

        If using PostgreSQL backend, filters are applied efficiently in the driver.
        Otherwise, uses filesystem with in-memory filtering.
        """
        if dataset not in settings.valid_datasets:
            raise ValidationError("Invalid dataset. Must be 'facility', 'us', or 'plants'")
        if offset < 0 or limit < 1:
            raise ValidationError("offset must be >= 0 and limit must be >= 1")
        if limit > settings.max_limit:
            limit = settings.max_limit

        # Validate date range
        if date_from and date_to:
            try:
                date_from_obj = pd.to_datetime(date_from)
                date_to_obj = pd.to_datetime(date_to)
                if date_from_obj > date_to_obj:
                    raise ValidationError("date_from must be <= date_to")
            except pd.errors.ParserError as exc:
                raise ValidationError("Invalid date format. Use YYYY-MM-DD") from exc

        storage_name = DataService.DATASET_NAME_MAP.get(dataset)

        # Build filters and get data
        total_count = 0
        if settings.database_url:
            try:
                driver = get_storage_driver()

                # Build filters dict for efficient querying
                filters = {}
                if facility_id and dataset == "facility":
                    filters["facility_id"] = facility_id
                if date_from or date_to:
                    if date_from and date_to:
                        filters["date"] = (date_from, date_to)
                    elif date_from:
                        filters["date"] = (date_from, "9999-12-31")
                    elif date_to:
                        filters["date"] = ("1900-01-01", date_to)

                # Load all filtered data (no pagination yet) to get total count
                df = driver.query(storage_name, filters=filters if filters else None)

                # Calculate total count BEFORE pagination
                total_count = len(df)

                # Now apply pagination on filtered data
                df = df.iloc[offset : offset + limit]

                logger.debug(
                    "Queried %s with filters=%s (total: %d)", dataset, filters, total_count
                )

            except FileNotFoundError:
                raise DataNotFoundError(f"Dataset '{dataset}' not found in database") from None
            except Exception as e:
                logger.error(f"Error querying {dataset} from database: {e}")
                raise ProcessingError("Failed to read dataset from database") from e

        else:
            # Filesystem fallback: load full dataset and filter in memory
            df = DataService._load_dataframe(dataset)

            has_date_column = "date" in df.columns
            if has_date_column:
                df["date"] = pd.to_datetime(df["date"])

            if date_from and has_date_column:
                try:
                    df = df[df["date"] >= pd.to_datetime(date_from)]
                except Exception as exc:
                    raise ValidationError("Invalid date_from format. Use YYYY-MM-DD") from exc

            if date_to and has_date_column:
                try:
                    df = df[df["date"] <= pd.to_datetime(date_to)]
                except Exception as exc:
                    raise ValidationError("Invalid date_to format. Use YYYY-MM-DD") from exc

            if facility_id:
                if dataset != "facility":
                    raise ValidationError(
                        "facility_id parameter only allowed for 'facility' dataset"
                    )
                df = df[df["facility_id"] == facility_id]

            if has_date_column:
                df = df.sort_values("date", ascending=False)

            # Get total count BEFORE pagination
            total_count = len(df)

            # Apply pagination
            df = df.iloc[offset : offset + limit]

        # Format response
        has_date_column = "date" in df.columns
        if has_date_column:
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

        # Join with plants for facility dataset
        if dataset == "facility":
            try:
                plants = DataService._load_dataframe("plants")
                df = df.merge(plants, on="facility_id", how="left")
            except Exception as e:
                logger.error(f"Error joining with plants: {e}")

        data = df.to_dict("records")

        return {
            "total_count": total_count,
            "offset": offset,
            "limit": limit,
            "returned": len(data),
            "data": data,
        }
