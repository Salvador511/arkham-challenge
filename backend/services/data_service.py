"""Data Service - Load and filter parquet files"""

import logging
from typing import Any

import pandas as pd

from app.config import settings
from exceptions import DataNotFoundError, ProcessingError, ValidationError

logger = logging.getLogger(__name__)


class DataService:
    """Service for loading and filtering parquet data"""

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
        Load dataset from parquet and apply filters + pagination
        """
        if dataset not in settings.valid_datasets:
            raise ValidationError("Invalid dataset. Must be 'facility', 'us', or 'plants'")
        if offset < 0 or limit < 1:
            raise ValidationError("offset must be >= 0 and limit must be >= 1")
        if limit > settings.max_limit:
            limit = settings.max_limit

        dataset_filepaths = {
            "facility": settings.facility_outages_file,
            "us": settings.us_outages_file,
            "plants": settings.plants_file,
        }
        filepath = dataset_filepaths[dataset]

        try:
            df = pd.read_parquet(filepath)
        except FileNotFoundError:
            raise DataNotFoundError(f"Dataset '{dataset}' not found") from None
        except Exception as e:
            logger.error(f"Error reading {filepath}: {e}")
            raise ProcessingError("Failed to read dataset") from e

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

        if date_from and date_to and has_date_column:
            if pd.to_datetime(date_from) > pd.to_datetime(date_to):
                raise ValidationError("date_from must be <= date_to")

        if facility_id:
            if dataset != "facility":
                raise ValidationError("facility_id parameter only allowed for 'facility' dataset")
            df = df[df["facility_id"] == facility_id]

        total_count = len(df)

        if has_date_column:
            df = df.sort_values("date", ascending=False)

        paginated_df = df.iloc[offset : offset + limit].copy()

        if dataset == "facility":
            try:
                plants = pd.read_parquet(settings.plants_file)
                paginated_df = paginated_df.merge(plants, on="facility_id", how="left")
            except Exception as e:
                logger.error(f"Error joining with plants: {e}")

        if has_date_column:
            paginated_df["date"] = paginated_df["date"].dt.strftime("%Y-%m-%d")

        data = paginated_df.to_dict("records")

        return {
            "total_count": total_count,
            "offset": offset,
            "limit": limit,
            "returned": len(data),
            "data": data,
        }
