"""Unit tests for DataService."""

from unittest.mock import patch

import pandas as pd
import pytest

from app.exceptions import DataNotFoundError, ValidationError
from services.data_service import DataService


class TestDataServiceValidation:
    """Tests for DataService input validation."""

    def test_invalid_dataset_raises_error(self):
        """Test that invalid dataset name raises ValidationError."""
        with pytest.raises(ValidationError, match="Invalid dataset"):
            DataService.get_dataset("invalid_dataset")

    def test_negative_offset_raises_error(self):
        """Test that negative offset raises ValidationError."""
        with pytest.raises(ValidationError, match="offset must be >= 0"):
            DataService.get_dataset("facility", offset=-1)

    def test_zero_limit_raises_error(self):
        """Test that limit < 1 raises ValidationError."""
        with pytest.raises(ValidationError, match="limit must be >= 1"):
            DataService.get_dataset("facility", limit=0)

    @patch("services.data_service.pd.read_parquet")
    def test_facility_id_only_for_facility_dataset(self, mock_read, sample_us_dataframe):
        """Test that facility_id parameter only works with 'facility' dataset."""
        mock_read.return_value = sample_us_dataframe

        with pytest.raises(ValidationError, match="facility_id parameter only allowed"):
            DataService.get_dataset("us", facility_id="F001")


class TestDataServiceDateFiltering:
    """Tests for date filtering logic."""

    @patch("services.data_service.pd.read_parquet")
    def test_invalid_date_from_format(self, mock_read):
        """Test that invalid date_from format raises ValidationError."""
        mock_read.return_value = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=5),
                "capacity": [1000] * 5,
                "outage": [100] * 5,
                "percent_outage": [10.0] * 5,
            }
        )

        with pytest.raises(ValidationError, match="Invalid date_from format"):
            DataService.get_dataset("us", date_from="invalid-date")

    @patch("services.data_service.pd.read_parquet")
    def test_invalid_date_to_format(self, mock_read):
        """Test that invalid date_to format raises ValidationError."""
        mock_read.return_value = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=5),
                "capacity": [1000] * 5,
                "outage": [100] * 5,
                "percent_outage": [10.0] * 5,
            }
        )

        with pytest.raises(ValidationError, match="Invalid date_to format"):
            DataService.get_dataset("us", date_to="bad-date")

    @patch("services.data_service.pd.read_parquet")
    def test_date_from_greater_than_date_to(self, mock_read):
        """Test that date_from > date_to raises ValidationError."""
        mock_read.return_value = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=5),
                "capacity": [1000] * 5,
                "outage": [100] * 5,
                "percent_outage": [10.0] * 5,
            }
        )

        with pytest.raises(ValidationError, match="date_from must be <= date_to"):
            DataService.get_dataset("us", date_from="2024-12-31", date_to="2024-01-01")


class TestDataServicePagination:
    """Tests for pagination logic."""

    @patch("services.data_service.pd.read_parquet")
    def test_limit_capped_at_max(self, mock_read, sample_us_dataframe):
        """Test that limit is capped at max_limit."""
        mock_read.return_value = sample_us_dataframe

        result = DataService.get_dataset("us", limit=5000)

        assert result["limit"] == 1000
        assert len(result["data"]) <= 1000

    @patch("services.data_service.pd.read_parquet")
    def test_pagination_offset_and_limit(self, mock_read):
        """Test pagination respects offset and limit."""
        # Create 20 records
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=20),
                "capacity": [1000] * 20,
                "outage": [100] * 20,
                "percent_outage": [10.0] * 20,
            }
        )
        mock_read.return_value = df

        result = DataService.get_dataset("us", offset=5, limit=5)

        assert result["offset"] == 5
        assert result["limit"] == 5
        assert result["returned"] == 5
        assert result["total_count"] == 20


class TestDataServiceMissingFile:
    """Tests for missing file handling."""

    @patch("services.data_service.pd.read_parquet")
    def test_missing_dataset_raises_error(self, mock_read):
        """Test that missing parquet file raises DataNotFoundError."""
        mock_read.side_effect = FileNotFoundError("File not found")

        with pytest.raises(DataNotFoundError, match="Dataset 'facility' not found"):
            DataService.get_dataset("facility")
