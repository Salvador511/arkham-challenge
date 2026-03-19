"""Pytest configuration and fixtures for unit tests."""

import pandas as pd
import pytest


@pytest.fixture
def sample_facility_dataframe():
    """Create a sample facility outages DataFrame for tests."""
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=5),
            "facility_id": ["F001", "F001", "F002", "F002", "F003"],
            "facility_name": ["Plant A", "Plant A", "Plant B", "Plant B", "Plant C"],
            "capacity": [1000.0, 1000.0, 1500.0, 1500.0, 2000.0],
            "outage": [100.0, 150.0, 200.0, 250.0, 0.0],
            "percent_outage": [10.0, 15.0, 13.3, 16.7, 0.0],
        }
    )


@pytest.fixture
def sample_us_dataframe():
    """Create a sample US outages DataFrame for tests."""
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=5),
            "capacity": [100000.0, 100000.0, 100000.0, 100000.0, 100000.0],
            "outage": [5000.0, 5500.0, 6000.0, 5800.0, 5200.0],
            "percent_outage": [5.0, 5.5, 6.0, 5.8, 5.2],
        }
    )


@pytest.fixture
def sample_plants_dataframe():
    """Create a sample plants DataFrame for tests."""
    return pd.DataFrame(
        {
            "facility_id": ["F001", "F002", "F003"],
            "facility_name": ["Plant A", "Plant B", "Plant C"],
        }
    )
