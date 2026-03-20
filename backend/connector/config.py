"""Configuration constants for EIA Data Connector."""

from pathlib import Path

# Get the absolute path to the backend directory (parent of connector)
CONNECTOR_DIR = Path(__file__).parent.resolve()
BACKEND_DIR = CONNECTOR_DIR.parent
STORAGE_DIR = BACKEND_DIR / "storage"

# API Configuration
FACILITY_OUTAGES_URL = "https://api.eia.gov/v2/nuclear-outages/facility-nuclear-outages/data/"
US_OUTAGES_URL = "https://api.eia.gov/v2/nuclear-outages/us-nuclear-outages/data/"
API_TIMEOUT = 30
MAX_RETRIES = 2
RETRY_DELAY = 1

# Data Configuration
PAGE_SIZE = 5000
DATA_FIELDS = ["capacity", "outage", "percentOutage"]
REQUIRED_FIELDS_FACILITY = [
    "period",
    "facility",
    "facilityName",
    "capacity",
    "outage",
    "percentOutage",
]
REQUIRED_FIELDS_US = ["period", "capacity", "outage", "percentOutage"]

# Column Mapping (API names → database names)
COLUMN_MAPPING = {
    "period": "date",
    "facility": "facility_id",
    "facilityName": "facility_name",
    "percentOutage": "percent_outage",
}

# Parquet Storage Paths
PLANTS_FILE = str(STORAGE_DIR / "plants.parquet")
FACILITY_OUTAGES_FILE = str(STORAGE_DIR / "facility_outages.parquet")
US_OUTAGES_FILE = str(STORAGE_DIR / "us_outages.parquet")

# Delta Table Configuration
DELTA_DIR = str(STORAGE_DIR / "delta")
PLANTS_DELTA = str(STORAGE_DIR / "delta" / "plants")
FACILITY_OUTAGES_DELTA = str(STORAGE_DIR / "delta" / "facility_outages")
US_OUTAGES_DELTA = str(STORAGE_DIR / "delta" / "us_outages")
STATE_FILE = str(STORAGE_DIR / "delta" / "state.json")
