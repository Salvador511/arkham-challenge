"""Configuration constants for EIA Data Connector."""

# API Configuration
FACILITY_OUTAGES_URL = "https://api.eia.gov/v2/nuclear-outages/facility-nuclear-outages/data/"
US_OUTAGES_URL = "https://api.eia.gov/v2/nuclear-outages/us-nuclear-outages/data/"
API_TIMEOUT = 30
MAX_RETRIES = 2
RETRY_DELAY = 1

# Data Configuration
PAGE_SIZE = 5000
DATA_FIELDS = ["capacity", "outage", "percentOutage"]
REQUIRED_FIELDS_FACILITY = ["period", "facility", "facilityName", "capacity", "outage", "percentOutage"]
REQUIRED_FIELDS_US = ["period", "capacity", "outage", "percentOutage"]

# Column Mapping (API names → database names)
COLUMN_MAPPING = {
    "period": "date",
    "facility": "facility_id",
    "facilityName": "facility_name",
    "percentOutage": "percent_outage",
}

# Storage Configuration
STORAGE_DIR = "storage"
PLANTS_FILE = f"{STORAGE_DIR}/plants.parquet"
FACILITY_OUTAGES_FILE = f"{STORAGE_DIR}/facility_outages.parquet"
US_OUTAGES_FILE = f"{STORAGE_DIR}/us_outages.parquet"
