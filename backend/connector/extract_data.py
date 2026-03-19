"""
EIA Data Connector - Extract Nuclear Outages data from EIA Open Data API.

This script extracts nuclear outages data for both Facility and US datasets,
validates and transforms the data, and saves it to Parquet files.

Usage:
    python extract_data.py

Requirements:
    - EIA_API_KEY environment variable must be set
"""

import logging
import os
import time
from typing import Any

import pandas as pd
import requests
from config import (
    API_TIMEOUT,
    COLUMN_MAPPING,
    DATA_FIELDS,
    FACILITY_OUTAGES_FILE,
    FACILITY_OUTAGES_URL,
    MAX_RETRIES,
    PAGE_SIZE,
    PLANTS_FILE,
    REQUIRED_FIELDS_FACILITY,
    REQUIRED_FIELDS_US,
    RETRY_DELAY,
    US_OUTAGES_FILE,
    US_OUTAGES_URL,
)
from dotenv import load_dotenv
from exceptions import APIError, InvalidAPIKeyError, NetworkError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def fetch_page(url: str, api_key: str, offset: int, length: int) -> dict | None:
    """
    Fetch a single page from EIA API with automatic retries.

    Args:
        url: API endpoint URL
        api_key: EIA API key
        offset: Pagination offset
        length: Number of records to fetch

    Returns:
        Parsed JSON response or None if all retries fail
    """
    params = {
        "api_key": api_key,
        "data[]": DATA_FIELDS,
        "offset": offset,
        "length": length,
    }

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=API_TIMEOUT)

            # Check for invalid credentials
            if response.status_code in (401, 403):
                raise InvalidAPIKeyError(
                    "Invalid API credentials. Check EIA_API_KEY."
                )

            # Success
            if response.status_code == 200:
                return response.json()

            # Retry on server error
            if attempt < MAX_RETRIES:
                logger.warning(
                    "API returned %s at offset %s. Retrying %s/%s...",
                    response.status_code,
                    offset,
                    attempt + 1,
                    MAX_RETRIES,
                )
                time.sleep(RETRY_DELAY)
                continue

            raise APIError(f"API failed with status {response.status_code}")

        except requests.exceptions.RequestException as exc:
            if attempt < MAX_RETRIES:
                logger.warning(
                    "Network error at offset %s (%s). Retrying %s/%s...",
                    offset,
                    exc,
                    attempt + 1,
                    MAX_RETRIES,
                )
                time.sleep(RETRY_DELAY)
                continue

            raise NetworkError(f"Network error after {MAX_RETRIES} retries: {exc}") from exc

    return None


def fetch_all_data(url: str, api_key: str, dataset_name: str) -> list[dict[str, Any]]:
    """
    Fetch all data from API using pagination.

    Args:
        url: API endpoint URL
        api_key: EIA API key
        dataset_name: Human-readable dataset name

    Returns:
        List of all records
    """
    logger.info("Fetching %s...", dataset_name)

    all_records = []
    offset = 0
    total = None

    while True:
        payload = fetch_page(url=url, api_key=api_key, offset=offset, length=PAGE_SIZE)

        if payload is None:
            logger.error("Failed to fetch page at offset %s", offset)
            break

        response_data = payload.get("response", {})
        records = response_data.get("data", [])
        total = int(response_data.get("total", 0))

        if not records:
            break

        all_records.extend(records)
        logger.info("Fetched %s / %s rows", len(all_records), total)

        # Stop if we've got all records
        if offset + PAGE_SIZE >= total:
            break

        offset += PAGE_SIZE

    logger.info("Total fetched for %s: %s rows", dataset_name, len(all_records))
    return all_records


def validate_record(record: dict, required_fields: list) -> bool:
    """
    Validate that record has all required fields.

    Args:
        record: Record to validate
        required_fields: List of field names that must be present

    Returns:
        True if valid, False otherwise
    """
    missing = [f for f in required_fields if f not in record or record[f] in (None, "")]

    if missing:
        logger.warning("Skipping record with missing fields: %s", missing)
        return False

    return True


def transform_data(records: list[dict], dataset_type: str, required_fields: list) -> pd.DataFrame:
    """
    Validate, transform, and enrich records.

    Args:
        records: List of raw records from API
        dataset_type: 'facility' or 'us' to determine output columns
        required_fields: List of required field names for validation

    Returns:
        Transformed DataFrame
    """
    # Filter valid records
    valid_records = [r for r in records if validate_record(r, required_fields)]
    logger.info("Valid records: %s / %s", len(valid_records), len(records))

    if not valid_records:
        logger.warning("No valid records found!")
        return pd.DataFrame()

    # Convert to DataFrame
    df = pd.DataFrame(valid_records)

    # Rename columns using config mapping
    df = df.rename(columns=COLUMN_MAPPING)

    # Convert numeric fields
    df["capacity"] = pd.to_numeric(df["capacity"], errors="coerce")
    df["outage"] = pd.to_numeric(df["outage"], errors="coerce")
    df["percent_outage"] = pd.to_numeric(df["percent_outage"], errors="coerce")

    if dataset_type == "facility":
        # Select final columns for facility_outages (keep facility_name for plants extraction)
        df = df[["date", "facility_id", "facility_name", "capacity", "outage", "percent_outage"]]
    else:  # us_outages
        # Select final columns for us_outages
        df = df[["date", "capacity", "outage", "percent_outage"]]

    return df


def extract_plants(facility_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract unique plants from facility_outages data.

    Args:
        facility_df: Facility outages DataFrame

    Returns:
        Plants DataFrame with facility_id, facility_name
    """
    # Get unique combinations of facility_id and facility_name
    df = facility_df[["facility_id", "facility_name"]].drop_duplicates()
    df = df.sort_values("facility_id").reset_index(drop=True)

    logger.info("Extracted %s unique plants", len(df))
    return df


def save_parquet(df: pd.DataFrame, filepath: str) -> None:
    """
    Save DataFrame to Parquet file.

    Args:
        df: DataFrame to save
        filepath: Path where to save

    Raises:
        Exception: If save fails
    """
    try:
        # Create storage directory if needed
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        df.to_parquet(filepath, index=False)
        logger.info("Saved %s rows to %s", len(df), filepath)
    except Exception as exc:
        logger.error("Failed to save parquet file: %s", exc)
        raise


def main() -> None:
    """Main entry point."""
    # Load environment
    load_dotenv()

    # Get API key
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        raise InvalidAPIKeyError("Missing EIA_API_KEY environment variable.")

    logger.info("=" * 70)
    logger.info("Starting EIA Data Connector")
    logger.info("=" * 70)

    try:
        # Fetch and process facility outages
        facility_records = fetch_all_data(
            url=FACILITY_OUTAGES_URL,
            api_key=api_key,
            dataset_name="Facility Nuclear Outages",
        )
        facility_df = transform_data(facility_records, "facility", REQUIRED_FIELDS_FACILITY)

        # Extract and save unique plants (before removing facility_name)
        plants_df = extract_plants(facility_df)
        save_parquet(plants_df, PLANTS_FILE)

        # Remove facility_name from facility_df before saving
        facility_df = facility_df[["date", "facility_id", "capacity", "outage", "percent_outage"]]
        save_parquet(facility_df, FACILITY_OUTAGES_FILE)

        # Fetch and process US outages
        us_records = fetch_all_data(
            url=US_OUTAGES_URL,
            api_key=api_key,
            dataset_name="US Nuclear Outages",
        )
        us_df = transform_data(us_records, "us", REQUIRED_FIELDS_US)
        save_parquet(us_df, US_OUTAGES_FILE)

        # Print summary
        logger.info("=" * 70)
        logger.info("EXTRACTION COMPLETED")
        logger.info("=" * 70)
        print(f"\n✓ Plants:           {len(plants_df)} rows → {PLANTS_FILE}")
        print(f"✓ Facility outages: {len(facility_df)} rows → {FACILITY_OUTAGES_FILE}")
        print(f"✓ US outages:       {len(us_df)} rows → {US_OUTAGES_FILE}")
        print()

    except (InvalidAPIKeyError, APIError, NetworkError) as exc:
        logger.error("Extraction failed: %s", exc)
        raise


if __name__ == "__main__":
    main()
