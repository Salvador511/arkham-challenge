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
    FACILITY_OUTAGES_DELTA,
    FACILITY_OUTAGES_FILE,
    FACILITY_OUTAGES_URL,
    MAX_RETRIES,
    PAGE_SIZE,
    PLANTS_DELTA,
    PLANTS_FILE,
    REQUIRED_FIELDS_FACILITY,
    REQUIRED_FIELDS_US,
    RETRY_DELAY,
    US_OUTAGES_DELTA,
    US_OUTAGES_FILE,
    US_OUTAGES_URL,
)
from deltalake import DeltaTable
from dotenv import load_dotenv
from exceptions import APIError, InvalidAPIKeyError, NetworkError
from state_manager import (
    delta_tables_exist,
    load_state,
    merge_dataframes,
    save_delta,
    save_state,
    vacuum_delta,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def fetch_page(
    url: str,
    api_key: str,
    offset: int,
    length: int,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict | None:
    """
    Fetch a single page from EIA API with automatic retries.

    Args:
        url: API endpoint URL
        api_key: EIA API key
        offset: Pagination offset
        length: Number of records to fetch
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)

    Returns:
        Parsed JSON response or None if all retries fail
    """
    params = {
        "api_key": api_key,
        "data[]": DATA_FIELDS,
        "offset": offset,
        "length": length,
    }

    if start_date:
        params["start"] = start_date
    if end_date:
        params["end"] = end_date

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=API_TIMEOUT)

            if response.status_code in (401, 403):
                raise InvalidAPIKeyError("Invalid API credentials. Check EIA_API_KEY.")

            if response.status_code == 200:
                return response.json()

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

            logger.error("API returned %s after all retries", response.status_code)
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


def fetch_all_data(
    url: str,
    api_key: str,
    dataset_name: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch all data from API using pagination.

    Args:
        url: API endpoint URL
        api_key: EIA API key
        dataset_name: Human-readable dataset name
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)

    Returns:
        List of all records
    """
    mode_str = f" since {start_date}" if start_date else ""
    logger.info("Fetching %s%s...", dataset_name, mode_str)

    all_records = []
    offset = 0
    total = None

    while True:
        payload = fetch_page(
            url=url,
            api_key=api_key,
            offset=offset,
            length=PAGE_SIZE,
            start_date=start_date,
            end_date=end_date,
        )

        if payload is None:
            logger.error("Failed to fetch page at offset %s", offset)
            break

        response_data = payload.get("response", {})
        records = response_data.get("data", [])
        total = int(response_data.get("total", 0))

        if not records:
            break

        all_records.extend(records)

        if offset + PAGE_SIZE >= total:
            break

        offset += PAGE_SIZE

    logger.info("Total fetched for %s: %s rows", dataset_name, len(all_records))
    return all_records


def fetch_last_data(
    url: str, api_key: str, dataset_name: str, start_date: str
) -> list[dict[str, Any]]:
    """
    Fetch incremental data from API since last extraction.

    Args:
        url: API endpoint URL
        api_key: EIA API key
        dataset_name: Human-readable dataset name
        start_date: Only return records with period >= start_date (YYYY-MM-DD)

    Returns:
        List of records since start_date
    """
    logger.info("Fetching incremental %s since %s...", dataset_name, start_date)

    filtered_records = fetch_all_data(url, api_key, dataset_name, start_date=start_date)

    logger.info("API returned %s new/modified records", len(filtered_records))
    return filtered_records


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
    valid_records = [r for r in records if validate_record(r, required_fields)]
    logger.info("Valid records: %s / %s", len(valid_records), len(records))

    if not valid_records:
        logger.warning("No valid records found!")
        return pd.DataFrame()

    df = pd.DataFrame(valid_records)

    df = df.rename(columns=COLUMN_MAPPING)

    # Convert numeric fields
    df["capacity"] = pd.to_numeric(df["capacity"], errors="coerce")
    df["outage"] = pd.to_numeric(df["outage"], errors="coerce")
    df["percent_outage"] = pd.to_numeric(df["percent_outage"], errors="coerce")

    if dataset_type == "facility":
        df = df[["date", "facility_id", "facility_name", "capacity", "outage", "percent_outage"]]
    else:  # us_outages
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
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        df.to_parquet(filepath, index=False)
        logger.info("Saved %s rows to %s", len(df), filepath)
    except Exception as exc:
        logger.error("Failed to save parquet file: %s", exc)
        raise


def save_final_output(facility_delta_path: str, us_delta_path: str, plants_delta_path: str) -> None:
    """
    Save final output Parquets from complete Delta tables for backend consumption.

    Args:
        facility_delta_path: Path to facility_outages Delta table
        us_delta_path: Path to us_outages Delta table
        plants_delta_path: Path to plants Delta table
    """
    try:
        facility_df = DeltaTable(facility_delta_path).to_pandas()
        us_df = DeltaTable(us_delta_path).to_pandas()
        plants_df = DeltaTable(plants_delta_path).to_pandas()

        if len(facility_df) > 0:
            facility_df.to_parquet(FACILITY_OUTAGES_FILE, index=False)
            logger.info("Saved final output: %s (%s rows)", FACILITY_OUTAGES_FILE, len(facility_df))
        else:
            logger.warning("Skipping empty facility_outages DataFrame")

        if len(us_df) > 0:
            us_df.to_parquet(US_OUTAGES_FILE, index=False)
            logger.info("Saved final output: %s (%s rows)", US_OUTAGES_FILE, len(us_df))
        else:
            logger.warning("Skipping empty us_outages DataFrame")

        if len(plants_df) > 0:
            plants_df.to_parquet(PLANTS_FILE, index=False)
            logger.info("Saved final output: %s (%s rows)", PLANTS_FILE, len(plants_df))
        else:
            logger.warning("Skipping empty plants DataFrame")

    except Exception as exc:
        logger.error("Failed to save final output: %s", exc)
        raise


def run_full_extraction(api_key: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Full extraction for first run: fetch all data and initialize Delta tables.

    Args:
        api_key: EIA API key

    Returns:
        Tuple of (facility_df, us_df, plants_df)
    """
    logger.info("First run detected - performing FULL extraction")
    logger.info("-" * 70)

    facility_records = fetch_all_data(
        url=FACILITY_OUTAGES_URL,
        api_key=api_key,
        dataset_name="Facility Nuclear Outages",
    )
    facility_df = transform_data(facility_records, "facility", REQUIRED_FIELDS_FACILITY)

    plants_df = extract_plants(facility_df)

    facility_df = facility_df[["date", "facility_id", "capacity", "outage", "percent_outage"]]

    us_records = fetch_all_data(
        url=US_OUTAGES_URL,
        api_key=api_key,
        dataset_name="US Nuclear Outages",
    )
    us_df = transform_data(us_records, "us", REQUIRED_FIELDS_US)

    save_delta(plants_df, PLANTS_DELTA, mode="overwrite")
    save_delta(facility_df, FACILITY_OUTAGES_DELTA, mode="overwrite")
    save_delta(us_df, US_OUTAGES_DELTA, mode="overwrite")

    save_final_output(FACILITY_OUTAGES_DELTA, US_OUTAGES_DELTA, PLANTS_DELTA)

    state = {
        "facility_outages": {
            "last_extraction_date": facility_df["date"].max() if len(facility_df) > 0 else None
        },
        "us_outages": {"last_extraction_date": us_df["date"].max() if len(us_df) > 0 else None},
        "plants": {
            "last_extraction_date": facility_df["date"].max() if len(facility_df) > 0 else None
        },
    }
    save_state(state)

    return facility_df, us_df, plants_df


def run_incremental_extraction(
    api_key: str, state: dict
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Incremental extraction for subsequent runs: fetch only new/modified data.

    Args:
        api_key: EIA API key
        state: Current state with last extraction dates

    Returns:
        Tuple of (facility_df, us_df, plants_df)
    """
    logger.info("Incremental run detected - fetching since last extraction")
    logger.info("-" * 70)

    facility_last_date = state["facility_outages"]["last_extraction_date"]
    us_last_date = state["us_outages"]["last_extraction_date"]

    facility_records = fetch_last_data(
        url=FACILITY_OUTAGES_URL,
        api_key=api_key,
        dataset_name="Facility Nuclear Outages",
        start_date=facility_last_date,
    )
    facility_df = transform_data(facility_records, "facility", REQUIRED_FIELDS_FACILITY)

    us_records = fetch_last_data(
        url=US_OUTAGES_URL,
        api_key=api_key,
        dataset_name="US Nuclear Outages",
        start_date=us_last_date,
    )
    us_df = transform_data(us_records, "us", REQUIRED_FIELDS_US)

    plants_df = extract_plants(facility_df)

    # Remove facility_name from facility_df only if not empty
    if len(facility_df) > 0:
        facility_df = facility_df[["date", "facility_id", "capacity", "outage", "percent_outage"]]

    if len(facility_df) > 0:
        merge_dataframes(FACILITY_OUTAGES_DELTA, facility_df, merge_keys=["date", "facility_id"])
        vacuum_delta(FACILITY_OUTAGES_DELTA)
        logger.info("Merged and vacuumed facility outages")

    if len(us_df) > 0:
        merge_dataframes(US_OUTAGES_DELTA, us_df, merge_keys=["date"])
        vacuum_delta(US_OUTAGES_DELTA)
        logger.info("Merged and vacuumed US outages")

    if len(plants_df) > 0:
        merge_dataframes(PLANTS_DELTA, plants_df, merge_keys=["facility_id"])
        vacuum_delta(PLANTS_DELTA)
        logger.info("Merged and vacuumed plants")

    if len(facility_df) > 0:
        state["facility_outages"]["last_extraction_date"] = facility_df["date"].max()
    if len(us_df) > 0:
        state["us_outages"]["last_extraction_date"] = us_df["date"].max()
    save_state(state)

    save_final_output(FACILITY_OUTAGES_DELTA, US_OUTAGES_DELTA, PLANTS_DELTA)

    return facility_df, us_df, plants_df


def print_summary(
    is_first_run: bool, facility_df: pd.DataFrame, us_df: pd.DataFrame, plants_df: pd.DataFrame
) -> None:
    """
    Print extraction summary.

    Args:
        is_first_run: Whether this was the first run
        facility_df: Facility outages DataFrame
        us_df: US outages DataFrame
        plants_df: Plants DataFrame
    """
    logger.info("=" * 70)
    logger.info("EXTRACTION COMPLETED")
    logger.info("=" * 70)
    print(f"\n✓ Extraction mode:   {'FULL (first run)' if is_first_run else 'INCREMENTAL'}")
    print(f"✓ Facility outages:  {len(facility_df)} rows processed")
    print(f"✓ US outages:        {len(us_df)} rows processed")
    print(f"✓ Plants:            {len(plants_df)} rows processed")
    print()


def main() -> None:
    """Main entry point."""
    load_dotenv()

    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        raise InvalidAPIKeyError("Missing EIA_API_KEY environment variable.")

    logger.info("=" * 70)
    logger.info("Starting EIA Data Connector")
    logger.info("=" * 70)

    try:
        is_first_run = not delta_tables_exist()

        if is_first_run:
            facility_df, us_df, plants_df = run_full_extraction(api_key)
        else:
            state = load_state()
            facility_df, us_df, plants_df = run_incremental_extraction(api_key, state)

        print_summary(is_first_run, facility_df, us_df, plants_df)

    except (InvalidAPIKeyError, APIError, NetworkError) as exc:
        logger.error("Extraction failed: %s", exc)
        raise


if __name__ == "__main__":
    main()
