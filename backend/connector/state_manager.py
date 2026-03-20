"""State management for incremental data extraction using Delta Lake."""

import json
import logging
import os
from pathlib import Path

import pandas as pd
from config import (
    DELTA_DIR,
    FACILITY_OUTAGES_DELTA,
    PLANTS_DELTA,
    STATE_FILE,
    US_OUTAGES_DELTA,
)
from deltalake import DeltaTable, write_deltalake

logger = logging.getLogger(__name__)


def delta_tables_exist() -> bool:
    """
    Check if Delta tables have been created.

    Returns:
        True if all Delta tables exist, False otherwise
    """
    return (
        Path(FACILITY_OUTAGES_DELTA).exists()
        and Path(US_OUTAGES_DELTA).exists()
        and Path(PLANTS_DELTA).exists()
    )


def load_state() -> dict:
    """
    Load extraction state (last extraction date).

    Returns:
        Dictionary with last extraction dates for each dataset
    """
    if not Path(STATE_FILE).exists():
        logger.warning("State file not found, returning empty state")
        return {
            "facility_outages": {"last_extraction_date": None},
            "us_outages": {"last_extraction_date": None},
            "plants": {"last_extraction_date": None},
        }

    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception as exc:
        logger.error("Failed to load state: %s", exc)
        raise


def save_state(state: dict) -> None:
    """
    Save extraction state (last extraction date).

    Args:
        state: Dictionary with extraction metadata

    Raises:
        Exception: If save fails
    """
    try:
        os.makedirs(DELTA_DIR, exist_ok=True)

        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
        logger.info("State saved to %s", STATE_FILE)
    except Exception as exc:
        logger.error("Failed to save state: %s", exc)
        raise


def save_delta(df: pd.DataFrame, table_path: str, mode: str = "overwrite") -> None:
    """
    Save DataFrame as Delta table.

    Args:
        df: DataFrame to save
        table_path: Path where to save Delta table
        mode: Write mode - 'overwrite' or 'append'

    Raises:
        Exception: If save fails
    """
    try:
        os.makedirs(DELTA_DIR, exist_ok=True)

        write_deltalake(table_path, df, mode=mode)
        logger.info("Saved %s rows to Delta table: %s", len(df), table_path)
    except Exception as exc:
        logger.error("Failed to save Delta table: %s", exc)
        raise


def merge_dataframes(table_path: str, new_df: pd.DataFrame, merge_keys: list[str]) -> None:
    """
    Merge new data into Delta table.

    Args:
        table_path: Path to Delta table
        new_df: New data to merge
        merge_keys: Column names to use as merge key (facility_id, date)

    Raises:
        Exception: If merge fails
    """
    try:
        if not Path(table_path).exists():
            write_deltalake(table_path, new_df, mode="overwrite")
            logger.info("Created new Delta table: %s", table_path)
            return

        try:
            delta_table = DeltaTable(table_path)
        except Exception as e:
            logger.warning("Delta table is corrupted or invalid, recreating: %s", e)
            write_deltalake(table_path, new_df, mode="overwrite")
            logger.info("Recreated Delta table: %s", table_path)
            return

        merge_predicate = " AND ".join([f"t.{key} = s.{key}" for key in merge_keys])

        (
            delta_table.merge(new_df, predicate=merge_predicate, target_alias="t", source_alias="s")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )

        logger.info("Merged %s rows into Delta table: %s", len(new_df), table_path)

    except Exception as exc:
        logger.error("Failed to merge data into Delta table: %s", exc)
        raise


def vacuum_delta(table_path: str) -> None:
    """
    Clean up obsolete files from Delta table after merge.

    Args:
        table_path: Path to Delta table

    Raises:
        Exception: If vacuum fails
    """
    try:
        if not Path(table_path).exists():
            logger.warning("Delta table not found for vacuum: %s", table_path)
            return

        delta_table = DeltaTable(table_path)
        delta_table.vacuum(retention_hours=168)
        logger.info("Vacuumed Delta table: %s", table_path)

    except Exception as exc:
        logger.error("Failed to vacuum Delta table: %s", exc)
        raise
