"""
Refresh Service - Handles data extraction triggering and status tracking.

Manages the extraction workflow including:
- Lock mechanism to prevent concurrent extractions
- Wrapping connector's main extraction logic
- Error handling and logging
"""

import logging
import time
from pathlib import Path

from connector.extract_data import main as connector_main
from connector.state_manager import delta_tables_exist
from exceptions import (
    APIError,
    ExtractionError,
    ExtractionLocked,
    ExtractionMessages,
    InvalidAPIKeyError,
    NetworkError,
)

logger = logging.getLogger(__name__)

# Lock file to prevent concurrent extractions
LOCK_FILE = Path(__file__).parent.parent / ".extraction_lock"
LOCK_TIMEOUT = 600  # 10 minutes - max expected extraction time


def acquire_lock() -> bool:
    """
    Attempt to acquire extraction lock.

    Returns:
        True if lock acquired, False if already locked
    """
    if LOCK_FILE.exists():
        # Check if lock is stale (timeout)
        lock_age = time.time() - LOCK_FILE.stat().st_mtime
        if lock_age > LOCK_TIMEOUT:
            logger.warning(f"Stale lock detected (age: {lock_age:.0f}s), removing...")
            LOCK_FILE.unlink()
        else:
            return False

    # Create lock file
    LOCK_FILE.touch()
    logger.info("Extraction lock acquired")
    return True


def release_lock() -> None:
    """Release extraction lock."""
    if LOCK_FILE.exists():
        LOCK_FILE.unlink()
        logger.info("Extraction lock released")


def is_extraction_in_progress() -> bool:
    """
    Check if an extraction is currently in progress.

    Returns:
        True if extraction is in progress, False otherwise
    """
    if not LOCK_FILE.exists():
        return False

    # Check if lock is stale
    lock_age = time.time() - LOCK_FILE.stat().st_mtime
    if lock_age > LOCK_TIMEOUT:
        logger.warning(f"Stale lock detected (age: {lock_age:.0f}s), removing...")
        LOCK_FILE.unlink()
        return False

    return True


def run_extraction() -> dict:
    """
    Execute data extraction (full or incremental).

    Calls connector's main() which automatically determines
    extraction type and handles all logic.

    Returns:
        Dictionary with extraction results and statistics
    """
    if not acquire_lock():
        raise ExtractionLocked("Extraction already in progress")

    try:
        # Call connector's main() - it handles all the logic
        connector_main()

        return {
            "status": "success",
            "message": "Data extraction completed successfully",
        }

    except (InvalidAPIKeyError, APIError, NetworkError) as exc:
        logger.error(f"Extraction failed: {exc}")
        raise ExtractionError(f"Extraction failed: {exc}") from exc
    finally:
        release_lock()


def trigger_extraction_async(background_tasks) -> tuple[dict, int]:
    """
    Trigger extraction - async for FULL, sync for INCREMENTAL.

    Determines extraction type and executes accordingly:
    - FULL: background async (returns 202)
    - INCREMENTAL: synchronous (returns 200 with summary)

    Args:
        background_tasks: FastAPI BackgroundTasks instance

    Returns:
        Tuple of (response_dict, http_status_code)
    """
    # Check if already extracting
    if is_extraction_in_progress():
        return {
            "status": "processing",
            "message": ExtractionMessages.EXTRACTION_IN_PROGRESS,
            "retry_after_seconds": ExtractionMessages.EXTRACTION_IN_PROGRESS_RETRY_SECONDS,
        }, 202

    # Determine extraction type
    is_full = not delta_tables_exist()

    if is_full:
        # FULL extraction: async in background
        background_tasks.add_task(run_extraction)
        return {
            "status": "processing",
            "extraction_type": "full",
            "message": ExtractionMessages.FULL_EXTRACTION_STARTED,
            "retry_after_seconds": ExtractionMessages.FULL_EXTRACTION_RETRY_SECONDS,
        }, 202
    else:
        # INCREMENTAL extraction: synchronous (wait for completion)
        try:
            run_extraction()
            return {
                "status": "success",
                "extraction_type": "incremental",
                "message": ExtractionMessages.INCREMENTAL_EXTRACTION_SUCCESS,
            }, 200
        except ExtractionError as exc:
            return {
                "status": "error",
                "error": "ExtractionError",
                "message": str(exc),
            }, 500
        except Exception:
            return {
                "status": "error",
                "error": "UnexpectedError",
                "message": ExtractionMessages.UNEXPECTED_ERROR,
            }, 500
