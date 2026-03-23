"""
Refresh Service - Handles data extraction triggering and status tracking.

Manages the extraction workflow including:
- Lock mechanism to prevent concurrent extractions
- Wrapping connector's main extraction logic
- Error handling and logging
"""

import logging
import os
import time
from pathlib import Path

from app.core.exceptions import (
    APIError,
    ExtractionError,
    ExtractionLocked,
    ExtractionMessages,
    InvalidAPIKeyError,
    NetworkError,
)
from connector.extract_data import main as connector_main
from connector.state_manager import delta_tables_exist

logger = logging.getLogger(__name__)

# Lock file to prevent concurrent extractions
LOCK_FILE = Path(__file__).parent.parent / ".extraction_lock"
LOCK_TIMEOUT = 600  # 10 minutes - max expected extraction time


def acquire_lock() -> bool:
    """
    Attempt to acquire extraction lock using atomic file creation.

    Uses os.open with O_CREAT | O_EXCL for atomic operation (no race condition).

    Returns:
        True if lock acquired, False if already locked
    """
    try:
        # Atomic file creation: fails if already exists
        fd = os.open(str(LOCK_FILE), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
        logger.info("Extraction lock acquired")
        return True
    except FileExistsError:
        # Lock file already exists - check if stale
        lock_age = time.time() - LOCK_FILE.stat().st_mtime
        if lock_age > LOCK_TIMEOUT:
            logger.warning(f"Stale lock detected (age: {lock_age:.0f}s), removing...")
            LOCK_FILE.unlink()
            # Retry recursively
            return acquire_lock()
        return False


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


def _run_extraction_background() -> None:
    """
    Run extraction in background (assumes lock is already acquired).

    Used for both FULL and INCREMENTAL extraction background tasks.
    Lock is already acquired by trigger_extraction_async.
    """
    try:
        connector_main()
        logger.info("✅ Background extraction completed")
    except Exception as exc:
        logger.error(f"Background extraction failed: {exc}")
    finally:
        release_lock()


def trigger_extraction_async(background_tasks) -> tuple[dict, int]:
    """
    Trigger extraction in background for both FULL and INCREMENTAL.

    Determines extraction type and executes accordingly:
    - FULL: background async (returns 202) - acquires lock BEFORE queuing task
    - INCREMENTAL: background async (returns 202) - acquires lock BEFORE queuing task

    Args:
        background_tasks: FastAPI BackgroundTasks instance

    Returns:
        Tuple of (response_dict, http_status_code)
    """
    # Determine extraction type first
    is_full = not delta_tables_exist()

    # Acquire lock BEFORE adding task to queue
    if not acquire_lock():
        return {
            "status": "processing",
            "message": ExtractionMessages.EXTRACTION_IN_PROGRESS,
            "retry_after_seconds": ExtractionMessages.EXTRACTION_IN_PROGRESS_RETRY_SECONDS,
        }, 202

    if is_full:
        # FULL extraction: async in background
        background_tasks.add_task(_run_extraction_background)
        return {
            "status": "processing",
            "extraction_type": "full",
            "message": ExtractionMessages.FULL_EXTRACTION_STARTED,
            "retry_after_seconds": ExtractionMessages.FULL_EXTRACTION_RETRY_SECONDS,
        }, 202
    else:
        # INCREMENTAL extraction: async in background
        background_tasks.add_task(_run_extraction_background)
        return {
            "status": "processing",
            "extraction_type": "incremental",
            "message": ExtractionMessages.INCREMENTAL_EXTRACTION_STARTED,
            "retry_after_seconds": ExtractionMessages.INCREMENTAL_EXTRACTION_RETRY_SECONDS,
        }, 202


def get_extraction_status() -> tuple[dict, int]:
    """
    Get current extraction status for polling.

    Returns:
        Tuple of (response_dict, http_status_code)
    """
    if is_extraction_in_progress():
        return {
            "status": "processing",
            "message": ExtractionMessages.EXTRACTION_IN_PROGRESS,
            "retry_after_seconds": ExtractionMessages.EXTRACTION_IN_PROGRESS_RETRY_SECONDS,
        }, 200

    return {
        "status": "idle",
        "message": "No extraction in progress",
    }, 200
