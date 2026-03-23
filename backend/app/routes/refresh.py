"""Refresh endpoints - POST /refresh"""

import logging

from fastapi import APIRouter, BackgroundTasks, Response

from app.core.exceptions import ExtractionError, ExtractionLocked, ExtractionMessages
from services.refresh_service import get_extraction_status, trigger_extraction_async

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Refresh"])


@router.post("/refresh", summary="Trigger full or incremental extraction")
async def refresh_data(background_tasks: BackgroundTasks, response: Response):
    """
    Trigger data extraction and refresh.

    This endpoint handles:
    - First-time full extraction (takes several minutes, returns 202)
    - Incremental extraction for subsequent runs (runs in background, returns 202)
    - Prevents concurrent extractions

    Returns:
    - 202 Accepted: For accepted FULL or INCREMENTAL extraction
    - 500 Internal Server Error: If extraction fails

    Response structure:
    - status: "processing" | "success" | "error"
    - extraction_type: "full" | "incremental"
    - message: Human-readable status message
    - retry_after_seconds: When to retry if still processing (202 only)
    """
    try:
        result, status_code = trigger_extraction_async(background_tasks)
        response.status_code = status_code

        return result

    except ExtractionLocked:
        logger.warning("Extraction requested while already in progress")
        response.status_code = 202
        return {
            "status": "processing",
            "message": ExtractionMessages.EXTRACTION_IN_PROGRESS,
            "retry_after_seconds": ExtractionMessages.EXTRACTION_IN_PROGRESS_RETRY_SECONDS,
        }
    except ExtractionError as exc:
        logger.error(f"Extraction error: {exc}")
        response.status_code = 500
        return {
            "status": "error",
            "error": "ExtractionError",
            "message": str(exc),
        }
    except Exception as exc:
        logger.error(f"Unexpected error during refresh: {exc}")
        response.status_code = 500
        return {
            "status": "error",
            "error": "InternalServerError",
            "message": ExtractionMessages.UNEXPECTED_ERROR,
        }


@router.get("/refresh/status", summary="Get extraction status")
async def refresh_status(response: Response):
    """Get current extraction status for polling clients."""
    result, status_code = get_extraction_status()
    response.status_code = status_code
    return result
