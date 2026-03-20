"""Error handlers - centralized exception handling for the API"""

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.schemas.responses import ErrorResponse
from exceptions import APIException, DataNotFoundError, ProcessingError, ValidationError

logger = logging.getLogger(__name__)


def register_error_handlers(app: FastAPI) -> None:
    """Register all error handlers with the FastAPI app"""

    @app.exception_handler(ValidationError)
    async def validation_error_handler(request: Request, exc: ValidationError):
        logger.warning(f"Validation error: {exc.message}")
        error_response = ErrorResponse(
            status="error",
            message=exc.message,
            error_code="VALIDATION_ERROR",
        )
        return JSONResponse(status_code=exc.status_code, content=error_response.model_dump())

    @app.exception_handler(DataNotFoundError)
    async def data_not_found_handler(request: Request, exc: DataNotFoundError):
        logger.warning(f"Data not found: {exc.message}")
        error_response = ErrorResponse(
            status="error",
            message=exc.message,
            error_code="DATA_NOT_FOUND",
        )
        return JSONResponse(status_code=exc.status_code, content=error_response.model_dump())

    @app.exception_handler(ProcessingError)
    async def processing_error_handler(request: Request, exc: ProcessingError):
        logger.error(f"Processing error: {exc.message}")
        error_response = ErrorResponse(
            status="error",
            message=exc.message,
            error_code="PROCESSING_ERROR",
        )
        return JSONResponse(status_code=exc.status_code, content=error_response.model_dump())

    @app.exception_handler(APIException)
    async def api_exception_handler(request: Request, exc: APIException):
        logger.error(f"API error: {exc.message}")
        error_response = ErrorResponse(
            status="error",
            message=exc.message,
            error_code="INTERNAL_ERROR",
        )
        return JSONResponse(status_code=exc.status_code, content=error_response.model_dump())

    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        logger.exception(f"Unexpected error: {exc}")
        error_response = ErrorResponse(
            status="error",
            message="Internal server error",
            error_code="INTERNAL_SERVER_ERROR",
        )
        return JSONResponse(status_code=500, content=error_response.model_dump())
