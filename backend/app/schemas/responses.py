"""Response schemas for standardized API responses"""
from typing import Any

from pydantic import BaseModel


class ErrorResponse(BaseModel):
    """Standardized error response"""

    status: str = "error"
    message: str
    error_code: str | None = None
    details: Any | None = None

    class Config:
        json_schema_extra = {
            "example": {
                "status": "error",
                "message": "Invalid parameter",
                "error_code": "VALIDATION_ERROR",
                "details": None,
            }
        }


class SuccessResponse(BaseModel):
    """Standardized success response wrapper"""

    status: str = "success"
    data: Any
    metadata: dict | None = None

    class Config:
        json_schema_extra = {
            "example": {
                "status": "success",
                "data": {},
                "metadata": None,
            }
        }
