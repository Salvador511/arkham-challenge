"""Response schemas for standardized API responses"""
from typing import Any, Optional

from pydantic import BaseModel


class ErrorResponse(BaseModel):
    """Standardized error response"""

    status: str = "error"
    message: str
    error_code: Optional[str] = None
    details: Optional[Any] = None

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
    metadata: Optional[dict] = None

    class Config:
        json_schema_extra = {
            "example": {
                "status": "success",
                "data": {},
                "metadata": None,
            }
        }
