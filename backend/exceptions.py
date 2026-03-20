"""Centralized exception definitions for the entire project."""


# ============================================================================
# BASE EXCEPTIONS
# ============================================================================


class APIException(Exception):
    """Base exception for all API errors."""

    def __init__(self, message: str, status_code: int = 500):
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"[{self.status_code}] {self.message}"


# ============================================================================
# API ROUTE EXCEPTIONS (400-404 range)
# ============================================================================


class ValidationError(APIException):
    """Raised when input validation fails (400)."""

    def __init__(self, message: str):
        super().__init__(message, status_code=400)


class DataNotFoundError(APIException):
    """Raised when dataset or resource is not found (404)."""

    def __init__(self, message: str):
        super().__init__(message, status_code=404)


# ============================================================================
# PROCESSING EXCEPTIONS (500 range)
# ============================================================================


class ProcessingError(APIException):
    """Raised when processing data fails (500)."""

    def __init__(self, message: str):
        super().__init__(message, status_code=500)


# ============================================================================
# EIA CONNECTOR EXCEPTIONS
# ============================================================================


class EIAConnectorError(Exception):
    """Base exception for EIA Connector operations."""

    def __init__(self, message: str, error_code: str | None = None):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.error_code:
            return f"[{self.error_code}] {self.message}"
        return self.message


class InvalidAPIKeyError(EIAConnectorError):
    """Raised when API key is missing or invalid."""

    MESSAGE = "Invalid or missing EIA API key"

    def __init__(self, message: str = MESSAGE):
        super().__init__(message, error_code="INVALID_API_KEY")


class APIError(EIAConnectorError):
    """Raised when EIA API returns an error."""

    def __init__(self, message: str, status_code: int | None = None):
        self.status_code = status_code
        error_code = f"API_ERROR_{status_code}" if status_code else "API_ERROR"
        super().__init__(message, error_code=error_code)


class NetworkError(EIAConnectorError):
    """Raised when network request fails."""

    MESSAGE = "Network error during request"

    def __init__(self, message: str = MESSAGE):
        super().__init__(message, error_code="NETWORK_ERROR")


class DataValidationError(EIAConnectorError):
    """Raised when record validation fails."""

    MESSAGE = "Failed to validate record"

    def __init__(self, message: str = MESSAGE, invalid_fields: list[str] | None = None):
        self.invalid_fields = invalid_fields or []
        full_message = message
        if self.invalid_fields:
            full_message += f" (Invalid fields: {', '.join(self.invalid_fields)})"
        super().__init__(full_message, error_code="DATA_VALIDATION_ERROR")


# ============================================================================
# EXTRACTION/REFRESH EXCEPTIONS & MESSAGES
# ============================================================================


class ExtractionError(Exception):
    """Raised when data extraction fails."""

    MESSAGE = "Extraction failed"

    def __init__(self, message: str = MESSAGE, error_code: str = "EXTRACTION_ERROR"):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"[{self.error_code}] {self.message}"


class ExtractionLocked(Exception):
    """Raised when an extraction is already in progress."""

    MESSAGE = "Extraction already in progress"
    RETRY_SECONDS = 300

    def __init__(self, message: str = MESSAGE, lock_age_seconds: int | None = None):
        self.message = message
        self.lock_age_seconds = lock_age_seconds
        self.retry_after_seconds = self.RETRY_SECONDS
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.lock_age_seconds:
            return f"{self.message} (locked for {self.lock_age_seconds}s)"
        return self.message


# ============================================================================
# EXTRACTION/REFRESH RESPONSE MESSAGES (Centralized)
# ============================================================================


class ExtractionMessages:
    """Centralized extraction/refresh response messages."""

    # Full Extraction (First Run)
    FULL_EXTRACTION_STARTED = "First extraction is in progress. This may take several minutes (2-5 min)."
    FULL_EXTRACTION_RETRY_SECONDS = 300

    # Incremental Extraction (Subsequent Runs)
    INCREMENTAL_EXTRACTION_SUCCESS = "Incremental extraction completed successfully."
    INCREMENTAL_EXTRACTION_RETRY_SECONDS = 60

    # Locked/In Progress
    EXTRACTION_IN_PROGRESS = "Extraction already in progress. Please try again in 5 minutes."
    EXTRACTION_IN_PROGRESS_RETRY_SECONDS = 300

    # Error Messages
    EXTRACTION_FAILED = "Data extraction failed"
    UNEXPECTED_ERROR = "An unexpected error occurred during extraction"


