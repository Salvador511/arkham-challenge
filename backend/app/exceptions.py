"""Custom exceptions for the API"""


class APIException(Exception):
    """Base exception for API errors"""

    def __init__(self, message: str, status_code: int = 500):
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)


class ValidationError(APIException):
    """Invalid input parameters (400)"""

    def __init__(self, message: str):
        super().__init__(message, status_code=400)


class DataNotFoundError(APIException):
    """Dataset or resource not found (404)"""

    def __init__(self, message: str):
        super().__init__(message, status_code=404)


class ProcessingError(APIException):
    """Error processing data (500)"""

    def __init__(self, message: str):
        super().__init__(message, status_code=500)
