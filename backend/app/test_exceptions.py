"""Unit tests for custom exceptions."""

from app.core.exceptions import (
    APIException,
    DataNotFoundError,
    ProcessingError,
    ValidationError,
)


class TestAPIException:
    """Tests for base APIException."""

    def test_api_exception_default_status_code(self):
        """Test that APIException defaults to 500 status code."""
        exc = APIException("Test error")
        assert exc.status_code == 500
        assert exc.message == "Test error"

    def test_api_exception_custom_status_code(self):
        """Test that APIException accepts custom status code."""
        exc = APIException("Test error", status_code=503)
        assert exc.status_code == 503

    def test_api_exception_is_exception(self):
        """Test that APIException is an Exception."""
        exc = APIException("Test")
        assert isinstance(exc, Exception)


class TestValidationError:
    """Tests for ValidationError."""

    def test_validation_error_status_code(self):
        """Test that ValidationError has status code 400."""
        exc = ValidationError("Invalid input")
        assert exc.status_code == 400
        assert exc.message == "Invalid input"

    def test_validation_error_is_api_exception(self):
        """Test that ValidationError is an APIException."""
        exc = ValidationError("Invalid input")
        assert isinstance(exc, APIException)

    def test_validation_error_message(self):
        """Test ValidationError message is preserved."""
        message = "Field 'date' is required"
        exc = ValidationError(message)
        assert str(exc) == f"[400] {message}"


class TestDataNotFoundError:
    """Tests for DataNotFoundError."""

    def test_data_not_found_status_code(self):
        """Test that DataNotFoundError has status code 404."""
        exc = DataNotFoundError("Resource not found")
        assert exc.status_code == 404
        assert exc.message == "Resource not found"

    def test_data_not_found_is_api_exception(self):
        """Test that DataNotFoundError is an APIException."""
        exc = DataNotFoundError("Resource not found")
        assert isinstance(exc, APIException)


class TestProcessingError:
    """Tests for ProcessingError."""

    def test_processing_error_status_code(self):
        """Test that ProcessingError has status code 500."""
        exc = ProcessingError("Process failed")
        assert exc.status_code == 500
        assert exc.message == "Process failed"

    def test_processing_error_is_api_exception(self):
        """Test that ProcessingError is an APIException."""
        exc = ProcessingError("Process failed")
        assert isinstance(exc, APIException)
