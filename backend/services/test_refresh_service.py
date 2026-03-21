"""Unit tests for RefreshService."""

import os
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from exceptions import (
    APIError,
    ExtractionError,
    ExtractionLocked,
    InvalidAPIKeyError,
    NetworkError,
)
from services.refresh_service import (
    LOCK_FILE,
    LOCK_TIMEOUT,
    acquire_lock,
    is_extraction_in_progress,
    release_lock,
    run_extraction,
    trigger_extraction_async,
)


@pytest.fixture
def cleanup_lock():
    """Cleanup lock file before and after test."""
    if LOCK_FILE.exists():
        LOCK_FILE.unlink()
    yield
    if LOCK_FILE.exists():
        LOCK_FILE.unlink()


class TestLockMechanism:
    """Tests for lock acquisition and release."""

    def test_acquire_lock_success(self, cleanup_lock):
        """Test successful lock acquisition."""
        assert acquire_lock() is True
        assert LOCK_FILE.exists()

    def test_acquire_lock_fails_when_locked(self, cleanup_lock):
        """Test that acquire_lock fails when already locked."""
        assert acquire_lock() is True
        assert acquire_lock() is False

    def test_release_lock(self, cleanup_lock):
        """Test lock release."""
        acquire_lock()
        assert LOCK_FILE.exists()
        release_lock()
        assert not LOCK_FILE.exists()

    def test_release_lock_when_no_lock(self, cleanup_lock):
        """Test release_lock doesn't fail when no lock exists."""
        # Should not raise any exception
        release_lock()
        assert not LOCK_FILE.exists()


class TestStaleLocknRemoval:
    """Tests for stale lock detection and removal."""

    def test_stale_lock_removed_on_acquire(self, cleanup_lock):
        """Test that stale lock is removed when acquiring."""
        # Create lock file
        LOCK_FILE.touch()
        # Set modification time to far in the past (older than LOCK_TIMEOUT)
        old_time = time.time() - (LOCK_TIMEOUT + 100)
        os.utime(LOCK_FILE, (old_time, old_time))

        # Should be able to acquire because lock is stale
        assert acquire_lock() is True

    def test_stale_lock_detected_by_is_extraction_in_progress(self, cleanup_lock):
        """Test that is_extraction_in_progress removes stale lock."""
        # Create lock file
        LOCK_FILE.touch()
        # Set modification time to far in the past
        old_time = time.time() - (LOCK_TIMEOUT + 100)
        os.utime(LOCK_FILE, (old_time, old_time))

        # Should return False because lock is stale (will be removed)
        assert is_extraction_in_progress() is False
        assert not LOCK_FILE.exists()

    def test_is_extraction_in_progress_returns_true_for_fresh_lock(self, cleanup_lock):
        """Test that is_extraction_in_progress returns True for fresh lock."""
        acquire_lock()

        assert is_extraction_in_progress() is True


class TestExtractionInProgress:
    """Tests for checking extraction status."""

    def test_no_extraction_in_progress_when_no_lock(self, cleanup_lock):
        """Test that no extraction is reported when no lock exists."""
        assert is_extraction_in_progress() is False

    def test_extraction_in_progress_when_locked(self, cleanup_lock):
        """Test that extraction is reported as in progress when locked."""
        acquire_lock()
        assert is_extraction_in_progress() is True


class TestRunExtraction:
    """Tests for extraction execution."""

    @patch("services.refresh_service.connector_main")
    def test_run_extraction_success(self, mock_connector, cleanup_lock):
        """Test successful extraction run."""
        result = run_extraction()

        assert result["status"] == "success"
        assert "message" in result
        mock_connector.assert_called_once()
        assert not LOCK_FILE.exists()  # Lock released

    @patch("services.refresh_service.connector_main")
    def test_run_extraction_fails_if_already_locked(self, mock_connector, cleanup_lock):
        """Test that run_extraction fails if already locked."""
        acquire_lock()

        with pytest.raises(ExtractionLocked):
            run_extraction()

        mock_connector.assert_not_called()

    @patch("services.refresh_service.connector_main")
    def test_run_extraction_releases_lock_on_error(self, mock_connector, cleanup_lock):
        """Test that lock is released even when extraction fails."""
        mock_connector.side_effect = InvalidAPIKeyError()

        with pytest.raises(ExtractionError):
            run_extraction()

        assert not LOCK_FILE.exists()  # Lock released

    @patch("services.refresh_service.connector_main")
    def test_run_extraction_wraps_invalid_api_key_error(self, mock_connector, cleanup_lock):
        """Test that InvalidAPIKeyError is wrapped in ExtractionError."""
        mock_connector.side_effect = InvalidAPIKeyError("Invalid key")

        with pytest.raises(ExtractionError, match="Extraction failed"):
            run_extraction()

    @patch("services.refresh_service.connector_main")
    def test_run_extraction_wraps_api_error(self, mock_connector, cleanup_lock):
        """Test that APIError is wrapped in ExtractionError."""
        mock_connector.side_effect = APIError("API failed", status_code=500)

        with pytest.raises(ExtractionError, match="Extraction failed"):
            run_extraction()

    @patch("services.refresh_service.connector_main")
    def test_run_extraction_wraps_network_error(self, mock_connector, cleanup_lock):
        """Test that NetworkError is wrapped in ExtractionError."""
        mock_connector.side_effect = NetworkError()

        with pytest.raises(ExtractionError, match="Extraction failed"):
            run_extraction()


class TestTriggerExtractionAsync:
    """Tests for async extraction triggering."""

    @patch("services.refresh_service.delta_tables_exist")
    @patch("services.refresh_service.connector_main")
    def test_full_extraction_returns_202(
        self, mock_connector, mock_delta_exists, cleanup_lock
    ):
        """Test that full extraction returns 202 Accepted."""
        mock_delta_exists.return_value = False
        background_tasks = MagicMock()

        response, status_code = trigger_extraction_async(background_tasks)

        assert status_code == 202
        assert response["status"] == "processing"
        assert response["extraction_type"] == "full"
        background_tasks.add_task.assert_called_once()

    @patch("services.refresh_service.delta_tables_exist")
    @patch("services.refresh_service.connector_main")
    def test_incremental_extraction_returns_200_on_success(
        self, mock_connector, mock_delta_exists, cleanup_lock
    ):
        """Test that successful incremental extraction returns 200."""
        mock_delta_exists.return_value = True

        background_tasks = MagicMock()

        response, status_code = trigger_extraction_async(background_tasks)

        assert status_code == 200
        assert response["status"] == "success"
        assert response["extraction_type"] == "incremental"

    @patch("services.refresh_service.delta_tables_exist")
    @patch("services.refresh_service.connector_main")
    def test_incremental_extraction_returns_500_on_error(
        self, mock_connector, mock_delta_exists, cleanup_lock
    ):
        """Test that failed incremental extraction returns 500."""
        mock_delta_exists.return_value = True
        mock_connector.side_effect = InvalidAPIKeyError()

        background_tasks = MagicMock()

        response, status_code = trigger_extraction_async(background_tasks)

        assert status_code == 500
        assert response["status"] == "error"
        assert response["error"] == "ExtractionError"

    @patch("services.refresh_service.delta_tables_exist")
    def test_extraction_already_in_progress_returns_202(
        self, mock_delta_exists, cleanup_lock
    ):
        """Test that locked extraction returns 202 Accepted."""
        acquire_lock()
        background_tasks = MagicMock()

        response, status_code = trigger_extraction_async(background_tasks)

        assert status_code == 202
        assert response["status"] == "processing"
        assert "Extraction already in progress" in response["message"]

    @patch("services.refresh_service.delta_tables_exist")
    @patch("services.refresh_service.connector_main")
    def test_incremental_extraction_unexpected_error(
        self, mock_connector, mock_delta_exists, cleanup_lock
    ):
        """Test that unexpected errors in incremental extraction return 500."""
        mock_delta_exists.return_value = True
        mock_connector.side_effect = RuntimeError("Unexpected error")

        background_tasks = MagicMock()

        response, status_code = trigger_extraction_async(background_tasks)

        assert status_code == 500
        assert response["status"] == "error"
        assert response["error"] == "UnexpectedError"
