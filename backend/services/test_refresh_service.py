"""Unit tests for RefreshService."""

import os
import time
from unittest.mock import MagicMock, patch

import pytest

from app.core.exceptions import (
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
    get_extraction_status,
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
        release_lock()
        assert not LOCK_FILE.exists()


class TestStaleLocknRemoval:
    """Tests for stale lock detection and removal."""

    def test_stale_lock_removed_on_acquire(self, cleanup_lock):
        """Test that stale lock is removed when acquiring."""
        LOCK_FILE.touch()
        old_time = time.time() - (LOCK_TIMEOUT + 100)
        os.utime(LOCK_FILE, (old_time, old_time))

        assert acquire_lock() is True

    def test_stale_lock_detected_by_is_extraction_in_progress(self, cleanup_lock):
        """Test that is_extraction_in_progress removes stale lock."""
        LOCK_FILE.touch()
        old_time = time.time() - (LOCK_TIMEOUT + 100)
        os.utime(LOCK_FILE, (old_time, old_time))

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


class TestExtractionStatus:
    """Tests for extraction polling status responses."""

    def test_get_extraction_status_processing_when_locked(self, cleanup_lock):
        """Status endpoint should report processing when lock exists."""
        acquire_lock()

        response, status_code = get_extraction_status()

        assert status_code == 200
        assert response["status"] == "processing"
        assert "retry_after_seconds" in response

    def test_get_extraction_status_idle_when_unlocked(self, cleanup_lock):
        """Status endpoint should report idle when no extraction is running."""
        response, status_code = get_extraction_status()

        assert status_code == 200
        assert response["status"] == "idle"


class TestRunExtraction:
    """Tests for extraction execution."""

    @patch("services.refresh_service.connector_main")
    def test_run_extraction_success(self, mock_connector, cleanup_lock):
        """Test successful extraction run."""
        result = run_extraction()

        assert result["status"] == "success"
        assert "message" in result
        mock_connector.assert_called_once()
        assert not LOCK_FILE.exists()

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

        assert not LOCK_FILE.exists()

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
    def test_full_extraction_returns_202(self, mock_connector, mock_delta_exists, cleanup_lock):
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
    def test_incremental_extraction_returns_202_and_runs_in_background(
        self, mock_connector, mock_delta_exists, cleanup_lock
    ):
        """Test that incremental extraction is accepted and queued in background."""
        mock_delta_exists.return_value = True

        background_tasks = MagicMock()

        response, status_code = trigger_extraction_async(background_tasks)

        assert status_code == 202
        assert response["status"] == "processing"
        assert response["extraction_type"] == "incremental"
        background_tasks.add_task.assert_called_once()
        mock_connector.assert_not_called()

    @patch("services.refresh_service.delta_tables_exist")
    @patch("services.refresh_service.connector_main")
    def test_incremental_extraction_does_not_execute_connector_in_request_thread(
        self, mock_connector, mock_delta_exists, cleanup_lock
    ):
        """Test that incremental extraction is queued even if connector would fail."""
        mock_delta_exists.return_value = True
        mock_connector.side_effect = InvalidAPIKeyError()

        background_tasks = MagicMock()

        response, status_code = trigger_extraction_async(background_tasks)

        assert status_code == 202
        assert response["status"] == "processing"
        assert response["extraction_type"] == "incremental"
        background_tasks.add_task.assert_called_once()
        mock_connector.assert_not_called()

    @patch("services.refresh_service.delta_tables_exist")
    def test_extraction_already_in_progress_returns_202(self, mock_delta_exists, cleanup_lock):
        """Test that locked extraction returns 202 Accepted."""
        acquire_lock()
        background_tasks = MagicMock()

        response, status_code = trigger_extraction_async(background_tasks)

        assert status_code == 202
        assert response["status"] == "processing"
        assert "Extraction already in progress" in response["message"]

    @patch("services.refresh_service.delta_tables_exist")
    def test_incremental_extraction_locked_returns_202(self, mock_delta_exists, cleanup_lock):
        """Test that incremental extraction returns processing when lock is held."""
        mock_delta_exists.return_value = True
        acquire_lock()

        background_tasks = MagicMock()

        response, status_code = trigger_extraction_async(background_tasks)

        assert status_code == 202
        assert response["status"] == "processing"
