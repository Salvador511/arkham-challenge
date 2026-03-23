"""Unit tests for state_manager functions."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / ".." / "connector"))


class TestVacuumDelta:
    """Tests for vacuum_delta function."""

    def test_vacuum_delta_success(self, mocker):
        """Test successful vacuum of Delta table."""
        from connector.state_manager import vacuum_delta

        mock_delta_table = mocker.MagicMock()
        mock_delta_class = mocker.patch("connector.state_manager.DeltaTable")
        mock_delta_class.return_value = mock_delta_table

        mocker.patch("connector.state_manager.Path.exists", return_value=True)

        vacuum_delta("/path/to/delta/table")

        mock_delta_class.assert_called_once_with("/path/to/delta/table")
        mock_delta_table.vacuum.assert_called_once_with(retention_hours=168)

    def test_vacuum_delta_table_not_found(self, mocker):
        """Test vacuum_delta handles missing table gracefully."""
        from connector.state_manager import vacuum_delta

        mocker.patch("connector.state_manager.Path.exists", return_value=False)

        vacuum_delta("/nonexistent/path/to/delta/table")

    def test_vacuum_delta_handles_exception(self, mocker):
        """Test vacuum_delta handles exceptions."""
        from connector.state_manager import vacuum_delta

        mock_delta_class = mocker.patch("connector.state_manager.DeltaTable")
        mock_delta_class.side_effect = Exception("Delta error")

        mocker.patch("connector.state_manager.Path.exists", return_value=True)

        try:
            vacuum_delta("/path/to/delta/table")
            raise AssertionError("Expected exception to be raised")
        except Exception as e:
            assert "Delta error" in str(e)


class TestMergeDataframes:
    """Tests for merge_dataframes function."""

    def test_merge_dataframes_success(self, sample_facility_dataframe, mocker):
        """Test successful merge of new data into Delta table."""
        from connector.state_manager import merge_dataframes

        mock_delta_table = mocker.MagicMock()
        mock_merge_builder = mocker.MagicMock()

        mock_delta_table.merge.return_value = mock_merge_builder
        mock_merge_builder.when_matched_update_all.return_value = mock_merge_builder
        mock_merge_builder.when_not_matched_insert_all.return_value = mock_merge_builder

        mock_delta_class = mocker.patch("connector.state_manager.DeltaTable")
        mock_delta_class.return_value = mock_delta_table

        mocker.patch("connector.state_manager.Path.exists", return_value=True)

        merge_dataframes(
            "/path/to/delta/table", sample_facility_dataframe, merge_keys=["date", "facility_id"]
        )

        assert mock_delta_table.merge.called
        assert mock_merge_builder.when_matched_update_all.called
        assert mock_merge_builder.when_not_matched_insert_all.called
        assert mock_merge_builder.execute.called

    def test_merge_dataframes_table_not_exists(self, sample_facility_dataframe, mocker):
        """Test merge_dataframes creates table if it doesn't exist."""
        from connector.state_manager import merge_dataframes

        mock_write = mocker.patch("connector.state_manager.write_deltalake")

        mocker.patch("connector.state_manager.Path.exists", return_value=False)

        merge_dataframes(
            "/path/to/delta/table", sample_facility_dataframe, merge_keys=["date", "facility_id"]
        )

        mock_write.assert_called_once()

    def test_merge_dataframes_corrupted_table(self, sample_facility_dataframe, mocker):
        """Test merge_dataframes recreates corrupted table."""
        from connector.state_manager import merge_dataframes

        mock_delta_class = mocker.patch("connector.state_manager.DeltaTable")
        mock_delta_class.side_effect = Exception("Corrupted table")

        mock_write = mocker.patch("connector.state_manager.write_deltalake")

        mocker.patch("connector.state_manager.Path.exists", return_value=True)

        merge_dataframes(
            "/path/to/delta/table", sample_facility_dataframe, merge_keys=["date", "facility_id"]
        )

        mock_write.assert_called_once()


class TestDeltaTablesExist:
    """Tests for delta_tables_exist function."""

    def test_delta_tables_exist_all_present(self, mocker):
        """Test delta_tables_exist returns True when all tables exist."""
        from connector.state_manager import delta_tables_exist

        mock_exists = mocker.patch("connector.state_manager.Path.exists")
        mock_exists.return_value = True

        result = delta_tables_exist()

        assert result is True

    def test_delta_tables_exist_some_missing(self, mocker):
        """Test delta_tables_exist returns False when any table is missing."""
        from connector.state_manager import delta_tables_exist

        mock_exists = mocker.patch("connector.state_manager.Path.exists")
        mock_exists.side_effect = [False, True, True]

        result = delta_tables_exist()

        assert result is False


class TestLoadState:
    """Tests for load_state function."""

    def test_load_state_success(self, tmp_path, mocker):
        """Test successful state loading."""
        import json

        from connector.state_manager import load_state

        state_file = tmp_path / "state.json"
        state_data = {
            "facility_outages": {"last_extraction_date": "2024-01-01"},
            "us_outages": {"last_extraction_date": "2024-01-01"},
            "plants": {"last_extraction_date": "F001"},
        }
        state_file.write_text(json.dumps(state_data))

        mocker.patch("connector.state_manager.STATE_FILE", str(state_file))

        result = load_state()

        assert result["facility_outages"]["last_extraction_date"] == "2024-01-01"

    def test_load_state_file_not_found(self, mocker):
        """Test load_state handles missing state file."""
        from connector.state_manager import load_state

        mocker.patch("connector.state_manager.STATE_FILE", "/nonexistent/state.json")

        try:
            load_state()
            raise AssertionError("Expected exception")
        except Exception:
            pass


class TestSaveState:
    """Tests for save_state function."""

    def test_save_state_success(self, tmp_path, mocker):
        """Test successful state saving."""

        from connector.state_manager import save_state

        state_data = {
            "facility_outages": {"last_extraction_date": "2024-01-01"},
            "us_outages": {"last_extraction_date": "2024-01-01"},
        }

        state_file = tmp_path / "state.json"
        mocker.patch("connector.state_manager.STATE_FILE", str(state_file))

        mock_open = mocker.patch("builtins.open", mocker.mock_open())

        save_state(state_data)

        assert mock_open.called
