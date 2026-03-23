"""Unit tests for connector validation functions."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / ".." / "connector"))


class TestValidateRecord:
    """Tests for validate_record function."""

    def test_validate_record_with_all_fields(self):
        """Test that record with all required fields passes validation."""
        from connector.extract_data import validate_record

        record = {
            "date": "2024-01-01",
            "facility_id": "F001",
            "facility_name": "Plant A",
            "capacity": 1000.0,
            "outage": 100.0,
            "percent_outage": 10.0,
        }
        required_fields = ["date", "facility_id", "facility_name"]

        assert validate_record(record, required_fields) is True

    def test_validate_record_missing_field(self):
        """Test that record missing required field fails validation."""
        from connector.extract_data import validate_record

        record = {
            "date": "2024-01-01",
            "facility_id": "F001",
        }
        required_fields = ["date", "facility_id", "facility_name"]

        assert validate_record(record, required_fields) is False

    def test_validate_record_empty_field(self):
        """Test that record with empty required field fails validation."""
        from connector.extract_data import validate_record

        record = {
            "date": "2024-01-01",
            "facility_id": "F001",
            "facility_name": "",
        }
        required_fields = ["date", "facility_id", "facility_name"]

        assert validate_record(record, required_fields) is False

    def test_validate_record_none_field(self):
        """Test that record with None required field fails validation."""
        from connector.extract_data import validate_record

        record = {
            "date": "2024-01-01",
            "facility_id": "F001",
            "facility_name": None,
        }
        required_fields = ["date", "facility_id", "facility_name"]

        assert validate_record(record, required_fields) is False

    def test_validate_record_no_required_fields(self):
        """Test validation with empty required fields list."""
        from connector.extract_data import validate_record

        record = {"some_field": "value"}

        assert validate_record(record, []) is True


class TestExtractPlants:
    """Tests for extract_plants function."""

    def test_extract_plants_unique_facilities(self, sample_facility_dataframe):
        """Test that extract_plants returns unique facility combinations."""
        from connector.extract_data import extract_plants

        plants_df = extract_plants(sample_facility_dataframe)

        assert len(plants_df) == 3
        assert list(plants_df.columns) == ["facility_id", "facility_name"]

    def test_extract_plants_sorted_by_id(self, sample_facility_dataframe):
        """Test that plants are sorted by facility_id."""
        from connector.extract_data import extract_plants

        plants_df = extract_plants(sample_facility_dataframe)

        facility_ids = plants_df["facility_id"].tolist()
        assert facility_ids == sorted(facility_ids)

    def test_extract_plants_no_duplicates(self, sample_facility_dataframe):
        """Test that extract_plants removes duplicates."""
        from connector.extract_data import extract_plants

        plants_df = extract_plants(sample_facility_dataframe)

        assert len(plants_df) == len(plants_df["facility_id"].unique())


class TestRunFullExtraction:
    """Tests for run_full_extraction function."""

    def test_run_full_extraction_success(
        self, sample_facility_dataframe, sample_us_dataframe, mocker
    ):
        """Test successful full extraction on first run."""
        from connector.extract_data import run_full_extraction

        mocker.patch("connector.extract_data.fetch_all_data")
        mocker.patch("connector.extract_data.transform_data")
        mocker.patch("connector.extract_data.extract_plants")
        mocker.patch("connector.extract_data.save_delta")
        mocker.patch("connector.extract_data.save_final_output")
        mocker.patch("connector.extract_data.save_state")

        import connector.extract_data as extract_module

        extract_module.transform_data.side_effect = [
            sample_facility_dataframe,
            sample_us_dataframe,
        ]
        extract_module.extract_plants.return_value = sample_facility_dataframe[
            ["facility_id", "facility_name"]
        ].drop_duplicates()

        facility_df, us_df, plants_df = run_full_extraction("test_api_key")

        assert facility_df is not None
        assert us_df is not None
        assert plants_df is not None

        assert extract_module.save_delta.call_count == 3
        assert extract_module.save_final_output.called
        assert extract_module.save_state.called


class TestRunIncrementalExtraction:
    """Tests for run_incremental_extraction function."""

    def test_run_incremental_extraction_with_data(
        self, sample_facility_dataframe, sample_us_dataframe, mocker
    ):
        """Test incremental extraction with new data."""
        from connector.extract_data import run_incremental_extraction

        mocker.patch("connector.extract_data.fetch_last_data")
        mocker.patch("connector.extract_data.transform_data")
        mocker.patch("connector.extract_data.extract_plants")
        mocker.patch("connector.extract_data.merge_dataframes")
        mocker.patch("connector.extract_data.vacuum_delta")
        mocker.patch("connector.extract_data.save_final_output")
        mocker.patch("connector.extract_data.save_state")

        import connector.extract_data as extract_module

        extract_module.transform_data.side_effect = [
            sample_facility_dataframe,
            sample_us_dataframe,
        ]
        extract_module.extract_plants.return_value = sample_facility_dataframe[
            ["facility_id", "facility_name"]
        ].drop_duplicates()

        state = {
            "facility_outages": {"last_extraction_date": "2024-01-01"},
            "us_outages": {"last_extraction_date": "2024-01-01"},
        }

        facility_df, us_df, plants_df = run_incremental_extraction("test_api_key", state)

        assert facility_df is not None
        assert us_df is not None

        assert extract_module.merge_dataframes.called
        assert extract_module.vacuum_delta.called
        assert extract_module.save_final_output.called

    def test_run_incremental_extraction_empty_data(self, mocker):
        """Test incremental extraction with no new data."""
        import pandas as pd

        from connector.extract_data import run_incremental_extraction

        mocker.patch("connector.extract_data.fetch_last_data", return_value=[])
        mocker.patch("connector.extract_data.transform_data", return_value=pd.DataFrame())
        mocker.patch("connector.extract_data.extract_plants", return_value=pd.DataFrame())
        mocker.patch("connector.extract_data.save_final_output")
        mocker.patch("connector.extract_data.save_state")

        state = {
            "facility_outages": {"last_extraction_date": "2024-01-01"},
            "us_outages": {"last_extraction_date": "2024-01-01"},
        }

        facility_df, us_df, plants_df = run_incremental_extraction("test_api_key", state)

        assert len(facility_df) == 0
        assert len(us_df) == 0


class TestSaveFinalOutput:
    """Tests for save_final_output function."""

    def test_save_final_output_success(
        self, sample_facility_dataframe, sample_us_dataframe, sample_plants_dataframe, mocker
    ):
        """Test saving final output from Delta tables."""
        from connector.extract_data import save_final_output

        mock_delta_table = mocker.MagicMock()
        mock_delta_class = mocker.patch("connector.extract_data.DeltaTable")
        mock_delta_class.side_effect = [
            mock_delta_table,
            mock_delta_table,
            mock_delta_table,
        ]

        mock_delta_table.to_pandas.side_effect = [
            sample_facility_dataframe,
            sample_us_dataframe,
            sample_plants_dataframe,
        ]

        mocker.patch("pandas.DataFrame.to_parquet")

        save_final_output("/delta/path", "/us/path", "/plants/path")

        assert mock_delta_class.call_count == 3
        assert mock_delta_table.to_pandas.call_count == 3

    def test_save_final_output_with_empty_dataframe(self, mocker):
        """Test save_final_output skips empty DataFrames."""
        import pandas as pd

        from connector.extract_data import save_final_output

        mock_delta_table = mocker.MagicMock()
        mock_delta_class = mocker.patch("connector.extract_data.DeltaTable")
        mock_delta_class.side_effect = [
            mock_delta_table,
            mock_delta_table,
            mock_delta_table,
        ]

        mock_delta_table.to_pandas.side_effect = [
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
        ]

        mocker.patch("pandas.DataFrame.to_parquet")

        save_final_output("/delta/path", "/us/path", "/plants/path")


class TestPrintSummary:
    """Tests for print_summary function."""

    def test_print_summary_first_run(self, sample_facility_dataframe, capsys):
        """Test print_summary displays first run info."""
        from connector.extract_data import print_summary

        print_summary(
            True,
            sample_facility_dataframe,
            sample_facility_dataframe[:2],
            sample_facility_dataframe[:3],
        )

        captured = capsys.readouterr()
        assert "FULL (first run)" in captured.out
        assert "5 rows processed" in captured.out

    def test_print_summary_incremental_run(self, sample_facility_dataframe, capsys):
        """Test print_summary displays incremental run info."""
        from connector.extract_data import print_summary

        print_summary(
            False,
            sample_facility_dataframe,
            sample_facility_dataframe[:1],
            sample_facility_dataframe[:2],
        )

        captured = capsys.readouterr()
        assert "INCREMENTAL" in captured.out
        assert "5 rows processed" in captured.out
