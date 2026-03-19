"""Unit tests for connector validation functions."""

# Import from the connector module
import sys
from pathlib import Path

# Agregar el path del connector para importar
sys.path.insert(0, str(Path(__file__).parent / ".." / "connector"))

# Después vamos a testear las funciones


class TestValidateRecord:
    """Tests for validate_record function."""

    def test_validate_record_with_all_fields(self):
        """Test that record with all required fields passes validation."""
        # Esta función se encuentra en extract_data.py
        # Vamos a importarla y testearla
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
            # missing facility_name
        }
        required_fields = ["date", "facility_id", "facility_name"]

        assert validate_record(record, required_fields) is False

    def test_validate_record_empty_field(self):
        """Test that record with empty required field fails validation."""
        from connector.extract_data import validate_record

        record = {
            "date": "2024-01-01",
            "facility_id": "F001",
            "facility_name": "",  # empty
        }
        required_fields = ["date", "facility_id", "facility_name"]

        assert validate_record(record, required_fields) is False

    def test_validate_record_none_field(self):
        """Test that record with None required field fails validation."""
        from connector.extract_data import validate_record

        record = {
            "date": "2024-01-01",
            "facility_id": "F001",
            "facility_name": None,  # None
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

        # Should have 3 unique plants (F001, F002, F003)
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

        # Check there are no duplicate facility_ids
        assert len(plants_df) == len(plants_df["facility_id"].unique())
