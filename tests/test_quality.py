"""Tests for Great Expectations validation logic."""


import pandas as pd

from src.quality.expectations import validate_bronze_dataframe, validate_silver_dataframe


class TestBronzeValidation:
    """Tests for Bronze layer data quality validation."""

    def test_valid_bronze_data_passes(self, sample_sensor_events):
        """Valid Bronze data passes all checks."""
        df = pd.DataFrame(sample_sensor_events)
        result = validate_bronze_dataframe(df)
        assert result["success"] is True

    def test_null_sensor_id_fails(self, sample_sensor_events):
        """Null sensor_id fails validation."""
        df = pd.DataFrame(sample_sensor_events)
        df.loc[0, "sensor_id"] = None
        result = validate_bronze_dataframe(df)
        failed_checks = [r["check"] for r in result["results"] if not r["success"]]
        assert "sensor_id_not_null" in failed_checks

    def test_null_timestamp_fails(self, sample_sensor_events):
        """Null timestamp fails validation."""
        df = pd.DataFrame(sample_sensor_events)
        df.loc[0, "timestamp"] = None
        result = validate_bronze_dataframe(df)
        failed_checks = [r["check"] for r in result["results"] if not r["success"]]
        assert "timestamp_not_null" in failed_checks

    def test_unknown_sensor_type_fails(self, sample_sensor_events):
        """Unknown sensor_type fails validation."""
        df = pd.DataFrame(sample_sensor_events)
        df.loc[0, "sensor_type"] = "radiation"
        result = validate_bronze_dataframe(df)
        failed_checks = [r["check"] for r in result["results"] if not r["success"]]
        assert "sensor_type_valid" in failed_checks

    def test_empty_dataframe_fails(self):
        """Empty DataFrame fails validation."""
        df = pd.DataFrame()
        result = validate_bronze_dataframe(df)
        assert result["success"] is False

    def test_high_null_rate_fails(self):
        """More than 1% nulls in a column fails."""
        data = [
            {"sensor_id": f"sensor-{i:03d}", "sensor_type": "temperature",
             "timestamp": "2024-01-01T00:00:00", "value": 22.0,
             "unit": "celsius", "location": None}
            for i in range(100)
        ]
        # Set 5 location values to non-null, leaving 95% null
        for i in range(5):
            data[i]["location"] = "floor-1-zone-A"
        df = pd.DataFrame(data)
        result = validate_bronze_dataframe(df)
        failed_checks = [r["check"] for r in result["results"] if not r["success"]]
        assert "null_rate_below_1pct" in failed_checks

    def test_all_sensor_types_valid(self):
        """All four sensor types are accepted."""
        data = []
        for st in ["temperature", "humidity", "pressure", "vibration"]:
            data.append({
                "sensor_id": "sensor-001", "sensor_type": st,
                "timestamp": "2024-01-01T00:00:00", "value": 22.0,
                "unit": "celsius", "location": "floor-1-zone-A",
            })
        df = pd.DataFrame(data)
        result = validate_bronze_dataframe(df)
        type_check = [r for r in result["results"] if r["check"] == "sensor_type_valid"][0]
        assert type_check["success"] is True


class TestSilverValidation:
    """Tests for Silver layer data quality validation."""

    def test_valid_silver_data_passes(self, sample_silver_df):
        """Valid Silver data passes all checks."""
        result = validate_silver_dataframe(sample_silver_df)
        assert result["success"] is True

    def test_duplicate_records_fail(self, sample_silver_df):
        """Duplicated (sensor_id, event_time) pairs fail validation."""
        df = pd.concat([sample_silver_df, sample_silver_df.iloc[[0]]], ignore_index=True)
        result = validate_silver_dataframe(df)
        failed_checks = [r["check"] for r in result["results"] if not r["success"]]
        assert "no_duplicates" in failed_checks

    def test_missing_anomaly_column_fails(self, sample_silver_df):
        """Missing is_anomaly column fails validation."""
        df = sample_silver_df.drop(columns=["is_anomaly"])
        result = validate_silver_dataframe(df)
        failed_checks = [r["check"] for r in result["results"] if not r["success"]]
        assert "is_anomaly_exists" in failed_checks

    def test_row_count_within_5pct_passes(self, sample_silver_df):
        """Silver row count within 5% of Bronze count passes."""
        result = validate_silver_dataframe(sample_silver_df, bronze_count=5)
        coverage_check = [r for r in result["results"] if r["check"] == "row_count_coverage"]
        assert len(coverage_check) == 1
        assert coverage_check[0]["success"] is True

    def test_row_count_outside_5pct_fails(self, sample_silver_df):
        """Silver row count far from Bronze count fails coverage check."""
        result = validate_silver_dataframe(sample_silver_df, bronze_count=100)
        coverage_check = [r for r in result["results"] if r["check"] == "row_count_coverage"]
        assert len(coverage_check) == 1
        assert coverage_check[0]["success"] is False

    def test_empty_dataframe_fails(self):
        """Empty DataFrame fails validation."""
        df = pd.DataFrame()
        result = validate_silver_dataframe(df)
        assert result["success"] is False
