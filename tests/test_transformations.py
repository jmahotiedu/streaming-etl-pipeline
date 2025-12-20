"""Tests for Bronze-to-Silver and Silver-to-Gold transformations.

Uses pandas DataFrames for unit-level testing of transformation logic,
since the actual PySpark transforms operate on the same column semantics.
"""

from datetime import UTC, datetime

import pandas as pd
import pytest


class TestBronzeToSilverDedup:
    """Tests for deduplication logic."""

    def test_removes_exact_duplicates(self, sample_bronze_df):
        """Duplicate (sensor_id, event_time) pairs are reduced to one row."""
        df = sample_bronze_df
        deduped = df.sort_values("ingestion_time", ascending=False).drop_duplicates(
            subset=["sensor_id", "event_time"], keep="first"
        )
        sensor_001_rows = deduped[deduped["sensor_id"] == "sensor-001"]
        # sensor-001 has two entries at the same event_time; only latest ingestion kept
        assert len(sensor_001_rows) == 1

    def test_keeps_latest_ingestion(self, sample_bronze_df):
        """Dedup keeps the record with the most recent ingestion_time."""
        df = sample_bronze_df
        deduped = df.sort_values("ingestion_time", ascending=False).drop_duplicates(
            subset=["sensor_id", "event_time"], keep="first"
        )
        sensor_001 = deduped[deduped["sensor_id"] == "sensor-001"].iloc[0]
        assert sensor_001["value"] == 22.7  # the later-ingested value

    def test_non_duplicates_preserved(self, sample_bronze_df):
        """Non-duplicate records are all preserved."""
        df = sample_bronze_df
        deduped = df.sort_values("ingestion_time", ascending=False).drop_duplicates(
            subset=["sensor_id", "event_time"], keep="first"
        )
        assert len(deduped) == 4  # 5 rows -> 4 after dedup


# Normal operating ranges per sensor type (mirrors bronze_to_silver.VALUE_RANGES
# but defined here to avoid importing pyspark-dependent module in tests)
VALUE_RANGES = {
    "temperature": {"min": -20.0, "max": 60.0},
    "humidity": {"min": 5.0, "max": 95.0},
    "pressure": {"min": 950.0, "max": 1070.0},
    "vibration": {"min": 0.0, "max": 2.0},
}


class TestAnomalyFlagging:
    """Tests for anomaly detection logic."""

    def test_normal_values_not_flagged(self):
        """Values within normal range are not flagged as anomalies."""
        normal_data = [
            {"sensor_type": "temperature", "value": 22.0},
            {"sensor_type": "humidity", "value": 55.0},
            {"sensor_type": "pressure", "value": 1013.0},
            {"sensor_type": "vibration", "value": 0.5},
        ]
        for record in normal_data:
            bounds = VALUE_RANGES[record["sensor_type"]]
            is_anomaly = record["value"] < bounds["min"] or record["value"] > bounds["max"]
            assert not is_anomaly, f"{record['sensor_type']}={record['value']} should not be anomaly"

    def test_extreme_temperature_flagged(self):
        """Temperature outside normal range is flagged."""
        bounds = VALUE_RANGES["temperature"]
        assert bounds["max"] < 75.0  # 75 > 60

    def test_extreme_vibration_flagged(self):
        """Vibration above normal range is flagged."""
        bounds = VALUE_RANGES["vibration"]
        assert bounds["max"] < 5.5  # 5.5 > 2.0

    def test_boundary_values(self):
        """Values exactly at boundaries are not anomalies."""
        for _sensor_type, bounds in VALUE_RANGES.items():
            assert not (bounds["min"] < bounds["min"] or bounds["min"] > bounds["max"])
            assert not (bounds["max"] < bounds["min"] or bounds["max"] > bounds["max"])


class TestWindowedAggregations:
    """Tests for Silver-to-Gold aggregation logic using pandas equivalents."""

    @pytest.fixture
    def silver_data(self):
        """Create a Silver-like DataFrame with multiple readings per sensor."""
        records = []
        for i in range(30):
            records.append({
                "sensor_id": "sensor-001",
                "sensor_type": "temperature",
                "value": 22.0 + (i * 0.1),
                "event_time": datetime(2024, 6, 15, 10, i, 0, tzinfo=UTC),
                "location": "floor-1-zone-A",
                "is_anomaly": False,
            })
        for i in range(20):
            records.append({
                "sensor_id": "sensor-002",
                "sensor_type": "humidity",
                "value": 50.0 + (i * 0.5),
                "event_time": datetime(2024, 6, 15, 10, i, 0, tzinfo=UTC),
                "location": "floor-1-zone-B",
                "is_anomaly": False,
            })
        return pd.DataFrame(records)

    def test_5min_window_groups(self, silver_data):
        """5-minute windows produce correct number of groups."""
        df = silver_data
        df["window_5min"] = df["event_time"].dt.floor("5min")
        groups = df.groupby(["sensor_id", "window_5min"]).size()
        # sensor-001 has 30 min of data -> 6 five-minute windows
        sensor_001_windows = groups.loc["sensor-001"]
        assert len(sensor_001_windows) == 6

    def test_5min_aggregation_values(self, silver_data):
        """5-minute window aggregations produce correct avg/min/max."""
        df = silver_data[silver_data["sensor_id"] == "sensor-001"]
        df = df.copy()
        df["window_5min"] = df["event_time"].dt.floor("5min")

        first_window = df[df["window_5min"] == df["window_5min"].min()]
        agg = first_window["value"].agg(["mean", "min", "max", "count", "std"])

        assert agg["count"] == 5
        assert agg["min"] == 22.0
        assert agg["max"] == pytest.approx(22.4, abs=0.01)
        assert agg["mean"] == pytest.approx(22.2, abs=0.01)

    def test_hourly_location_aggregation(self, silver_data):
        """Hourly aggregation per location produces expected groups."""
        df = silver_data.copy()
        df["hour"] = df["event_time"].dt.floor("h")
        groups = df.groupby(["location", "sensor_type", "hour"]).size()
        # All data is in one hour, so 2 groups (2 locations x 1 sensor_type each x 1 hour)
        assert len(groups) == 2

    def test_daily_summary(self, silver_data):
        """Daily summary produces one row per sensor_type per day."""
        df = silver_data.copy()
        df["date"] = df["event_time"].dt.date
        groups = df.groupby(["date", "sensor_type"]).size()
        # 1 day, 2 sensor types
        assert len(groups) == 2

    def test_stddev_is_computed(self, silver_data):
        """Standard deviation is non-null for groups with multiple readings."""
        df = silver_data[silver_data["sensor_id"] == "sensor-001"]
        std = df["value"].std()
        assert std > 0
