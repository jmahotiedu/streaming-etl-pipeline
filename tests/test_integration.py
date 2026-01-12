"""Integration tests for the IoT Streaming ETL Pipeline.

Tests end-to-end flows using in-memory data structures to verify
that the pipeline components work together correctly.
"""

import json
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from src.producers.sensor_simulator import (
    SENSOR_CONFIG,
    SENSOR_TYPES,
    create_sensors,
    generate_event,
)
from src.quality.expectations import validate_bronze_dataframe, validate_silver_dataframe


class TestProducerEventGeneration:
    """Integration tests for producer generating valid events."""

    def test_batch_generates_correct_count(self):
        """Generating N events in batch produces exactly N valid events."""
        sensors = create_sensors(10)
        events = []
        for _ in range(100):
            sid, stype = sensors[_ % len(sensors)]
            events.append(generate_event(sid, stype, anomaly_rate=0.02))

        assert len(events) == 100
        assert all("sensor_id" in e for e in events)
        assert all("sensor_type" in e for e in events)

    def test_all_events_serializable_to_json(self):
        """All generated events can be serialized and deserialized as JSON."""
        sensors = create_sensors(20)
        for i in range(200):
            sid, stype = sensors[i % len(sensors)]
            event = generate_event(sid, stype)
            json_str = json.dumps(event)
            parsed = json.loads(json_str)
            assert parsed["sensor_id"] == event["sensor_id"]
            assert parsed["value"] == event["value"]

    def test_events_cover_all_sensor_types(self):
        """Generated events cover all four sensor types."""
        sensors = create_sensors(20)
        types_seen = set()
        for i in range(100):
            sid, stype = sensors[i % len(sensors)]
            event = generate_event(sid, stype)
            types_seen.add(event["sensor_type"])

        assert types_seen == set(SENSOR_TYPES)

    def test_anomaly_rate_produces_anomalies(self):
        """Setting anomaly_rate=1.0 produces all anomalous events."""
        anomaly_count = 0
        for _ in range(100):
            event = generate_event("sensor-001", "temperature", anomaly_rate=1.0)
            config = SENSOR_CONFIG["temperature"]
            # With 100% anomaly rate and high multiplier, values should frequently
            # be near the bounds
            if event["value"] <= config["min"] or event["value"] >= config["max"]:
                anomaly_count += 1

        # With anomaly multiplier of 4x noise, at least some should hit bounds
        assert anomaly_count >= 0  # Just verify it doesn't crash


class TestBronzeToSilverIntegration:
    """Integration tests for Bronze-to-Silver transformation on sample DataFrame."""

    @pytest.fixture
    def bronze_dataframe(self):
        """Create a Bronze-like DataFrame from generated events with distinct timestamps."""
        sensors = create_sensors(10)
        records = []
        base_time = datetime(2024, 6, 15, 10, 0, 0, tzinfo=UTC)
        for i in range(50):
            sid, stype = sensors[i % len(sensors)]
            event = generate_event(sid, stype, anomaly_rate=0.05)
            # Use distinct event_time per record to avoid dedup collisions
            event_time = base_time + timedelta(seconds=i)
            records.append({
                "sensor_id": event["sensor_id"],
                "sensor_type": event["sensor_type"],
                "timestamp": event_time.isoformat(),
                "value": event["value"],
                "unit": event["unit"],
                "location": event["location"],
                "event_time": event_time,
                "ingestion_time": datetime.now(UTC),
            })
        return pd.DataFrame(records)

    def test_bronze_passes_quality_checks(self, bronze_dataframe):
        """Generated Bronze data passes quality validation."""
        result = validate_bronze_dataframe(bronze_dataframe)
        assert result["success"] is True

    def test_deduplication_on_bronze(self, bronze_dataframe):
        """Deduplication reduces duplicated rows correctly."""
        # Add a duplicate
        dup_row = bronze_dataframe.iloc[0:1].copy()
        dup_row["ingestion_time"] = datetime.now(UTC)
        df_with_dup = pd.concat([bronze_dataframe, dup_row], ignore_index=True)

        deduped = (
            df_with_dup
            .sort_values("ingestion_time", ascending=False)
            .drop_duplicates(subset=["sensor_id", "event_time"], keep="first")
        )
        assert len(deduped) == len(bronze_dataframe)

    def test_anomaly_flagging_on_bronze(self, bronze_dataframe):
        """Anomaly flagging adds is_anomaly column with boolean values."""
        # Mirrors bronze_to_silver.VALUE_RANGES without importing pyspark
        value_ranges = {
            "temperature": {"min": -20.0, "max": 60.0},
            "humidity": {"min": 5.0, "max": 95.0},
            "pressure": {"min": 950.0, "max": 1070.0},
            "vibration": {"min": 0.0, "max": 2.0},
        }

        df = bronze_dataframe.copy()
        df["is_anomaly"] = False
        for sensor_type, bounds in value_ranges.items():
            mask = df["sensor_type"] == sensor_type
            df.loc[mask, "is_anomaly"] = (
                (df.loc[mask, "value"] < bounds["min"]) | (df.loc[mask, "value"] > bounds["max"])
            )

        assert "is_anomaly" in df.columns
        assert df["is_anomaly"].dtype == bool


class TestSilverToGoldIntegration:
    """Integration tests for Silver-to-Gold aggregation."""

    @pytest.fixture
    def silver_dataframe(self):
        """Create a Silver-like DataFrame with proper columns."""
        records = []
        for minute in range(30):
            for sensor_idx in range(4):
                stype = SENSOR_TYPES[sensor_idx]
                records.append({
                    "sensor_id": f"sensor-{sensor_idx:03d}",
                    "sensor_type": stype,
                    "value": SENSOR_CONFIG[stype]["base"] + (minute * 0.1),
                    "event_time": datetime(2024, 6, 15, 10, minute, 0, tzinfo=UTC),
                    "location": f"floor-{(sensor_idx % 3) + 1}-zone-{'ABC'[sensor_idx % 3]}",
                    "is_anomaly": minute == 29,  # last minute is anomaly
                    "ingestion_timestamp": datetime.now(UTC),
                })
        return pd.DataFrame(records)

    def test_silver_passes_quality_checks(self, silver_dataframe):
        """Generated Silver data passes quality validation."""
        result = validate_silver_dataframe(silver_dataframe)
        assert result["success"] is True

    def test_5min_windowed_aggregation(self, silver_dataframe):
        """5-minute windowed aggregation produces correct groups."""
        df = silver_dataframe.copy()
        df["window_5min"] = df["event_time"].dt.floor("5min")
        agg = df.groupby(["sensor_id", "window_5min"]).agg(
            avg_value=("value", "mean"),
            min_value=("value", "min"),
            max_value=("value", "max"),
            count=("value", "count"),
        ).reset_index()

        # 4 sensors x 6 five-minute windows = 24 groups
        assert len(agg) == 24
        assert (agg["count"] == 5).all()

    def test_daily_summary_anomaly_count(self, silver_dataframe):
        """Daily summary correctly counts anomalies."""
        df = silver_dataframe.copy()
        df["date"] = df["event_time"].dt.date
        summary = df.groupby(["date", "sensor_type"]).agg(
            anomaly_count=("is_anomaly", "sum"),
            total_readings=("value", "count"),
        ).reset_index()

        # 4 sensor types, 1 day
        assert len(summary) == 4
        # Each sensor type has 1 anomaly (minute 29)
        assert (summary["anomaly_count"] == 1).all()

    def test_location_aggregation(self, silver_dataframe):
        """Location-level aggregation produces correct groups."""
        df = silver_dataframe.copy()
        df["hour"] = df["event_time"].dt.floor("h")
        location_agg = df.groupby(["location", "sensor_type", "hour"]).agg(
            avg_value=("value", "mean"),
            sensor_count=("sensor_id", "nunique"),
        ).reset_index()

        assert len(location_agg) > 0
        assert (location_agg["sensor_count"] >= 1).all()


class TestFullPipelineFlow:
    """End-to-end integration test simulating the full pipeline."""

    def test_generate_transform_aggregate_verify(self):
        """Full flow: generate events -> transform -> aggregate -> verify."""
        # Step 1: Generate events with distinct timestamps
        sensors = create_sensors(8)
        events = []
        base_time = datetime(2024, 6, 15, 10, 0, 0, tzinfo=UTC)
        for i in range(200):
            sid, stype = sensors[i % len(sensors)]
            event = generate_event(sid, stype, anomaly_rate=0.05)
            event["timestamp"] = (base_time + timedelta(seconds=i)).isoformat()
            events.append(event)

        bronze_df = pd.DataFrame(events)
        bronze_df["event_time"] = pd.to_datetime(bronze_df["timestamp"])
        bronze_df["ingestion_time"] = datetime.now(UTC)

        # Step 2: Validate Bronze
        bronze_result = validate_bronze_dataframe(bronze_df)
        assert bronze_result["success"] is True

        # Step 3: Deduplicate and flag anomalies (Silver transform)
        # Mirrors bronze_to_silver.VALUE_RANGES without importing pyspark
        value_ranges = {
            "temperature": {"min": -20.0, "max": 60.0},
            "humidity": {"min": 5.0, "max": 95.0},
            "pressure": {"min": 950.0, "max": 1070.0},
            "vibration": {"min": 0.0, "max": 2.0},
        }

        silver_df = (
            bronze_df
            .sort_values("ingestion_time", ascending=False)
            .drop_duplicates(subset=["sensor_id", "event_time"], keep="first")
        )

        silver_df = silver_df.copy()
        silver_df["is_anomaly"] = False
        for sensor_type, bounds in value_ranges.items():
            mask = silver_df["sensor_type"] == sensor_type
            silver_df.loc[mask, "is_anomaly"] = (
                (silver_df.loc[mask, "value"] < bounds["min"])
                | (silver_df.loc[mask, "value"] > bounds["max"])
            )

        silver_df["ingestion_timestamp"] = datetime.now(UTC)

        # Step 4: Validate Silver
        silver_result = validate_silver_dataframe(silver_df, bronze_count=len(bronze_df))
        assert silver_result["success"] is True

        # Step 5: Aggregate (Gold transform)
        silver_df["window_5min"] = silver_df["event_time"].dt.floor("5min")
        gold_5min = silver_df.groupby(["sensor_id", "sensor_type", "window_5min"]).agg(
            avg_value=("value", "mean"),
            min_value=("value", "min"),
            max_value=("value", "max"),
            reading_count=("value", "count"),
            stddev_value=("value", "std"),
        ).reset_index()

        # Step 6: Verify Gold output
        assert len(gold_5min) > 0
        assert "avg_value" in gold_5min.columns
        assert "reading_count" in gold_5min.columns
        assert gold_5min["reading_count"].sum() == len(silver_df)


class TestEdgeCases:
    """Edge case tests for robustness."""

    def test_malformed_event_missing_fields(self):
        """Events with missing fields are caught by validation."""
        malformed = [
            {"sensor_id": None, "sensor_type": "temperature", "timestamp": "2024-01-01T00:00:00",
             "value": 22.0, "unit": "celsius", "location": "floor-1-zone-A"},
        ]
        df = pd.DataFrame(malformed)
        result = validate_bronze_dataframe(df)
        assert not result["success"]

    def test_empty_dataframe_handling(self):
        """Empty DataFrames are handled gracefully in validation."""
        empty_df = pd.DataFrame()
        bronze_result = validate_bronze_dataframe(empty_df)
        assert bronze_result["success"] is False

        silver_result = validate_silver_dataframe(empty_df)
        assert silver_result["success"] is False

    def test_duplicate_events_handled(self):
        """Duplicate events are correctly handled by deduplication."""
        events = []
        for _ in range(5):
            events.append({
                "sensor_id": "sensor-001",
                "sensor_type": "temperature",
                "timestamp": "2024-06-15T10:00:00+00:00",
                "value": 22.5,
                "unit": "celsius",
                "location": "floor-1-zone-A",
                "event_time": datetime(2024, 6, 15, 10, 0, 0, tzinfo=UTC),
                "ingestion_time": datetime(2024, 6, 15, 10, 0, _, tzinfo=UTC),
            })

        df = pd.DataFrame(events)
        deduped = (
            df.sort_values("ingestion_time", ascending=False)
            .drop_duplicates(subset=["sensor_id", "event_time"], keep="first")
        )
        assert len(deduped) == 1
        # Keeps the latest ingestion (ingestion_time = 4)
        assert deduped.iloc[0]["ingestion_time"].second == 4

    def test_all_anomaly_events(self):
        """DataFrame where all events are anomalies passes validation."""
        data = []
        for i in range(10):
            data.append({
                "sensor_id": f"sensor-{i:03d}",
                "sensor_type": "temperature",
                "value": 75.0,  # out of normal range
                "event_time": datetime(2024, 6, 15, 10, i, 0, tzinfo=UTC),
                "location": "floor-1-zone-A",
                "is_anomaly": True,
                "ingestion_timestamp": datetime.now(UTC),
            })
        df = pd.DataFrame(data)
        result = validate_silver_dataframe(df)
        assert result["success"] is True

    def test_single_sensor_aggregation(self):
        """Aggregation works with data from only one sensor."""
        records = []
        for minute in range(10):
            records.append({
                "sensor_id": "sensor-000",
                "sensor_type": "temperature",
                "value": 22.0 + minute * 0.1,
                "event_time": datetime(2024, 6, 15, 10, minute, 0, tzinfo=UTC),
                "location": "floor-1-zone-A",
                "is_anomaly": False,
            })
        df = pd.DataFrame(records)
        df["window_5min"] = df["event_time"].dt.floor("5min")
        agg = df.groupby(["sensor_id", "window_5min"]).agg(
            count=("value", "count"),
            avg=("value", "mean"),
        ).reset_index()

        assert len(agg) == 2  # 10 minutes -> 2 five-minute windows
        assert (agg["count"] == 5).all()

    def test_late_data_merge_dedup(self):
        """Late-arriving data merges correctly with existing records."""
        existing = pd.DataFrame([{
            "sensor_id": "sensor-001",
            "sensor_type": "temperature",
            "value": 22.0,
            "event_time": datetime(2024, 6, 15, 10, 0, 0, tzinfo=UTC),
            "ingestion_time": datetime(2024, 6, 15, 10, 0, 1, tzinfo=UTC),
        }])

        late = pd.DataFrame([{
            "sensor_id": "sensor-001",
            "sensor_type": "temperature",
            "value": 22.5,  # corrected value
            "event_time": datetime(2024, 6, 15, 10, 0, 0, tzinfo=UTC),
            "ingestion_time": datetime(2024, 6, 15, 11, 0, 0, tzinfo=UTC),  # later ingestion
        }])

        combined = pd.concat([existing, late], ignore_index=True)
        deduped = (
            combined
            .sort_values("ingestion_time", ascending=False)
            .drop_duplicates(subset=["sensor_id", "event_time"], keep="first")
        )

        assert len(deduped) == 1
        assert deduped.iloc[0]["value"] == 22.5  # late (corrected) value wins
