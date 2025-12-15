"""Shared test fixtures for the streaming ETL pipeline."""

from datetime import UTC, datetime

import pytest


@pytest.fixture
def sample_sensor_events():
    """Return a list of sample sensor event dictionaries."""
    return [
        {
            "sensor_id": "sensor-001",
            "sensor_type": "temperature",
            "timestamp": "2024-06-15T10:00:00+00:00",
            "value": 22.5,
            "unit": "celsius",
            "location": "floor-1-zone-A",
        },
        {
            "sensor_id": "sensor-002",
            "sensor_type": "humidity",
            "timestamp": "2024-06-15T10:00:01+00:00",
            "value": 55.3,
            "unit": "percent",
            "location": "floor-1-zone-B",
        },
        {
            "sensor_id": "sensor-003",
            "sensor_type": "pressure",
            "timestamp": "2024-06-15T10:00:02+00:00",
            "value": 1013.25,
            "unit": "hPa",
            "location": "floor-2-zone-A",
        },
        {
            "sensor_id": "sensor-004",
            "sensor_type": "vibration",
            "timestamp": "2024-06-15T10:00:03+00:00",
            "value": 0.45,
            "unit": "g",
            "location": "floor-2-zone-C",
        },
        {
            "sensor_id": "sensor-001",
            "sensor_type": "temperature",
            "timestamp": "2024-06-15T10:00:05+00:00",
            "value": 23.1,
            "unit": "celsius",
            "location": "floor-1-zone-A",
        },
    ]


@pytest.fixture
def sample_anomalous_events():
    """Return sensor events with anomalous values."""
    return [
        {
            "sensor_id": "sensor-010",
            "sensor_type": "temperature",
            "timestamp": "2024-06-15T10:05:00+00:00",
            "value": 75.0,
            "unit": "celsius",
            "location": "floor-3-zone-A",
        },
        {
            "sensor_id": "sensor-011",
            "sensor_type": "humidity",
            "timestamp": "2024-06-15T10:05:01+00:00",
            "value": 99.5,
            "unit": "percent",
            "location": "floor-3-zone-B",
        },
        {
            "sensor_id": "sensor-012",
            "sensor_type": "vibration",
            "timestamp": "2024-06-15T10:05:02+00:00",
            "value": 5.5,
            "unit": "g",
            "location": "floor-3-zone-C",
        },
    ]


@pytest.fixture
def sample_bronze_df():
    """Return a pandas DataFrame mimicking Bronze layer data."""
    import pandas as pd

    data = [
        {"sensor_id": "sensor-001", "sensor_type": "temperature", "timestamp": "2024-06-15T10:00:00+00:00", "value": 22.5, "unit": "celsius", "location": "floor-1-zone-A", "event_time": datetime(2024, 6, 15, 10, 0, 0, tzinfo=UTC), "ingestion_time": datetime(2024, 6, 15, 10, 0, 1, tzinfo=UTC)},
        {"sensor_id": "sensor-002", "sensor_type": "humidity", "timestamp": "2024-06-15T10:00:01+00:00", "value": 55.3, "unit": "percent", "location": "floor-1-zone-B", "event_time": datetime(2024, 6, 15, 10, 0, 1, tzinfo=UTC), "ingestion_time": datetime(2024, 6, 15, 10, 0, 2, tzinfo=UTC)},
        {"sensor_id": "sensor-003", "sensor_type": "pressure", "timestamp": "2024-06-15T10:00:02+00:00", "value": 1013.25, "unit": "hPa", "location": "floor-2-zone-A", "event_time": datetime(2024, 6, 15, 10, 0, 2, tzinfo=UTC), "ingestion_time": datetime(2024, 6, 15, 10, 0, 3, tzinfo=UTC)},
        {"sensor_id": "sensor-004", "sensor_type": "vibration", "timestamp": "2024-06-15T10:00:03+00:00", "value": 0.45, "unit": "g", "location": "floor-2-zone-C", "event_time": datetime(2024, 6, 15, 10, 0, 3, tzinfo=UTC), "ingestion_time": datetime(2024, 6, 15, 10, 0, 4, tzinfo=UTC)},
        # Duplicate of sensor-001 at the same event_time
        {"sensor_id": "sensor-001", "sensor_type": "temperature", "timestamp": "2024-06-15T10:00:00+00:00", "value": 22.7, "unit": "celsius", "location": "floor-1-zone-A", "event_time": datetime(2024, 6, 15, 10, 0, 0, tzinfo=UTC), "ingestion_time": datetime(2024, 6, 15, 10, 0, 5, tzinfo=UTC)},
    ]
    return pd.DataFrame(data)


@pytest.fixture
def sample_silver_df():
    """Return a pandas DataFrame mimicking Silver layer data (deduplicated, with anomaly flags)."""
    import pandas as pd

    data = [
        {"sensor_id": "sensor-001", "sensor_type": "temperature", "value": 22.5, "event_time": datetime(2024, 6, 15, 10, 0, 0, tzinfo=UTC), "location": "floor-1-zone-A", "is_anomaly": False, "ingestion_timestamp": datetime(2024, 6, 15, 10, 0, 1, tzinfo=UTC)},
        {"sensor_id": "sensor-002", "sensor_type": "humidity", "value": 55.3, "event_time": datetime(2024, 6, 15, 10, 0, 1, tzinfo=UTC), "location": "floor-1-zone-B", "is_anomaly": False, "ingestion_timestamp": datetime(2024, 6, 15, 10, 0, 2, tzinfo=UTC)},
        {"sensor_id": "sensor-003", "sensor_type": "pressure", "value": 1013.25, "event_time": datetime(2024, 6, 15, 10, 0, 2, tzinfo=UTC), "location": "floor-2-zone-A", "is_anomaly": False, "ingestion_timestamp": datetime(2024, 6, 15, 10, 0, 3, tzinfo=UTC)},
        {"sensor_id": "sensor-004", "sensor_type": "vibration", "value": 0.45, "event_time": datetime(2024, 6, 15, 10, 0, 3, tzinfo=UTC), "location": "floor-2-zone-C", "is_anomaly": False, "ingestion_timestamp": datetime(2024, 6, 15, 10, 0, 4, tzinfo=UTC)},
        {"sensor_id": "sensor-010", "sensor_type": "temperature", "value": 75.0, "event_time": datetime(2024, 6, 15, 10, 5, 0, tzinfo=UTC), "location": "floor-3-zone-A", "is_anomaly": True, "ingestion_timestamp": datetime(2024, 6, 15, 10, 5, 1, tzinfo=UTC)},
    ]
    return pd.DataFrame(data)
