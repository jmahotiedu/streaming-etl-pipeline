"""Tests for the sensor simulator / Kafka producer."""

import json
from datetime import datetime

from src.producers.sensor_simulator import (
    SENSOR_CONFIG,
    SENSOR_TYPES,
    create_sensors,
    generate_event,
)


class TestGenerateEvent:
    """Tests for generate_event function."""

    def test_event_has_required_fields(self):
        """Generated event contains all required schema fields."""
        event = generate_event("sensor-001", "temperature")
        required_fields = {"sensor_id", "sensor_type", "timestamp", "value", "unit", "location"}
        assert required_fields.issubset(event.keys())

    def test_event_sensor_id_matches(self):
        """sensor_id in event matches the input."""
        event = generate_event("sensor-042", "humidity")
        assert event["sensor_id"] == "sensor-042"

    def test_event_sensor_type_matches(self):
        """sensor_type in event matches the input."""
        event = generate_event("sensor-001", "pressure")
        assert event["sensor_type"] == "pressure"

    def test_event_unit_matches_sensor_type(self):
        """Unit in event is correct for the sensor type."""
        expected_units = {
            "temperature": "celsius",
            "humidity": "percent",
            "pressure": "hPa",
            "vibration": "g",
        }
        for sensor_type, expected_unit in expected_units.items():
            event = generate_event("sensor-001", sensor_type)
            assert event["unit"] == expected_unit, f"Wrong unit for {sensor_type}"

    def test_event_value_within_physical_bounds(self):
        """Generated values stay within physical bounds for each sensor type."""
        for sensor_type in SENSOR_TYPES:
            config = SENSOR_CONFIG[sensor_type]
            for _ in range(100):
                event = generate_event("sensor-001", sensor_type, anomaly_rate=0.0)
                assert config["min"] <= event["value"] <= config["max"], (
                    f"{sensor_type} value {event['value']} out of [{config['min']}, {config['max']}]"
                )

    def test_event_value_within_bounds_even_with_anomalies(self):
        """Values are clamped even when anomaly is injected."""
        for sensor_type in SENSOR_TYPES:
            config = SENSOR_CONFIG[sensor_type]
            for _ in range(200):
                event = generate_event("sensor-001", sensor_type, anomaly_rate=1.0)
                assert config["min"] <= event["value"] <= config["max"]

    def test_event_timestamp_is_iso_format(self):
        """Timestamp is valid ISO 8601 format."""
        event = generate_event("sensor-001", "temperature")
        parsed = datetime.fromisoformat(event["timestamp"])
        assert parsed is not None

    def test_event_location_format(self):
        """Location follows floor-N-zone-X pattern."""
        event = generate_event("sensor-001", "temperature")
        location = event["location"]
        assert location.startswith("floor-")
        assert "-zone-" in location

    def test_event_value_is_rounded(self):
        """Values are rounded to 2 decimal places."""
        for _ in range(50):
            event = generate_event("sensor-001", "temperature")
            value_str = str(event["value"])
            if "." in value_str:
                decimals = len(value_str.split(".")[1])
                assert decimals <= 2

    def test_event_serializable_to_json(self):
        """Event can be serialized to JSON without errors."""
        event = generate_event("sensor-001", "temperature")
        json_str = json.dumps(event)
        parsed = json.loads(json_str)
        assert parsed == event


class TestCreateSensors:
    """Tests for create_sensors function."""

    def test_creates_correct_count(self):
        """Creates the requested number of sensors."""
        sensors = create_sensors(10)
        assert len(sensors) == 10

    def test_sensor_ids_are_unique(self):
        """All sensor IDs are unique."""
        sensors = create_sensors(50)
        ids = [s[0] for s in sensors]
        assert len(set(ids)) == 50

    def test_sensor_ids_follow_pattern(self):
        """Sensor IDs follow sensor-NNN pattern."""
        sensors = create_sensors(5)
        for sid, _ in sensors:
            assert sid.startswith("sensor-")
            num_part = sid.split("-")[1]
            assert len(num_part) == 3
            assert num_part.isdigit()

    def test_sensor_types_are_distributed(self):
        """Sensor types are distributed across all types."""
        sensors = create_sensors(20)
        types = {s[1] for s in sensors}
        assert types == set(SENSOR_TYPES)

    def test_empty_sensors(self):
        """Creating zero sensors returns empty list."""
        sensors = create_sensors(0)
        assert sensors == []
