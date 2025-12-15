"""Simulate IoT sensor data and publish to Kafka.

Generates realistic sensor readings for temperature, humidity, pressure,
and vibration sensors with configurable rates, Gaussian noise, and
occasional anomaly injection. Supports batch mode, CSV replay, and
Prometheus metrics.
"""

import argparse
import csv
import json
import logging
import random
import time
from datetime import UTC, datetime

from prometheus_client import Counter, start_http_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SENSOR_TYPES = ["temperature", "humidity", "pressure", "vibration"]

SENSOR_CONFIG = {
    "temperature": {"base": 22.0, "noise": 5.0, "unit": "celsius", "min": -40.0, "max": 85.0},
    "humidity": {"base": 55.0, "noise": 15.0, "unit": "percent", "min": 0.0, "max": 100.0},
    "pressure": {"base": 1013.0, "noise": 20.0, "unit": "hPa", "min": 300.0, "max": 1100.0},
    "vibration": {"base": 0.5, "noise": 0.3, "unit": "g", "min": 0.0, "max": 10.0},
}

ANOMALY_MULTIPLIERS = {
    "temperature": 4.0,
    "humidity": 3.0,
    "pressure": 5.0,
    "vibration": 10.0,
}

events_produced = Counter(
    "events_produced_total",
    "Total number of sensor events produced",
    ["sensor_type"],
)

anomalies_injected = Counter(
    "anomalies_injected_total",
    "Total number of anomalous events injected",
    ["sensor_type"],
)


def generate_event(
    sensor_id: str,
    sensor_type: str,
    anomaly_rate: float = 0.01,
) -> dict:
    """Generate a single sensor event with realistic values.

    Args:
        sensor_id: Unique sensor identifier.
        sensor_type: One of temperature, humidity, pressure, vibration.
        anomaly_rate: Probability of injecting an anomalous reading (0.0 - 1.0).

    Returns:
        Dictionary representing a sensor event.
    """
    config = SENSOR_CONFIG[sensor_type]
    is_anomaly = random.random() < anomaly_rate

    if is_anomaly:
        # Spike: multiply noise by 3x the anomaly multiplier for obvious anomalies
        value = config["base"] + random.gauss(0, config["noise"] * ANOMALY_MULTIPLIERS[sensor_type])
        anomalies_injected.labels(sensor_type=sensor_type).inc()
    else:
        value = config["base"] + random.gauss(0, config["noise"])

    value = round(max(config["min"], min(config["max"], value)), 2)

    return {
        "sensor_id": sensor_id,
        "sensor_type": sensor_type,
        "timestamp": datetime.now(UTC).isoformat(),
        "value": value,
        "unit": config["unit"],
        "location": f"floor-{random.randint(1, 5)}-zone-{random.choice('ABCD')}",
    }


def create_sensors(num_sensors: int) -> list[tuple[str, str]]:
    """Create a list of (sensor_id, sensor_type) pairs."""
    sensors = []
    for i in range(num_sensors):
        sensor_type = SENSOR_TYPES[i % len(SENSOR_TYPES)]
        sensors.append((f"sensor-{i:03d}", sensor_type))
    return sensors


def replay_csv(
    csv_path: str,
    bootstrap_servers: str,
    topic: str = "sensor-events",
    delay: float = 0.01,
) -> int:
    """Replay historical sensor data from a CSV file through Kafka.

    The CSV must have columns: sensor_id, sensor_type, timestamp, value, unit, location.

    Args:
        csv_path: Path to the CSV file.
        bootstrap_servers: Kafka bootstrap servers string.
        topic: Kafka topic to publish to.
        delay: Delay between events in seconds.

    Returns:
        Total number of events replayed.
    """
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )

    total_sent = 0
    logger.info("Replaying CSV: %s -> topic=%s", csv_path, topic)

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            event = {
                "sensor_id": row["sensor_id"],
                "sensor_type": row["sensor_type"],
                "timestamp": row.get("timestamp", datetime.now(UTC).isoformat()),
                "value": float(row["value"]),
                "unit": row["unit"],
                "location": row.get("location", "floor-1-zone-A"),
            }
            producer.send(topic, key=event["sensor_id"], value=event)
            events_produced.labels(sensor_type=event["sensor_type"]).inc()
            total_sent += 1

            if delay > 0:
                time.sleep(delay)

            if total_sent % 1000 == 0:
                logger.info("Replayed %d events", total_sent)

    producer.flush()
    producer.close()
    logger.info("CSV replay complete. Total events: %d", total_sent)
    return total_sent


def run_producer(
    bootstrap_servers: str,
    topic: str = "sensor-events",
    num_sensors: int = 50,
    events_per_second: int = 100,
    anomaly_rate: float = 0.01,
    max_events: int = 0,
) -> int:
    """Run the Kafka producer loop.

    Args:
        bootstrap_servers: Kafka bootstrap servers string.
        topic: Kafka topic to publish to.
        num_sensors: Number of simulated sensors.
        events_per_second: Target events per second.
        anomaly_rate: Probability of anomaly per event.
        max_events: Max events to produce (0 = unlimited / continuous mode).

    Returns:
        Total number of events produced.
    """
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        linger_ms=10,
        batch_size=32768,
    )

    sensors = create_sensors(num_sensors)
    total_sent = 0
    mode = "batch" if max_events > 0 else "continuous"

    logger.info(
        "Starting producer (%s mode): %d sensors, %d events/sec, anomaly_rate=%.3f, topic=%s",
        mode,
        num_sensors,
        events_per_second,
        anomaly_rate,
        topic,
    )

    try:
        while True:
            batch_start = time.monotonic()

            for _ in range(events_per_second):
                sensor_id, sensor_type = random.choice(sensors)
                event = generate_event(sensor_id, sensor_type, anomaly_rate)
                producer.send(
                    topic,
                    key=sensor_id,
                    value=event,
                )
                events_produced.labels(sensor_type=sensor_type).inc()
                total_sent += 1

                if max_events > 0 and total_sent >= max_events:
                    producer.flush()
                    logger.info("Batch complete: produced %d events.", max_events)
                    return total_sent

            producer.flush()

            elapsed = time.monotonic() - batch_start
            sleep_time = max(0, 1.0 - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

            if total_sent % (events_per_second * 10) == 0:
                logger.info("Produced %d events so far", total_sent)

    except KeyboardInterrupt:
        logger.info("Shutting down producer. Total events: %d", total_sent)
    finally:
        producer.close()

    return total_sent


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="IoT Sensor Data Simulator - Kafka Producer",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic",
        default="sensor-events",
        help="Kafka topic to publish to (default: sensor-events)",
    )
    parser.add_argument(
        "--num-sensors",
        type=int,
        default=50,
        help="Number of simulated sensors (default: 50)",
    )
    parser.add_argument(
        "--events-per-second",
        type=int,
        default=100,
        help="Target events per second (default: 100)",
    )
    parser.add_argument(
        "--anomaly-rate",
        type=float,
        default=0.02,
        help="Anomaly injection probability (default: 0.02 = 2%%)",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Max events to produce; 0=continuous, >0=batch mode (default: 0)",
    )
    parser.add_argument(
        "--replay-csv",
        default=None,
        help="Path to CSV file for historical data replay mode",
    )
    parser.add_argument(
        "--replay-delay",
        type=float,
        default=0.01,
        help="Delay between replayed events in seconds (default: 0.01)",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=8000,
        help="Prometheus metrics HTTP port (default: 8000)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Entry point for the sensor simulator."""
    args = parse_args(argv)

    start_http_server(args.metrics_port)
    logger.info("Prometheus metrics server started on port %d", args.metrics_port)

    if args.replay_csv:
        replay_csv(
            csv_path=args.replay_csv,
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            delay=args.replay_delay,
        )
    else:
        run_producer(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            num_sensors=args.num_sensors,
            events_per_second=args.events_per_second,
            anomaly_rate=args.anomaly_rate,
            max_events=args.max_events,
        )


if __name__ == "__main__":
    main()
