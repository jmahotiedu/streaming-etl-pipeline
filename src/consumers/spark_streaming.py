"""PySpark Structured Streaming consumer for IoT sensor events.

Reads JSON events from Kafka, parses with a defined schema, and writes
raw Parquet to S3 Bronze layer partitioned by sensor_type with configurable
micro-batch triggers, watermarking for late data, dead-letter handling
for malformed JSON, and Spark metrics logging.
"""

import argparse
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SENSOR_SCHEMA = StructType([
    StructField("sensor_id", StringType(), nullable=False),
    StructField("sensor_type", StringType(), nullable=False),
    StructField("timestamp", StringType(), nullable=False),
    StructField("value", DoubleType(), nullable=False),
    StructField("unit", StringType(), nullable=False),
    StructField("location", StringType(), nullable=True),
])


def create_spark_session(app_name: str = "SensorStreamingConsumer") -> SparkSession:
    """Create and configure a SparkSession for streaming."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def create_streaming_query(
    spark: SparkSession,
    kafka_servers: str,
    topic: str,
    bronze_path: str,
    checkpoint_path: str,
    trigger_interval: str = "30 seconds",
    starting_offsets: str = "latest",
    dead_letter_path: str | None = None,
    watermark_duration: str = "10 minutes",
):
    """Create and start the streaming query.

    Args:
        spark: Active SparkSession.
        kafka_servers: Kafka bootstrap servers.
        topic: Kafka topic to subscribe to.
        bronze_path: S3/local path for Bronze Parquet output.
        checkpoint_path: S3/local path for streaming checkpoints.
        trigger_interval: Micro-batch trigger interval.
        starting_offsets: Kafka starting offsets (latest or earliest).
        dead_letter_path: Path to write malformed/unparseable JSON events.
            If None, defaults to {bronze_path}/../dead_letter/.
        watermark_duration: Duration for watermark on event_time for late data.

    Returns:
        Active StreamingQuery handle.
    """
    if dead_letter_path is None:
        dead_letter_path = bronze_path.rstrip("/").rsplit("/", 1)[0] + "/dead_letter/"

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON and separate valid from malformed
    with_json = (
        raw_stream
        .select(
            col("value").cast("string").alias("raw_value"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("data", from_json(col("raw_value"), SENSOR_SCHEMA))
    )

    # Malformed: data is null after parsing
    malformed = (
        with_json
        .filter(col("data").isNull())
        .select(
            col("raw_value"),
            col("kafka_timestamp"),
            current_timestamp().alias("error_time"),
            lit("json_parse_failure").alias("error_type"),
        )
    )

    # Write malformed events to dead-letter path
    (
        malformed.writeStream
        .format("json")
        .option("path", dead_letter_path)
        .option("checkpointLocation", checkpoint_path + "_dead_letter")
        .trigger(processingTime=trigger_interval)
        .outputMode("append")
        .queryName("dead_letter_sensor_events")
        .start()
    )

    logger.info("Dead-letter query started: malformed events -> %s", dead_letter_path)

    # Valid records with watermarking for late data
    parsed = (
        with_json
        .filter(col("data").isNotNull())
        .select(
            "data.sensor_id",
            "data.sensor_type",
            col("data.timestamp").alias("event_timestamp"),
            "data.value",
            "data.unit",
            "data.location",
            "kafka_timestamp",
        )
        .withColumn("event_time", col("event_timestamp").cast(TimestampType()))
        .withColumn("ingestion_time", current_timestamp())
        .drop("event_timestamp")
        .withWatermark("event_time", watermark_duration)
    )

    query = (
        parsed.writeStream
        .format("parquet")
        .option("path", bronze_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("sensor_type")
        .trigger(processingTime=trigger_interval)
        .outputMode("append")
        .queryName("bronze_sensor_events")
        .start()
    )

    logger.info(
        "Streaming query started: topic=%s -> %s (trigger=%s, watermark=%s)",
        topic,
        bronze_path,
        trigger_interval,
        watermark_duration,
    )

    return query


def log_streaming_metrics(query) -> None:
    """Log Spark streaming query progress metrics."""
    progress = query.lastProgress
    if progress:
        num_input_rows = progress.get("numInputRows", 0)
        input_rows_per_sec = progress.get("inputRowsPerSecond", 0)
        processed_rows_per_sec = progress.get("processedRowsPerSecond", 0)
        batch_id = progress.get("batchId", -1)
        batch_duration_ms = progress.get("batchDuration", 0)

        logger.info(
            "Batch %d: %d rows, %.1f rows/sec in, %.1f rows/sec processed, %d ms",
            batch_id,
            num_input_rows,
            input_rows_per_sec,
            processed_rows_per_sec,
            batch_duration_ms,
        )


def main() -> None:
    """Entry point for the streaming consumer."""
    # Configurable trigger interval via environment variable
    default_trigger = os.environ.get("SPARK_TRIGGER_INTERVAL", "30 seconds")

    parser = argparse.ArgumentParser(description="Spark Streaming Consumer for Sensor Events")
    parser.add_argument(
        "--kafka-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic",
        default="sensor-events",
        help="Kafka topic (default: sensor-events)",
    )
    parser.add_argument(
        "--bronze-path",
        default="s3a://pipeline-bronze/sensor-events/",
        help="Output path for Bronze Parquet data",
    )
    parser.add_argument(
        "--checkpoint-path",
        default="s3a://pipeline-bronze/checkpoints/sensor/",
        help="Checkpoint path for streaming state",
    )
    parser.add_argument(
        "--trigger-interval",
        default=default_trigger,
        help=f"Processing trigger interval (default: {default_trigger})",
    )
    parser.add_argument(
        "--dead-letter-path",
        default=None,
        help="Path for dead-letter queue (malformed events)",
    )
    parser.add_argument(
        "--watermark",
        default="10 minutes",
        help="Watermark duration for late data (default: 10 minutes)",
    )
    args = parser.parse_args()

    spark = create_spark_session()

    query = create_streaming_query(
        spark=spark,
        kafka_servers=args.kafka_servers,
        topic=args.topic,
        bronze_path=args.bronze_path,
        checkpoint_path=args.checkpoint_path,
        trigger_interval=args.trigger_interval,
        dead_letter_path=args.dead_letter_path,
        watermark_duration=args.watermark,
    )

    # Periodically log metrics while waiting
    while query.isActive:
        query.awaitTermination(timeout=60)
        log_streaming_metrics(query)


if __name__ == "__main__":
    main()
