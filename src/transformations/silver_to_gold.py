"""Silver to Gold transformation for sensor data.

Computes windowed aggregations from Silver layer:
- 5-minute windows per sensor (avg, min, max, count, stddev, percentiles)
- Hourly aggregations per location with sensor counts
- Daily summaries across all sensors with health metrics

Writes results to Gold layer as Parquet.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    date_format,
    lit,
    percentile_approx,
    stddev,
    to_date,
    when,
    window,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.functions import (
    min as spark_min,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Expected readings per sensor per 5-minute window (assuming ~1 event/sec)
EXPECTED_READINGS_PER_5MIN = 300


def compute_sensor_5min_aggregations(df: DataFrame) -> DataFrame:
    """Compute 5-minute windowed aggregations per sensor.

    Returns DataFrame with columns: sensor_id, sensor_type, location,
    window_start, window_end, avg_value, min_value, max_value,
    reading_count, stddev_value, p50_value, p95_value, p99_value,
    sensor_health_pct.
    """
    return (
        df.groupBy(
            col("sensor_id"),
            col("sensor_type"),
            col("location"),
            window(col("event_time"), "5 minutes"),
        )
        .agg(
            avg("value").alias("avg_value"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
            count("value").alias("reading_count"),
            stddev("value").alias("stddev_value"),
            percentile_approx("value", 0.50).alias("p50_value"),
            percentile_approx("value", 0.95).alias("p95_value"),
            percentile_approx("value", 0.99).alias("p99_value"),
        )
        .withColumn(
            "sensor_health_pct",
            when(
                lit(EXPECTED_READINGS_PER_5MIN) > 0,
                (col("reading_count") / lit(EXPECTED_READINGS_PER_5MIN)) * 100,
            ).otherwise(lit(100.0)),
        )
        .select(
            "sensor_id",
            "sensor_type",
            "location",
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "avg_value",
            "min_value",
            "max_value",
            "reading_count",
            "stddev_value",
            "p50_value",
            "p95_value",
            "p99_value",
            "sensor_health_pct",
        )
    )


def compute_location_hourly_aggregations(df: DataFrame) -> DataFrame:
    """Compute hourly aggregations per location with sensor counts.

    Returns DataFrame with columns: location, hour_start, sensor_type,
    avg_value, min_value, max_value, reading_count, stddev_value,
    p50_value, p95_value, p99_value, unique_sensor_count.
    """
    return (
        df.groupBy(
            col("location"),
            col("sensor_type"),
            window(col("event_time"), "1 hour"),
        )
        .agg(
            avg("value").alias("avg_value"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
            count("value").alias("reading_count"),
            stddev("value").alias("stddev_value"),
            percentile_approx("value", 0.50).alias("p50_value"),
            percentile_approx("value", 0.95).alias("p95_value"),
            percentile_approx("value", 0.99).alias("p99_value"),
            countDistinct("sensor_id").alias("unique_sensor_count"),
        )
        .select(
            "location",
            "sensor_type",
            col("window.start").alias("hour_start"),
            col("window.end").alias("hour_end"),
            "avg_value",
            "min_value",
            "max_value",
            "reading_count",
            "stddev_value",
            "p50_value",
            "p95_value",
            "p99_value",
            "unique_sensor_count",
        )
    )


def compute_daily_summaries(df: DataFrame) -> DataFrame:
    """Compute daily summary statistics across all sensors.

    Returns DataFrame with columns: date, sensor_type, avg_value,
    min_value, max_value, total_readings, stddev_value, anomaly_count,
    unique_sensor_count, date_str.
    """
    return (
        df.groupBy(
            to_date(col("event_time")).alias("date"),
            col("sensor_type"),
        )
        .agg(
            avg("value").alias("avg_value"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
            count("value").alias("total_readings"),
            stddev("value").alias("stddev_value"),
            spark_sum(
                when(col("is_anomaly") == True, lit(1)).otherwise(lit(0))  # noqa: E712
            ).alias("anomaly_count"),
            countDistinct("sensor_id").alias("unique_sensor_count"),
        )
        .withColumn("date_str", date_format("date", "yyyy-MM-dd"))
    )


def transform_silver_to_gold(
    spark: SparkSession,
    silver_path: str,
    gold_base_path: str,
    processing_date: str | None = None,
) -> dict[str, int]:
    """Run the full Silver-to-Gold transformation.

    Args:
        spark: Active SparkSession.
        silver_path: Path to Silver Parquet data.
        gold_base_path: Base path for Gold Parquet output.
        processing_date: Optional date filter (YYYY-MM-DD).

    Returns:
        Dictionary of output table names to record counts.
    """
    logger.info("Reading Silver data from %s", silver_path)
    df = spark.read.parquet(silver_path)

    if processing_date:
        df = df.filter(to_date(col("event_time")) == lit(processing_date))

    record_count = df.count()
    logger.info("Silver records: %d", record_count)

    # 5-minute sensor aggregations
    sensor_5min = compute_sensor_5min_aggregations(df)
    sensor_5min_path = f"{gold_base_path}/sensor_5min/"
    sensor_5min.write.mode("overwrite").partitionBy("sensor_type").parquet(sensor_5min_path)
    sensor_5min_count = sensor_5min.count()
    logger.info("Gold sensor_5min records: %d -> %s", sensor_5min_count, sensor_5min_path)

    # Hourly location aggregations
    location_hourly = compute_location_hourly_aggregations(df)
    location_hourly_path = f"{gold_base_path}/location_hourly/"
    location_hourly.write.mode("overwrite").partitionBy("sensor_type").parquet(location_hourly_path)
    location_hourly_count = location_hourly.count()
    logger.info("Gold location_hourly records: %d -> %s", location_hourly_count, location_hourly_path)

    # Daily summaries
    daily = compute_daily_summaries(df)
    daily_path = f"{gold_base_path}/daily_summary/"
    daily.write.mode("overwrite").partitionBy("sensor_type").parquet(daily_path)
    daily_count = daily.count()
    logger.info("Gold daily_summary records: %d -> %s", daily_count, daily_path)

    return {
        "sensor_5min": sensor_5min_count,
        "location_hourly": location_hourly_count,
        "daily_summary": daily_count,
    }


def main() -> None:
    """CLI entry point for Silver-to-Gold transformation."""
    import argparse

    parser = argparse.ArgumentParser(description="Silver to Gold transformation")
    parser.add_argument("--silver-path", required=True, help="Silver layer input path")
    parser.add_argument("--gold-path", required=True, help="Gold layer output base path")
    parser.add_argument("--date", default=None, help="Processing date (YYYY-MM-DD)")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("SilverToGold")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    results = transform_silver_to_gold(
        spark=spark,
        silver_path=args.silver_path,
        gold_base_path=args.gold_path,
        processing_date=args.date,
    )

    for table, cnt in results.items():
        logger.info("Output %s: %d records", table, cnt)

    spark.stop()


if __name__ == "__main__":
    main()
