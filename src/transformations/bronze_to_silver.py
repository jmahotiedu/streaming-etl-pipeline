"""Bronze to Silver transformation for sensor data.

Reads raw Parquet from Bronze layer, deduplicates on (sensor_id, timestamp),
validates value ranges per sensor type, adds anomaly flags (range-based and
z-score-based), data lineage columns, and handles late-arriving data by
merging with existing Silver records.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
)
from pyspark.sql.functions import (
    avg,
    col,
    current_timestamp,
    lit,
    row_number,
    stddev,
    when,
)
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

PIPELINE_VERSION = "1.2.0"

# Normal operating ranges per sensor type
VALUE_RANGES = {
    "temperature": {"min": -20.0, "max": 60.0},
    "humidity": {"min": 5.0, "max": 95.0},
    "pressure": {"min": 950.0, "max": 1070.0},
    "vibration": {"min": 0.0, "max": 2.0},
}


def deduplicate(df: DataFrame) -> DataFrame:
    """Remove duplicate records based on (sensor_id, event_time).

    Keeps the most recently ingested record when duplicates exist.
    """
    window_spec = Window.partitionBy("sensor_id", "event_time").orderBy(
        col("ingestion_time").desc()
    )
    return (
        df.withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )


def flag_anomalies(df: DataFrame) -> DataFrame:
    """Add is_anomaly column based on sensor-type-specific value ranges.

    Values outside the normal operating range for a given sensor type
    are flagged as anomalies.
    """
    conditions = None
    for sensor_type, bounds in VALUE_RANGES.items():
        type_condition = (
            (col("sensor_type") == lit(sensor_type))
            & ((col("value") < bounds["min"]) | (col("value") > bounds["max"]))
        )
        conditions = type_condition if conditions is None else conditions | type_condition

    return df.withColumn("is_anomaly", when(conditions, lit(True)).otherwise(lit(False)))


def flag_zscore_anomalies(df: DataFrame, window_size: int = 100, threshold: float = 3.0) -> DataFrame:
    """Add z-score based anomaly detection using a rolling window.

    Computes a rolling z-score per sensor and flags readings where
    |z-score| > threshold as anomalies. Merges with the existing
    is_anomaly column (logical OR).

    Args:
        df: DataFrame with sensor readings.
        window_size: Number of preceding rows in the rolling window.
        threshold: Z-score threshold for anomaly flagging.

    Returns:
        DataFrame with updated is_anomaly column.
    """
    window_spec = (
        Window.partitionBy("sensor_id")
        .orderBy("event_time")
        .rowsBetween(-window_size, 0)
    )

    df_with_stats = (
        df
        .withColumn("_rolling_avg", avg("value").over(window_spec))
        .withColumn("_rolling_std", stddev("value").over(window_spec))
    )

    df_with_zscore = df_with_stats.withColumn(
        "_zscore",
        when(
            (col("_rolling_std").isNotNull()) & (col("_rolling_std") > 0),
            spark_abs((col("value") - col("_rolling_avg")) / col("_rolling_std")),
        ).otherwise(lit(0.0)),
    )

    df_flagged = df_with_zscore.withColumn(
        "is_anomaly",
        when(
            col("is_anomaly") | (col("_zscore") > threshold),
            lit(True),
        ).otherwise(lit(False)),
    )

    return df_flagged.drop("_rolling_avg", "_rolling_std", "_zscore")


def validate_not_null(df: DataFrame) -> DataFrame:
    """Drop records with null values in critical columns."""
    critical_columns = ["sensor_id", "sensor_type", "value", "event_time"]
    result = df
    for column in critical_columns:
        result = result.filter(col(column).isNotNull())
    return result


def add_lineage_columns(df: DataFrame, source_file: str = "kafka-stream") -> DataFrame:
    """Add data lineage columns for traceability.

    Args:
        df: Input DataFrame.
        source_file: Source identifier (e.g., file path or stream name).

    Returns:
        DataFrame with source_file, processing_timestamp, pipeline_version columns.
    """
    return (
        df
        .withColumn("source_file", lit(source_file))
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("pipeline_version", lit(PIPELINE_VERSION))
    )


def merge_late_data(existing_df: DataFrame, new_df: DataFrame) -> DataFrame:
    """Merge late-arriving data with existing Silver records.

    Late data is combined with existing records and re-deduplicated,
    keeping the most recent ingestion for each (sensor_id, event_time).

    Args:
        existing_df: Current Silver DataFrame.
        new_df: New/late-arriving records.

    Returns:
        Merged and deduplicated DataFrame.
    """
    combined = existing_df.unionByName(new_df, allowMissingColumns=True)
    return deduplicate(combined)


def transform_bronze_to_silver(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
    processing_date: str | None = None,
    source_file: str = "kafka-stream",
    existing_silver_path: str | None = None,
) -> int:
    """Run the full Bronze-to-Silver transformation.

    Args:
        spark: Active SparkSession.
        bronze_path: Path to Bronze Parquet data.
        silver_path: Path to write Silver Parquet data.
        processing_date: Optional date filter (YYYY-MM-DD) to process
            only a specific partition.
        source_file: Source identifier for lineage tracking.
        existing_silver_path: Path to existing Silver data for late-data merge.

    Returns:
        Number of records written to Silver.
    """
    logger.info("Reading Bronze data from %s", bronze_path)
    df = spark.read.parquet(bronze_path)

    if processing_date:
        df = df.filter(col("event_time").cast("date") == lit(processing_date))

    initial_count = df.count()
    logger.info("Bronze records read: %d", initial_count)

    df = validate_not_null(df)
    null_filtered_count = df.count()
    logger.info("After null filtering: %d (removed %d)", null_filtered_count, initial_count - null_filtered_count)

    df = deduplicate(df)
    dedup_count = df.count()
    logger.info("After deduplication: %d (removed %d)", dedup_count, null_filtered_count - dedup_count)

    df = flag_anomalies(df)
    df = flag_zscore_anomalies(df)

    df = df.withColumn("ingestion_timestamp", current_timestamp())
    df = add_lineage_columns(df, source_file=source_file)

    # Merge with existing Silver data for late-arriving records
    if existing_silver_path:
        try:
            existing = spark.read.parquet(existing_silver_path)
            df = merge_late_data(existing, df)
            logger.info("Merged with existing Silver data from %s", existing_silver_path)
        except Exception:
            logger.warning("No existing Silver data found at %s, skipping merge", existing_silver_path)

    logger.info("Writing Silver data to %s", silver_path)
    df.write.mode("overwrite").partitionBy("sensor_type").parquet(silver_path)

    final_count = df.count()
    logger.info("Silver records written: %d", final_count)
    return final_count


def main() -> None:
    """CLI entry point for Bronze-to-Silver transformation."""
    import argparse

    parser = argparse.ArgumentParser(description="Bronze to Silver transformation")
    parser.add_argument("--bronze-path", required=True, help="Bronze layer input path")
    parser.add_argument("--silver-path", required=True, help="Silver layer output path")
    parser.add_argument("--date", default=None, help="Processing date (YYYY-MM-DD)")
    parser.add_argument("--source-file", default="kafka-stream", help="Source identifier for lineage")
    parser.add_argument("--existing-silver", default=None, help="Existing Silver path for late-data merge")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("BronzeToSilver")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    transform_bronze_to_silver(
        spark=spark,
        bronze_path=args.bronze_path,
        silver_path=args.silver_path,
        processing_date=args.date,
        source_file=args.source_file,
        existing_silver_path=args.existing_silver,
    )

    spark.stop()


if __name__ == "__main__":
    main()
