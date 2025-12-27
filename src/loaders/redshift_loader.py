"""Load Gold layer data from S3 into Redshift.

Uses Redshift COPY command to bulk-load Parquet from S3 Gold bucket.
Implements idempotent loads via delete-then-insert by window partition
to prevent duplicates on re-runs.
"""

import logging

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def get_connection(
    host: str,
    port: int,
    dbname: str,
    user: str,
    password: str,
) -> psycopg2.extensions.connection:
    """Create a Redshift connection via psycopg2."""
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )


def build_delete_sql(
    table: str,
    window_start: str,
    window_end: str,
) -> str:
    """Build a DELETE statement for idempotent loading.

    Removes existing records in the target window partition before
    inserting new data to prevent duplicates.

    Args:
        table: Target Redshift table name.
        window_start: Start of the time window (ISO timestamp).
        window_end: End of the time window (ISO timestamp).

    Returns:
        SQL DELETE statement string.
    """
    return (
        f"DELETE FROM {table} "
        f"WHERE window_start >= '{window_start}' "
        f"AND window_start < '{window_end}'"
    )


def build_copy_sql(
    table: str,
    s3_path: str,
    iam_role: str,
    region: str = "us-east-1",
) -> str:
    """Build a Redshift COPY command for Parquet data from S3.

    Args:
        table: Target Redshift table name.
        s3_path: S3 path to Parquet files.
        iam_role: IAM role ARN for Redshift to access S3.
        region: AWS region.

    Returns:
        SQL COPY statement string.
    """
    return (
        f"COPY {table} "
        f"FROM '{s3_path}' "
        f"IAM_ROLE '{iam_role}' "
        f"FORMAT AS PARQUET "
        f"REGION '{region}'"
    )


def load_sensor_readings(
    conn: psycopg2.extensions.connection,
    s3_gold_path: str,
    iam_role: str,
    window_start: str,
    window_end: str,
    region: str = "us-east-1",
) -> int:
    """Idempotently load sensor readings from Gold S3 to Redshift.

    Performs delete-then-insert within the given time window to ensure
    idempotent loads on re-runs.

    Args:
        conn: Active Redshift connection.
        s3_gold_path: S3 path to Gold sensor_5min Parquet.
        iam_role: IAM role ARN for S3 access.
        window_start: Window start timestamp (ISO format).
        window_end: Window end timestamp (ISO format).
        region: AWS region.

    Returns:
        Number of rows loaded (from stl_load_commits is not trivial,
        so returns -1 and logs success).
    """
    table = "fact_sensor_readings"

    delete_sql = build_delete_sql(table, window_start, window_end)
    copy_sql = build_copy_sql(table, s3_gold_path, iam_role, region)

    cursor = conn.cursor()
    try:
        logger.info("Deleting existing records: %s to %s", window_start, window_end)
        cursor.execute(delete_sql)
        deleted = cursor.rowcount
        logger.info("Deleted %d existing records", deleted)

        logger.info("Loading from %s", s3_gold_path)
        cursor.execute(copy_sql)

        conn.commit()
        logger.info("COPY completed successfully for window %s to %s", window_start, window_end)

        return deleted
    except Exception:
        conn.rollback()
        logger.exception("Load failed, rolled back transaction")
        raise
    finally:
        cursor.close()


def update_dim_sensors(
    conn: psycopg2.extensions.connection,
    s3_gold_path: str,
    iam_role: str,
    region: str = "us-east-1",
) -> None:
    """Update the dim_sensors dimension table from Gold data.

    Uses a staging table pattern: load into temp, then merge into dim.
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TEMP TABLE staging_sensors (
                sensor_id VARCHAR(20),
                sensor_type VARCHAR(20),
                location VARCHAR(50),
                window_start TIMESTAMP,
                window_end TIMESTAMP
            )
        """)

        copy_sql = build_copy_sql("staging_sensors", s3_gold_path, iam_role, region)
        cursor.execute(copy_sql)

        cursor.execute("""
            MERGE INTO dim_sensors
            USING (
                SELECT
                    sensor_id,
                    MAX(sensor_type) AS sensor_type,
                    MAX(location) AS location,
                    MIN(window_start) AS first_seen,
                    MAX(window_end) AS last_seen
                FROM staging_sensors
                GROUP BY sensor_id
            ) AS src
            ON dim_sensors.sensor_id = src.sensor_id
            WHEN MATCHED THEN UPDATE SET
                last_seen = GREATEST(dim_sensors.last_seen, src.last_seen),
                location = src.location
            WHEN NOT MATCHED THEN INSERT (sensor_id, sensor_type, location, first_seen, last_seen)
            VALUES (src.sensor_id, src.sensor_type, src.location, src.first_seen, src.last_seen)
        """)

        cursor.execute("DROP TABLE staging_sensors")
        conn.commit()
        logger.info("dim_sensors updated successfully")
    except Exception:
        conn.rollback()
        logger.exception("dim_sensors update failed")
        raise
    finally:
        cursor.close()


def initialize_schema(conn: psycopg2.extensions.connection, schema_path: str) -> None:
    """Execute the Redshift DDL schema file.

    Args:
        conn: Active Redshift connection.
        schema_path: Path to the SQL schema file.
    """
    with open(schema_path) as f:
        sql = f.read()

    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
        logger.info("Schema initialized from %s", schema_path)
    except Exception:
        conn.rollback()
        logger.exception("Schema initialization failed")
        raise
    finally:
        cursor.close()


def main() -> None:
    """CLI entry point for Redshift loader."""
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Load Gold data to Redshift")
    parser.add_argument("--host", default=os.getenv("REDSHIFT_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("REDSHIFT_PORT", "5439")))
    parser.add_argument("--dbname", default=os.getenv("REDSHIFT_DB", "analytics"))
    parser.add_argument("--user", default=os.getenv("REDSHIFT_USER", "admin"))
    parser.add_argument("--password", default=os.getenv("REDSHIFT_PASSWORD", ""))
    parser.add_argument("--s3-path", required=True, help="S3 Gold path to load")
    parser.add_argument("--iam-role", required=True, help="IAM role ARN for Redshift")
    parser.add_argument("--window-start", required=True, help="Window start (ISO timestamp)")
    parser.add_argument("--window-end", required=True, help="Window end (ISO timestamp)")
    parser.add_argument("--init-schema", default=None, help="Path to schema SQL file")
    args = parser.parse_args()

    conn = get_connection(args.host, args.port, args.dbname, args.user, args.password)

    try:
        if args.init_schema:
            initialize_schema(conn, args.init_schema)

        load_sensor_readings(
            conn=conn,
            s3_gold_path=args.s3_path,
            iam_role=args.iam_role,
            window_start=args.window_start,
            window_end=args.window_end,
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
