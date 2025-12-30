"""Backfill DAG for processing historical sensor data.

Reads historical CSV/Parquet from S3, processes through the same
Bronze-to-Silver-to-Gold pipeline, and loads results to Redshift.
Includes data validation between stages, backfill date range parameters,
and cleanup of temporary files.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

EMR_CLUSTER_ID = "{{ var.value.emr_cluster_id }}"

S3_RAW = "s3://pipeline-bronze-{{ var.value.environment }}/historical/"
S3_BRONZE = "s3://pipeline-bronze-{{ var.value.environment }}/sensor-events/"
S3_SILVER = "s3://pipeline-silver-{{ var.value.environment }}/sensor-events/"
S3_GOLD = "s3://pipeline-gold-{{ var.value.environment }}/"
S3_TEMP = "s3://pipeline-bronze-{{ var.value.environment }}/tmp/"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
    "email": ["data-team@example.com"],
    "depends_on_past": True,
}


def _ingest_historical_data(**context):
    """Read historical CSV from S3 and convert to Bronze Parquet."""
    processing_date = context["ds"]
    return [
        {
            "Name": f"ingest_historical_{processing_date}",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--master", "yarn",
                    "--conf", "spark.sql.sources.partitionOverwriteMode=dynamic",
                    "s3://pipeline-bronze-dev/scripts/ingest_historical.py",
                    "--input-path", f"{S3_RAW}{processing_date}/",
                    "--output-path", S3_BRONZE,
                    "--date", processing_date,
                ],
            },
        }
    ]


def _validate_bronze(**context):
    """Validate Bronze data after ingestion."""
    import logging

    import boto3

    logger = logging.getLogger(__name__)
    processing_date = context["ds"]
    env = context["var"]["value"].get("environment", "dev")
    bucket = f"pipeline-bronze-{env}"
    prefix = "sensor-events/"

    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5)

    if "Contents" not in response or not response["Contents"]:
        raise ValueError(f"No Bronze data found for {processing_date}")

    total_size = sum(obj["Size"] for obj in response["Contents"])
    logger.info("Bronze validation passed: %d objects, %d bytes", len(response["Contents"]), total_size)

    if total_size == 0:
        raise ValueError("Bronze data is empty (0 bytes)")

    return "bronze_validation_passed"


def _transform_bronze_to_silver(**context):
    """Submit Bronze-to-Silver batch transformation."""
    processing_date = context["ds"]
    return [
        {
            "Name": f"backfill_bronze_to_silver_{processing_date}",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--master", "yarn",
                    "s3://pipeline-bronze-dev/scripts/bronze_to_silver.py",
                    "--bronze-path", S3_BRONZE,
                    "--silver-path", S3_SILVER,
                    "--date", processing_date,
                ],
            },
        }
    ]


def _validate_silver(**context):
    """Validate Silver data after transformation."""
    from src.quality.expectations import run_validation

    silver_path = f"{S3_SILVER}sensor_type=temperature/"
    run_validation("silver", silver_path)
    return "silver_validation_passed"


def _transform_silver_to_gold(**context):
    """Submit Silver-to-Gold batch aggregation."""
    processing_date = context["ds"]
    return [
        {
            "Name": f"backfill_silver_to_gold_{processing_date}",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--master", "yarn",
                    "s3://pipeline-bronze-dev/scripts/silver_to_gold.py",
                    "--silver-path", S3_SILVER,
                    "--gold-path", S3_GOLD,
                    "--date", processing_date,
                ],
            },
        }
    ]


def _validate_gold(**context):
    """Validate Gold data after aggregation."""
    import logging

    import boto3

    logger = logging.getLogger(__name__)
    env = context["var"]["value"].get("environment", "dev")
    bucket = f"pipeline-gold-{env}"

    s3 = boto3.client("s3")
    for subdir in ["sensor_5min", "location_hourly", "daily_summary"]:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=f"{subdir}/", MaxKeys=5)
        if "Contents" not in response or not response["Contents"]:
            raise ValueError(f"No Gold data found in {subdir}/")
        logger.info("Gold %s: %d objects found", subdir, len(response["Contents"]))

    return "gold_validation_passed"


def _run_quality_checks(**context):
    """Run quality checks on backfilled data."""
    from src.quality.expectations import run_validation

    context["ds"]
    silver_path = f"{S3_SILVER}sensor_type=temperature/"

    run_validation("silver", silver_path)
    return "backfill_quality_passed"


def _load_backfill_to_redshift(**context):
    """Load backfilled Gold data to Redshift."""
    from src.loaders.redshift_loader import get_connection, load_sensor_readings

    processing_date = context["ds"]
    window_start = f"{processing_date}T00:00:00"
    window_end = f"{processing_date}T23:59:59"

    conn = get_connection(
        host=context["var"]["value"]["redshift_host"],
        port=int(context["var"]["value"].get("redshift_port", "5439")),
        dbname=context["var"]["value"].get("redshift_db", "analytics"),
        user=context["var"]["value"]["redshift_user"],
        password=context["var"]["value"]["redshift_password"],
    )

    try:
        load_sensor_readings(
            conn=conn,
            s3_gold_path=f"{S3_GOLD}sensor_5min/",
            iam_role=context["var"]["value"]["redshift_iam_role"],
            window_start=window_start,
            window_end=window_end,
        )
    finally:
        conn.close()


def _cleanup_temp_files(**context):
    """Remove temporary files created during backfill processing."""
    import logging

    import boto3

    logger = logging.getLogger(__name__)
    env = context["var"]["value"].get("environment", "dev")
    bucket = f"pipeline-bronze-{env}"
    prefix = "tmp/"

    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if "Contents" not in response:
        logger.info("No temp files to clean up")
        return "no_cleanup_needed"

    objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
    if objects:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
        logger.info("Cleaned up %d temporary files from s3://%s/%s", len(objects), bucket, prefix)

    return f"cleaned_{len(objects)}_files"


with DAG(
    dag_id="batch_pipeline_dag",
    default_args=default_args,
    description="Backfill DAG for processing historical sensor data",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 12, 31),
    catchup=True,
    tags=["batch", "backfill", "etl"],
    max_active_runs=3,
    params={
        "backfill_start_date": Param(
            default="2024-01-01",
            type="string",
            description="Start date for backfill (YYYY-MM-DD)",
        ),
        "backfill_end_date": Param(
            default="2024-12-31",
            type="string",
            description="End date for backfill (YYYY-MM-DD)",
        ),
    },
) as dag:

    ingest = EmrAddStepsOperator(
        task_id="ingest_historical",
        job_flow_id=EMR_CLUSTER_ID,
        steps=_ingest_historical_data,
    )

    wait_ingest = EmrStepSensor(
        task_id="wait_ingest",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='ingest_historical', key='return_value')[0] }}",
        poke_interval=30,
        timeout=3600,
    )

    validate_bronze_task = PythonOperator(
        task_id="validate_bronze",
        python_callable=_validate_bronze,
    )

    bronze_to_silver = EmrAddStepsOperator(
        task_id="bronze_to_silver",
        job_flow_id=EMR_CLUSTER_ID,
        steps=_transform_bronze_to_silver,
    )

    wait_b2s = EmrStepSensor(
        task_id="wait_bronze_to_silver",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='bronze_to_silver', key='return_value')[0] }}",
        poke_interval=30,
        timeout=1800,
    )

    validate_silver_task = PythonOperator(
        task_id="validate_silver",
        python_callable=_validate_silver,
    )

    silver_to_gold = EmrAddStepsOperator(
        task_id="silver_to_gold",
        job_flow_id=EMR_CLUSTER_ID,
        steps=_transform_silver_to_gold,
    )

    wait_s2g = EmrStepSensor(
        task_id="wait_silver_to_gold",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='silver_to_gold', key='return_value')[0] }}",
        poke_interval=30,
        timeout=1800,
    )

    validate_gold_task = PythonOperator(
        task_id="validate_gold",
        python_callable=_validate_gold,
    )

    quality = PythonOperator(
        task_id="quality_checks",
        python_callable=_run_quality_checks,
    )

    load = PythonOperator(
        task_id="load_to_redshift",
        python_callable=_load_backfill_to_redshift,
    )

    cleanup = PythonOperator(
        task_id="cleanup_temp_files",
        python_callable=_cleanup_temp_files,
        trigger_rule="all_done",
    )

    (
        ingest
        >> wait_ingest
        >> validate_bronze_task
        >> bronze_to_silver
        >> wait_b2s
        >> validate_silver_task
        >> silver_to_gold
        >> wait_s2g
        >> validate_gold_task
        >> quality
        >> load
        >> cleanup
    )
