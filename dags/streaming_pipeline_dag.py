"""Daily DAG for managing the streaming ETL pipeline.

Orchestrates: ensure streaming job running, data freshness check,
Bronze-to-Silver transformation, Silver-to-Gold aggregation, data quality
checks, Redshift load, daily summary email, and Slack failure notifications.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

EMR_CLUSTER_ID = "{{ var.value.emr_cluster_id }}"
IAM_ROLE = "{{ var.value.redshift_iam_role }}"
REDSHIFT_CONN_ID = "redshift_default"
SLACK_WEBHOOK_URL = "{{ var.value.get('slack_webhook_url', '') }}"

S3_BRONZE = "s3://pipeline-bronze-{{ var.value.environment }}/sensor-events/"
S3_SILVER = "s3://pipeline-silver-{{ var.value.environment }}/sensor-events/"
S3_GOLD = "s3://pipeline-gold-{{ var.value.environment }}/"


def _sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Handle SLA miss by sending a Slack notification."""
    import logging

    logger = logging.getLogger(__name__)
    missed_tasks = [str(t) for t in task_list]
    logger.warning("SLA MISS: dag=%s tasks=%s", dag.dag_id, missed_tasks)

    slack_url = SLACK_WEBHOOK_URL
    if slack_url and not slack_url.startswith("{{"):
        try:
            import json
            from urllib.request import Request, urlopen

            payload = {
                "text": f":warning: SLA Miss on `{dag.dag_id}`: tasks {missed_tasks}",
            }
            req = Request(
                slack_url,
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"},
            )
            urlopen(req, timeout=10)  # noqa: S310
        except Exception:
            logger.exception("Failed to send SLA miss Slack notification")


default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-team@example.com"],
    "depends_on_past": False,
    "sla": timedelta(hours=2),
}


def _check_streaming_job(**context):
    """Check that the Spark streaming job is running on EMR."""
    import boto3

    emr = boto3.client("emr")
    cluster_id = context["var"]["value"]["emr_cluster_id"]

    steps = emr.list_steps(
        ClusterId=cluster_id,
        StepStates=["RUNNING"],
    )

    streaming_running = any(
        "spark_streaming" in step["Name"].lower()
        for step in steps.get("Steps", [])
    )

    if streaming_running:
        context["ti"].xcom_push(key="streaming_status", value="running")
        return "streaming_already_running"
    else:
        context["ti"].xcom_push(key="streaming_status", value="needs_restart")
        return "start_streaming_job"


def _check_data_freshness(**context):
    """Check that Bronze has data within the last 30 minutes.

    Uses boto3 to check the most recent object modification time in the
    Bronze S3 prefix.
    """
    import logging

    import boto3

    logger = logging.getLogger(__name__)
    s3 = boto3.client("s3")
    env = context["var"]["value"].get("environment", "dev")
    bucket = f"pipeline-bronze-{env}"
    prefix = "sensor-events/"

    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        MaxKeys=10,
    )

    if "Contents" not in response or not response["Contents"]:
        raise ValueError(f"No data found in s3://{bucket}/{prefix}")

    latest_modified = max(obj["LastModified"] for obj in response["Contents"])
    from datetime import UTC

    now = datetime.now(UTC)
    age_minutes = (now - latest_modified).total_seconds() / 60

    logger.info("Latest Bronze data is %.1f minutes old", age_minutes)

    if age_minutes > 30:
        raise ValueError(
            f"Bronze data is stale: {age_minutes:.1f} minutes old (threshold: 30 min)"
        )

    return f"data_fresh_{age_minutes:.0f}min"


def _run_bronze_to_silver(**context):
    """Submit Bronze-to-Silver Spark job to EMR."""
    processing_date = context["ds"]
    return [
        {
            "Name": f"bronze_to_silver_{processing_date}",
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


def _run_silver_to_gold(**context):
    """Submit Silver-to-Gold Spark job to EMR."""
    processing_date = context["ds"]
    return [
        {
            "Name": f"silver_to_gold_{processing_date}",
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


def _run_quality_checks(**context):
    """Run Great Expectations validation on Bronze and Silver data."""
    from src.quality.expectations import run_validation

    context["ds"]
    bronze_path = f"{S3_BRONZE}sensor_type=temperature/"
    silver_path = f"{S3_SILVER}sensor_type=temperature/"

    run_validation("bronze", bronze_path)
    run_validation("silver", silver_path)

    return "quality_checks_passed"


def _load_to_redshift(**context):
    """Load Gold data to Redshift using COPY command."""
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


def _generate_daily_summary(**context):
    """Generate a daily summary email with pipeline statistics."""
    import logging

    logger = logging.getLogger(__name__)
    processing_date = context["ds"]
    dag_run = context["dag_run"]
    task_instances = dag_run.get_task_instances()

    failed_tasks = [ti.task_id for ti in task_instances if ti.state == "failed"]
    succeeded_tasks = [ti.task_id for ti in task_instances if ti.state == "success"]

    summary = (
        f"Pipeline Daily Summary - {processing_date}\n"
        f"{'=' * 50}\n"
        f"Succeeded tasks: {len(succeeded_tasks)}\n"
        f"Failed tasks: {len(failed_tasks)}\n"
    )

    if failed_tasks:
        summary += f"Failed: {', '.join(failed_tasks)}\n"

    summary += f"\nDAG run: {dag_run.run_id}\n"

    logger.info("Daily summary:\n%s", summary)
    context["ti"].xcom_push(key="daily_summary", value=summary)
    return summary


def _send_notification(**context):
    """Send pipeline completion notification via Slack if configured."""
    import logging

    logger = logging.getLogger(__name__)
    dag_run = context["dag_run"]
    task_instances = dag_run.get_task_instances()
    failed = [ti.task_id for ti in task_instances if ti.state == "failed"]

    if failed:
        message = f":x: Pipeline FAILED for {context['ds']} - Failed tasks: {failed}"
        status = "FAILED"
    else:
        message = f":white_check_mark: Pipeline completed successfully for {context['ds']}"
        status = "SUCCESS"

    logger.info("Pipeline status: %s", status)
    context["ti"].xcom_push(key="notification", value=message)

    slack_url = SLACK_WEBHOOK_URL
    if slack_url and not slack_url.startswith("{{"):
        try:
            import json
            from urllib.request import Request, urlopen

            payload = {"text": message}
            req = Request(
                slack_url,
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"},
            )
            urlopen(req, timeout=10)  # noqa: S310
            logger.info("Slack notification sent")
        except Exception:
            logger.exception("Failed to send Slack notification")


with DAG(
    dag_id="streaming_pipeline_dag",
    default_args=default_args,
    description="Daily orchestration for the streaming ETL pipeline",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["streaming", "etl", "production"],
    max_active_runs=1,
    sla_miss_callback=_sla_miss_callback,
) as dag:

    check_streaming = PythonOperator(
        task_id="check_streaming_job",
        python_callable=_check_streaming_job,
    )

    check_freshness = PythonOperator(
        task_id="check_data_freshness",
        python_callable=_check_data_freshness,
    )

    bronze_to_silver_step = EmrAddStepsOperator(
        task_id="bronze_to_silver",
        job_flow_id=EMR_CLUSTER_ID,
        steps=_run_bronze_to_silver,
    )

    wait_bronze_to_silver = EmrStepSensor(
        task_id="wait_bronze_to_silver",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='bronze_to_silver', key='return_value')[0] }}",
        poke_interval=30,
        timeout=1800,
    )

    silver_to_gold_step = EmrAddStepsOperator(
        task_id="silver_to_gold",
        job_flow_id=EMR_CLUSTER_ID,
        steps=_run_silver_to_gold,
    )

    wait_silver_to_gold = EmrStepSensor(
        task_id="wait_silver_to_gold",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='silver_to_gold', key='return_value')[0] }}",
        poke_interval=30,
        timeout=1800,
    )

    quality_checks = PythonOperator(
        task_id="quality_checks",
        python_callable=_run_quality_checks,
    )

    load_redshift = PythonOperator(
        task_id="load_to_redshift",
        python_callable=_load_to_redshift,
    )

    daily_summary = PythonOperator(
        task_id="generate_daily_summary",
        python_callable=_generate_daily_summary,
        trigger_rule="all_done",
    )

    notify = PythonOperator(
        task_id="send_notification",
        python_callable=_send_notification,
        trigger_rule="all_done",
    )

    (
        check_streaming
        >> check_freshness
        >> bronze_to_silver_step
        >> wait_bronze_to_silver
        >> silver_to_gold_step
        >> wait_silver_to_gold
        >> quality_checks
        >> load_redshift
        >> daily_summary
        >> notify
    )
