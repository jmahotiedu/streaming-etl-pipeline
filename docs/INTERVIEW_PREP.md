# Interview Preparation Q&A

25+ questions and answers covering the design, implementation, and trade-offs of the IoT Streaming ETL Pipeline.

---

## Architecture & Design Decisions

### 1. Why Kafka over other message queues (RabbitMQ, SQS)?

Kafka provides durable, ordered, replayable event logs which are essential for data engineering pipelines. Unlike RabbitMQ (message queue, not a log), Kafka retains messages for configurable periods allowing reprocessing. Unlike SQS, Kafka supports consumer groups with partition-level parallelism and exactly-once semantics with transactions. For IoT data at scale (millions of events/day), Kafka's append-only log and horizontal scaling through partitions are ideal.

### 2. Exactly-once vs at-least-once semantics -- what does this pipeline use?

The producer uses `acks=all` with retries, which provides at-least-once delivery to Kafka. The Spark Structured Streaming consumer uses checkpointing, which combined with Kafka offsets gives effectively-once processing. The Redshift loader uses delete-then-insert (idempotent loads) so re-runs do not create duplicates. End-to-end, this achieves effectively-once semantics.

### 3. How do you handle late data in streaming?

Three mechanisms: (1) Spark watermarking with a 10-minute window, allowing late events within that window to be included in aggregations. (2) Bronze-to-Silver transformation merges late-arriving data with existing Silver records using union + re-deduplication. (3) Airflow daily batch processing catches anything the streaming watermark missed.

### 4. What is the Kafka partitioning strategy?

Events are keyed by `sensor_id`, so all readings from the same sensor go to the same partition. This ensures per-sensor ordering (important for z-score anomaly detection) and enables partition-level parallelism in consumers. The topic has 3 partitions by default, which can be increased for higher throughput.

### 5. Explain Spark checkpointing and fault tolerance.

Spark Structured Streaming uses write-ahead logs (WAL) stored at the checkpoint location. On failure, the query restarts from the last committed offset/batch, reprocessing any incomplete micro-batch. This prevents data loss and, combined with idempotent Parquet writes, prevents duplicates. Checkpoints store: current offsets, state of aggregations, and metadata about processed batches.

### 6. What are the trade-offs between batch and streaming?

Streaming provides low-latency insights (30-second micro-batches) but is more complex to operate and debug. Batch provides simpler, cheaper processing with higher latency. This pipeline uses both: streaming for real-time Bronze ingestion and monitoring, batch (Airflow-orchestrated) for Silver/Gold transformations and Redshift loading. This hybrid approach balances freshness with reliability.

### 7. Why the Medallion architecture (Bronze/Silver/Gold)?

Medallion provides clear data quality tiers: Bronze (raw, append-only), Silver (deduplicated, validated, anomaly-flagged), Gold (aggregated, business-ready). Benefits: (1) Raw data is never lost -- always reprocessable. (2) Each layer has clear ownership and SLAs. (3) Consumers can choose the right quality/latency trade-off. (4) Debugging data issues is straightforward -- trace from Gold back to Bronze.

---

## Data Quality & Monitoring

### 8. How do you ensure data quality in a streaming pipeline?

Multiple layers: (1) Producer-side: values clamped to physical bounds per sensor type. (2) Consumer-side: schema validation, malformed JSON routed to dead-letter queue. (3) Bronze-to-Silver: null filtering, deduplication, range-based and z-score anomaly flagging. (4) Great Expectations suites validate Bronze and Silver data. (5) Prometheus alerts for quality failures, consumer lag, and throughput drops.

### 9. Describe the monitoring and alerting strategy.

Prometheus scrapes metrics from the producer (events_produced_total, anomalies_injected_total), Spark, and Airflow. Grafana dashboards visualize throughput, consumer lag, processing latency (p50/p95/p99), and quality pass/fail rates. Alert rules fire on: consumer lag > 10K for 5 min, producer down for 2 min, quality failures, and p95 latency > 60s. Slack notifications via Airflow on DAG failures and SLA misses.

### 10. How does the dead-letter queue work?

The Spark streaming consumer attempts to parse each Kafka message as JSON against the sensor schema. If parsing fails (null result from from_json), the raw message is written to a separate dead-letter path as JSON with error metadata. This prevents malformed events from crashing the pipeline while preserving them for investigation.

### 11. What is z-score anomaly detection and why use it?

Z-score measures how many standard deviations a value is from the rolling mean. A z-score > 3 (99.7th percentile) indicates a statistical outlier. This complements range-based detection: a reading of 59 C for temperature is within physical bounds but is a z-score anomaly if the sensor normally reads 22 C. The rolling window (100 readings) adapts to each sensor's normal behavior.

---

## Scaling & Operations

### 12. How would you scale this pipeline 10x?

Producer: increase partition count, run multiple producer instances. Kafka: add brokers, increase partition count per topic. Spark: add worker nodes to EMR, increase executor memory/cores. Storage: S3 scales automatically. Redshift: increase RPUs for serverless. Key bottleneck is typically Spark processing -- scale EMR horizontally first.

### 13. What is the idempotent loading strategy for Redshift?

Delete-then-insert within a time window partition. Before COPY, we DELETE existing records where `window_start >= start AND window_start < end`. Then COPY new data. Both operations run in a single transaction, so on failure we rollback to the previous state. Re-running the same window produces identical results.

### 14. How do you handle schema evolution?

Several strategies: (1) Schema Registry for Kafka (Avro/Protobuf with compatibility checks). (2) Spark schema defined explicitly -- new fields added with nullable=True for backward compatibility. (3) Silver/Gold transformations use `allowMissingColumns=True` for union operations. (4) Parquet is columnar and supports schema evolution natively.

### 15. Infrastructure as code benefits -- why Terraform?

Terraform provides: (1) Reproducible environments -- dev/staging/prod from same code. (2) State management -- knows what exists and what needs changing. (3) Plan before apply -- preview destructive changes. (4) Module ecosystem -- AWS provider handles MSK, EMR, S3, Redshift. (5) Drift detection -- alerts when manual changes diverge from code.

---

## Cost & Optimization

### 16. What are the cost optimization approaches?

(1) EMR spot instances for worker nodes (70% savings). (2) S3 lifecycle policies to transition old Bronze data to Glacier. (3) Redshift Serverless auto-scales RPUs to zero when idle. (4) Kafka tiered storage moves cold data to S3. (5) Right-sizing: kafka.t3.small for dev, scale for production. (6) Parquet + Snappy compression reduces storage and I/O costs.

### 17. How do you estimate the monthly AWS cost?

Key components: MSK (3 brokers kafka.t3.small ~$200/mo), EMR (1 master + 2 core m5.large ~$300/mo), S3 (~$5/TB/mo), Redshift Serverless (~$50-200/mo depending on usage), MWAA (~$400/mo for smallest environment). Total: ~$1000-1500/mo for dev. Production at 10x scale: ~$3000-5000/mo with spot instances.

---

## Airflow & Orchestration

### 18. Airflow vs other orchestrators (Dagster, Prefect, Step Functions)?

Airflow chosen for: (1) Industry standard with largest community. (2) AWS MWAA provides managed service. (3) Rich operator ecosystem (EMR, S3, Redshift). (4) Mature UI for monitoring and manual triggering. Trade-offs: Airflow's scheduler can be heavy, DAGs are Python but not fully type-safe, and it requires a metadata database. Dagster offers better testing and asset lineage but smaller ecosystem.

### 19. How do SLA miss callbacks work?

Each task can have an `sla` timedelta. If a task has not completed within that duration from the DAG start time, Airflow calls the `sla_miss_callback`. Our callback sends a Slack notification with the DAG name and missed tasks. This is separate from task retries -- SLA misses fire even if the task eventually succeeds.

### 20. Why separate streaming and batch DAGs?

Separation of concerns: the streaming DAG manages the continuous pipeline (check streaming job, daily transformations, quality checks, loading). The batch DAG handles historical backfill with different retry policies, depends_on_past=True, and date range parameters. They share the same transformation code but have different orchestration requirements.

---

## Implementation Details

### 21. Why Parquet over CSV/JSON for the data lake?

Parquet provides: (1) Columnar storage -- queries that read specific columns skip others. (2) Built-in compression (Snappy) -- 5-10x smaller than CSV. (3) Schema enforcement -- prevents silent data corruption. (4) Predicate pushdown -- Spark/Athena skip irrelevant row groups. (5) Native support in Spark, pandas, Redshift COPY, and Athena.

### 22. How does data lineage work in Silver?

Three columns added during Bronze-to-Silver transformation: `source_file` (identifies the Kafka stream or CSV file), `processing_timestamp` (when the transformation ran), `pipeline_version` (semantic version of the transform code). This enables debugging: trace any Silver record back to its source and processing run.

### 23. What is the sensor health metric?

Sensor health = (actual readings received / expected readings) * 100% per 5-minute window. Expected is ~300 readings (1/sec for 5 min). Health < 90% suggests the sensor is intermittently failing. This is computed in Gold and displayed in the analytics dashboard. Alerts can trigger on sustained low health.

### 24. How does the analytics dashboard connect to data?

The Streamlit dashboard first attempts to read Parquet from the Gold layer (configurable via GOLD_DATA_PATH env var). If Redshift is configured (REDSHIFT_CONN_STRING), it queries directly. If neither is available, it generates realistic demo data. This allows the dashboard to work in development without a full pipeline running.

### 25. Explain the Prometheus metrics exposed by the producer.

Two counter metrics: `events_produced_total` (labeled by sensor_type) tracks throughput, and `anomalies_injected_total` (labeled by sensor_type) tracks anomaly injection rate. These enable Grafana dashboards showing events/min by type and anomaly rate, plus alerts when throughput drops to zero (producer down).

---

## Advanced Topics

### 26. How would you add real-time anomaly detection with ML?

Replace z-score with a trained model: (1) Train an isolation forest or autoencoder on historical Silver data. (2) Deploy as a Spark UDF or sidecar microservice. (3) Score each event in the Bronze-to-Silver transformation. (4) Use MLflow for model versioning and A/B testing. (5) Retrain periodically on new data via Airflow.

### 27. How would you implement exactly-once end-to-end?

(1) Kafka transactions (producer enable.idempotence=true, consumer read_committed). (2) Spark Structured Streaming with Kafka sink and checkpoints provides exactly-once between Kafka and Spark. (3) Idempotent Parquet writes (overwrite mode). (4) Redshift delete-then-insert. The hardest gap is Spark-to-S3 -- use S3 committer protocol (EMRFS) for atomic writes.

### 28. What security considerations apply?

(1) MSK: TLS encryption in transit, IAM authentication, VPC-only access. (2) S3: server-side encryption (SSE-S3 or SSE-KMS), bucket policies restricting access to pipeline IAM roles. (3) Redshift: VPC, SSL connections, IAM-based authentication. (4) Airflow: RBAC, encrypted connections, secrets backend (AWS Secrets Manager). (5) Network: all services in private subnets, NAT gateway for outbound.
