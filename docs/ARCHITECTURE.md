# Architecture

## Component Descriptions

### Sensor Simulator (Producer)
**Technology:** Python, kafka-python, Prometheus client
**Purpose:** Generates realistic IoT sensor data (temperature, humidity, pressure, vibration) and publishes JSON events to Kafka. Supports continuous mode, batch mode (N events then exit), and CSV replay for historical data. Exposes Prometheus metrics for throughput and anomaly rate monitoring.

### Apache Kafka (AWS MSK)
**Technology:** Confluent Platform 7.5 (local), AWS MSK (production)
**Purpose:** Distributed event streaming platform serving as the central data bus. Provides durable, ordered, replayable event logs. Topic `sensor-events` with 3 partitions, keyed by sensor_id for per-sensor ordering.

### Spark Structured Streaming (Consumer)
**Technology:** PySpark 3.5 Structured Streaming, AWS EMR
**Purpose:** Reads JSON events from Kafka in micro-batches (configurable trigger interval), parses against a defined schema, applies watermarking for late data (10 minutes), and writes raw Parquet to S3 Bronze layer. Malformed events are routed to a dead-letter path instead of crashing the pipeline.

### Bronze-to-Silver Transformation
**Technology:** PySpark batch processing
**Purpose:** Cleans and enriches raw data: null filtering, deduplication (keeps latest ingestion per sensor_id + event_time), range-based anomaly detection, z-score rolling window anomaly detection, and data lineage column injection (source_file, processing_timestamp, pipeline_version). Supports merging late-arriving data with existing Silver records.

### Silver-to-Gold Transformation
**Technology:** PySpark batch processing
**Purpose:** Computes business-ready aggregations: 5-minute windowed stats per sensor (avg, min, max, stddev, p50, p95, p99, reading count, sensor health %), hourly location-level aggregations with sensor counts, and daily summaries with anomaly counts.

### Redshift Loader
**Technology:** psycopg2, Redshift COPY command
**Purpose:** Idempotently loads Gold Parquet data into Redshift using delete-then-insert pattern within time window partitions. Also maintains a dim_sensors dimension table using staging table + MERGE pattern.

### Great Expectations (Data Quality)
**Technology:** Custom validation functions (GE-compatible interface)
**Purpose:** Validates data at Bronze and Silver layers. Bronze checks: non-null critical fields, valid sensor types, physical value ranges, null rate < 1%. Silver checks: no duplicates, is_anomaly column present and boolean, row count coverage within 5% of Bronze.

### Apache Airflow (AWS MWAA)
**Technology:** Airflow 2.8, AWS MWAA
**Purpose:** Orchestrates the daily pipeline: streaming job health check, data freshness verification, Bronze-to-Silver transformation, Silver-to-Gold aggregation, quality checks, Redshift loading, daily summary generation, and Slack notifications. SLA miss callbacks alert on delayed processing.

### Prometheus + Grafana (Monitoring)
**Technology:** Prometheus 2.48, Grafana 10.2
**Purpose:** Prometheus scrapes metrics from the producer, Spark, and Airflow. Grafana provides real-time dashboards: throughput per sensor type, consumer lag with thresholds, processing latency percentiles, anomaly injection rate, S3 write status, and quality pass/fail rates. Alert rules fire on consumer lag, producer downtime, quality failures, and high latency.

### Streamlit Dashboard (Analytics)
**Technology:** Streamlit, pandas, numpy
**Purpose:** Interactive analytics dashboard with 5 pages: Real-Time Overview (latest readings, active sensors, health), Sensor Trends (per-type line charts), Location Heatmap (floor/zone grid), Anomaly Timeline (filterable scatter plot), and Pipeline Health (freshness, quality rate, sensor health distribution). Reads from Gold Parquet or falls back to demo data.

---

## Data Flow Diagram

```
Sensor Simulator                 Apache Kafka (MSK)
  (Python)          ------>      topic: sensor-events
  50 sensors                     3 partitions
  100 events/sec                 keyed by sensor_id
  2% anomaly rate                     |
       |                              |
       | Prometheus                   v
       | metrics              Spark Structured Streaming
       |                        (PySpark / EMR)
       v                      watermark: 10 min
  Prometheus ------>           trigger: 30 sec
  Grafana                           |
  Dashboards              +--------+--------+
  Alerts                  |                 |
                    Valid events      Malformed events
                          |                 |
                          v                 v
                    S3 Bronze         S3 Dead Letter
                    (raw Parquet)     (JSON errors)
                          |
                          v
                   Bronze-to-Silver
                   (Spark Batch)
                   - deduplicate
                   - validate nulls
                   - flag anomalies (range + z-score)
                   - add lineage columns
                          |
                          v
                    S3 Silver
                    (cleaned Parquet)
                          |
                          v
                   Silver-to-Gold
                   (Spark Batch)
                   - 5-min windowed aggs (p50/p95/p99)
                   - hourly location aggs
                   - daily summaries
                          |
                          v
                    S3 Gold              Streamlit
                    (aggregated)  -----> Dashboard
                          |              (analytics)
                          v
                    Redshift Loader
                    (delete + COPY)
                          |
                          v
                    AWS Redshift
                    (analytics warehouse)

  Orchestration: Apache Airflow (MWAA)
  - streaming_pipeline_dag (daily)
  - batch_pipeline_dag (backfill)
  - Quality checks (Great Expectations)
  - Slack notifications on failure
```

---

## Design Decisions and Trade-offs

| Decision | Rationale | Trade-off |
|----------|-----------|-----------|
| Kafka over SQS | Replayable log, consumer groups, ordering | Higher operational complexity |
| Spark Structured Streaming | Unified batch+streaming API, exactly-once | JVM overhead, cold start latency |
| Parquet over CSV/JSON | Columnar, compressed, schema-enforced | Not human-readable, write-once |
| Medallion architecture | Clear quality tiers, reprocessability | 3x storage cost (mitigated by lifecycle) |
| Idempotent Redshift loads | Safe re-runs, no duplicates | Delete step adds latency |
| Z-score + range anomaly detection | Catches statistical and physical outliers | Z-score needs sufficient history |
| Dead-letter queue | Malformed data does not crash pipeline | Requires monitoring and reprocessing |
| Airflow over Step Functions | Rich UI, ecosystem, managed via MWAA | Heavier infrastructure |

---

## Scaling Considerations

- **Kafka:** Add partitions for higher parallelism. Add brokers for more storage/throughput. Use tiered storage for cold data.
- **Spark:** Scale EMR horizontally (add core nodes). Use spot instances for workers (70% savings). Increase executor memory for larger windows.
- **S3:** Effectively unlimited. Use lifecycle policies to tier old Bronze data to Glacier.
- **Redshift:** Serverless auto-scales RPUs. For sustained load, consider provisioned clusters with reserved instances.
- **Airflow:** MWAA scales workers automatically. For complex DAGs, increase scheduler capacity.

---

## Security Considerations

### Encryption
- **In transit:** TLS for Kafka (MSK), HTTPS for S3 API, SSL for Redshift connections
- **At rest:** SSE-S3 or SSE-KMS for S3 buckets, AES-256 for Redshift, EBS encryption for EMR

### Authentication & Authorization
- **IAM roles:** Separate roles for producer, consumer, Airflow, Redshift loader
- **Kafka:** IAM authentication via MSK (no plaintext credentials)
- **Redshift:** IAM-based authentication, no password in connection strings
- **Airflow:** Secrets backend (AWS Secrets Manager) for connection strings

### Network
- **VPC:** Data-plane services stay in private subnets; public subnets are used only for ingress ALBs and optional demo shell tasks.
- **Security groups:** Least-privilege access between services
- **NAT gateway:** For outbound internet access (pip, Docker pulls)
- **Public exposure scope:** Optional Streamlit demo shell is internet-facing via ALB; MSK, EMR, and Redshift remain VPC-only.
