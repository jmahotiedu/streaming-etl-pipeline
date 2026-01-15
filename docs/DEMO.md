# Step-by-Step Demo

This guide walks through a complete local demo of the IoT Streaming ETL Pipeline.

## Prerequisites

- **Docker** and **Docker Compose** (v2.20+)
- **Python 3.11+** with pip
- **AWS CLI** (configured, or use local-only mode)
- ~8 GB available RAM for Docker containers

## 1. Start All Services

```bash
cd streaming-etl-pipeline
docker-compose up -d
```

Verify all services are healthy:

```bash
docker-compose ps
```

Expected services:
- `zookeeper` (port 2181)
- `kafka` (port 9092)
- `spark-master` (port 8081)
- `spark-worker-1`, `spark-worker-2`
- `postgres` (port 5432)
- `airflow-webserver` (port 8080)
- `airflow-scheduler`
- `prometheus` (port 9090)
- `grafana` (port 3000)

Wait ~60 seconds for all services to stabilize.

## 2. Install Python Dependencies

```bash
pip install -e ".[dev]"
```

## 3. Start the Sensor Producer

```bash
python -m src.producers.sensor_simulator \
  --bootstrap-servers localhost:9092 \
  --num-sensors 50 \
  --events-per-second 100 \
  --anomaly-rate 0.02
```

You should see log output like:
```
Starting producer (continuous mode): 50 sensors, 100 events/sec ...
Produced 1000 events so far
```

## 4. Verify Events in Kafka

In a new terminal:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic sensor-events \
  --from-beginning \
  --max-messages 5
```

You should see JSON sensor events.

## 5. Observe Spark Processing

Open the Spark Master UI: http://localhost:8081

If running the streaming consumer locally:

```bash
spark-submit src/consumers/spark_streaming.py \
  --kafka-servers localhost:9092 \
  --bronze-path /tmp/bronze/sensor-events/ \
  --checkpoint-path /tmp/bronze/checkpoints/ \
  --trigger-interval "10 seconds"
```

## 6. Trigger Airflow DAG Manually

1. Open Airflow UI: http://localhost:8080 (admin / admin)
2. Enable the `streaming_pipeline_dag`
3. Click "Trigger DAG" to run manually
4. Monitor task progress in the Graph view

## 7. Check Data Quality Results

```bash
pytest tests/ -v -k "test_quality"
```

Or check Airflow logs for the `quality_checks` task.

## 8. View Grafana Dashboard

1. Open Grafana: http://localhost:3000 (admin / admin)
2. Navigate to "Pipeline Health" dashboard
3. Observe:
   - Events Produced per Minute
   - Kafka Consumer Lag
   - Processing Latency (p50/p95/p99)
   - Data Quality Pass/Fail
   - Anomaly injection rate

## 9. Query Data (Redshift or Local)

For local development, check Parquet files directly:

```python
import pandas as pd

bronze = pd.read_parquet("/tmp/bronze/sensor-events/")
print(f"Bronze records: {len(bronze)}")
print(bronze.head())
```

## 10. Open Analytics Dashboard

```bash
streamlit run src/dashboard/app.py
```

Navigate through the pages:
- **Real-Time Overview**: Live sensor metrics
- **Sensor Trends**: Per-type line charts
- **Location Heatmap**: Floor/zone grid
- **Anomaly Timeline**: Anomaly scatter plot
- **Pipeline Health**: Consumer lag, quality rate

## 11. Run Tests

```bash
pytest tests/ -v --cov=src
ruff check src/ dags/ tests/
```

## 12. Batch Mode Demo

Send exactly 500 events and exit:

```bash
python -m src.producers.sensor_simulator \
  --bootstrap-servers localhost:9092 \
  --max-events 500
```

## 13. Teardown

```bash
docker-compose down -v
```

This removes all containers and volumes. See `docs/TEARDOWN.md` for AWS resource cleanup.
