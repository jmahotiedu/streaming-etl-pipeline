-- Redshift DDL for IoT Streaming ETL Pipeline
-- Fact and dimension tables for sensor analytics

-- Fact table: windowed sensor reading aggregations from Gold layer
CREATE TABLE IF NOT EXISTS fact_sensor_readings (
    reading_id BIGINT IDENTITY(1,1),
    sensor_id VARCHAR(20) NOT NULL,
    sensor_type VARCHAR(20) NOT NULL,
    location VARCHAR(50),
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    avg_value DOUBLE PRECISION,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    reading_count INTEGER,
    stddev_value DOUBLE PRECISION,
    loaded_at TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (reading_id)
)
DISTSTYLE KEY
DISTKEY (sensor_id)
SORTKEY (window_start);

-- Dimension: sensor metadata
CREATE TABLE IF NOT EXISTS dim_sensors (
    sensor_id VARCHAR(20) PRIMARY KEY,
    sensor_type VARCHAR(20) NOT NULL,
    location VARCHAR(50),
    first_seen TIMESTAMP,
    last_seen TIMESTAMP
)
DISTSTYLE ALL;

-- Dimension: time for efficient date-based joins
CREATE TABLE IF NOT EXISTS dim_time (
    time_key INTEGER PRIMARY KEY,
    full_timestamp TIMESTAMP NOT NULL,
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
)
DISTSTYLE ALL
SORTKEY (full_timestamp);

-- Populate dim_time for 2024-2026 (hourly granularity)
INSERT INTO dim_time (time_key, full_timestamp, hour, day, month, year, day_of_week, is_weekend)
SELECT
    ROW_NUMBER() OVER (ORDER BY ts) AS time_key,
    ts AS full_timestamp,
    EXTRACT(HOUR FROM ts) AS hour,
    EXTRACT(DAY FROM ts) AS day,
    EXTRACT(MONTH FROM ts) AS month,
    EXTRACT(YEAR FROM ts) AS year,
    EXTRACT(DOW FROM ts) AS day_of_week,
    CASE WHEN EXTRACT(DOW FROM ts) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM (
    SELECT
        DATEADD(HOUR, seq, '2024-01-01'::TIMESTAMP) AS ts
    FROM (
        SELECT ROW_NUMBER() OVER () - 1 AS seq
        FROM stl_scan
        LIMIT 26280  -- ~3 years of hours
    )
)
WHERE NOT EXISTS (SELECT 1 FROM dim_time LIMIT 1);
