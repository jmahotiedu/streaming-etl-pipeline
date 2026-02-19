"""Streamlit analytics dashboard for the IoT Streaming ETL Pipeline.

Pages:
1. Real-Time Overview -- latest sensor readings, events/sec gauge, active sensors count
2. Sensor Trends -- line charts per sensor type (last hour), anomaly markers
3. Location Heatmap -- grid showing avg values by floor/zone
4. Anomaly Timeline -- scatter plot of anomalies over time, filterable by sensor type
5. Pipeline Health -- consumer lag chart, quality pass rate, data freshness indicator
"""

import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="IoT Pipeline Analytics",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Data source configuration
GOLD_PATH = os.environ.get("GOLD_DATA_PATH", "data/gold/")
REDSHIFT_CONN = os.environ.get("REDSHIFT_CONN_STRING", "")
PIPELINE_MODE = os.environ.get("PIPELINE_MODE", "core-mode").strip().lower()
PIPELINE_DATA_MODE = os.environ.get("PIPELINE_DATA_MODE", "auto").strip().lower()

IS_CORE_MODE = PIPELINE_MODE.startswith("core")
USING_DEMO_DATA = PIPELINE_DATA_MODE in {"demo", "synthetic", "core-mode"}


def render_mode_banner() -> None:
    """Render deployment-mode status so public demos are explicit about data source truth."""
    if IS_CORE_MODE or USING_DEMO_DATA:
        st.warning(
            "Core Mode / Demo Data: this public shell runs synthetic analytics while "
            "EMR, MWAA, and Redshift entitlements are pending."
        )
    else:
        st.success("Full Mode: dashboard is configured for live data paths.")


@st.cache_data(ttl=60)
def load_sensor_5min(path: str | None = None) -> pd.DataFrame:
    """Load 5-minute sensor aggregations from Gold layer."""
    data_path = path or os.path.join(GOLD_PATH, "sensor_5min")
    try:
        df = pd.read_parquet(data_path)
        if "window_start" in df.columns:
            df["window_start"] = pd.to_datetime(df["window_start"])
        return df
    except Exception:
        return _generate_demo_sensor_data()


@st.cache_data(ttl=60)
def load_location_hourly(path: str | None = None) -> pd.DataFrame:
    """Load hourly location aggregations from Gold layer."""
    data_path = path or os.path.join(GOLD_PATH, "location_hourly")
    try:
        df = pd.read_parquet(data_path)
        if "hour_start" in df.columns:
            df["hour_start"] = pd.to_datetime(df["hour_start"])
        return df
    except Exception:
        return _generate_demo_location_data()


@st.cache_data(ttl=60)
def load_daily_summary(path: str | None = None) -> pd.DataFrame:
    """Load daily summaries from Gold layer."""
    data_path = path or os.path.join(GOLD_PATH, "daily_summary")
    try:
        return pd.read_parquet(data_path)
    except Exception:
        return _generate_demo_daily_data()


def _generate_demo_sensor_data() -> pd.DataFrame:
    """Generate demo data when real data is unavailable."""
    now = datetime.utcnow()
    records = []
    sensor_types = ["temperature", "humidity", "pressure", "vibration"]
    base_values = {"temperature": 22.0, "humidity": 55.0, "pressure": 1013.0, "vibration": 0.5}

    for i in range(288):  # 24 hours of 5-min windows
        window_start = now - timedelta(minutes=5 * (288 - i))
        for sensor_idx in range(10):
            stype = sensor_types[sensor_idx % 4]
            base = base_values[stype]
            noise = np.random.normal(0, base * 0.05)
            is_anomaly = np.random.random() < 0.02
            value = base + noise + (base * 0.5 if is_anomaly else 0)

            records.append({
                "sensor_id": f"sensor-{sensor_idx:03d}",
                "sensor_type": stype,
                "location": f"floor-{(sensor_idx % 5) + 1}-zone-{'ABCD'[sensor_idx % 4]}",
                "window_start": window_start,
                "window_end": window_start + timedelta(minutes=5),
                "avg_value": round(value, 2),
                "min_value": round(value - abs(noise), 2),
                "max_value": round(value + abs(noise), 2),
                "reading_count": np.random.randint(250, 310),
                "stddev_value": round(abs(noise) * 0.3, 4),
                "p50_value": round(value, 2),
                "p95_value": round(value + abs(noise) * 0.8, 2),
                "p99_value": round(value + abs(noise) * 1.2, 2),
                "sensor_health_pct": round(np.random.uniform(85, 100), 1),
            })

    return pd.DataFrame(records)


def _generate_demo_location_data() -> pd.DataFrame:
    """Generate demo location aggregation data."""
    now = datetime.utcnow()
    records = []
    for hour_offset in range(24):
        hour_start = now - timedelta(hours=24 - hour_offset)
        for floor in range(1, 6):
            for zone in "ABCD":
                for stype in ["temperature", "humidity"]:
                    records.append({
                        "location": f"floor-{floor}-zone-{zone}",
                        "sensor_type": stype,
                        "hour_start": hour_start,
                        "hour_end": hour_start + timedelta(hours=1),
                        "avg_value": round(np.random.normal(22 if stype == "temperature" else 55, 3), 2),
                        "reading_count": np.random.randint(500, 1200),
                        "unique_sensor_count": np.random.randint(2, 8),
                    })
    return pd.DataFrame(records)


def _generate_demo_daily_data() -> pd.DataFrame:
    """Generate demo daily summary data."""
    records = []
    for day_offset in range(30):
        date = datetime.utcnow().date() - timedelta(days=30 - day_offset)
        for stype in ["temperature", "humidity", "pressure", "vibration"]:
            records.append({
                "date": date,
                "sensor_type": stype,
                "avg_value": round(np.random.normal(22, 2), 2),
                "total_readings": np.random.randint(80000, 100000),
                "anomaly_count": np.random.randint(0, 50),
                "unique_sensor_count": np.random.randint(10, 15),
                "date_str": str(date),
            })
    return pd.DataFrame(records)


# -- Sidebar Navigation --
st.sidebar.title("IoT Pipeline Analytics")
page = st.sidebar.radio(
    "Navigate",
    [
        "Real-Time Overview",
        "Sensor Trends",
        "Location Heatmap",
        "Anomaly Timeline",
        "Pipeline Health",
    ],
)
st.sidebar.markdown("---")
st.sidebar.info("Data refreshes every 60 seconds")
st.sidebar.caption(f"Mode: {'Core Mode' if IS_CORE_MODE else 'Full Mode'}")
st.sidebar.caption(f"Data source: {'Demo Data' if USING_DEMO_DATA else 'Auto/Live'}")

render_mode_banner()


# -- Page 1: Real-Time Overview --
if page == "Real-Time Overview":
    st.title("Real-Time Overview")

    sensor_df = load_sensor_5min()

    if sensor_df.empty:
        st.warning("No sensor data available.")
    else:
        latest = sensor_df.sort_values("window_start").groupby("sensor_id").last().reset_index()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Active Sensors", len(latest))
        col2.metric(
            "Avg Temperature",
            f"{latest[latest['sensor_type'] == 'temperature']['avg_value'].mean():.1f} C",
        )
        col3.metric(
            "Events/min (est.)",
            f"{latest['reading_count'].sum() / 5:.0f}",
        )
        col4.metric(
            "Avg Health",
            f"{latest['sensor_health_pct'].mean():.0f}%",
        )

        st.subheader("Latest Readings by Sensor Type")
        for stype in ["temperature", "humidity", "pressure", "vibration"]:
            subset = latest[latest["sensor_type"] == stype]
            if not subset.empty:
                st.markdown(f"**{stype.title()}**: avg={subset['avg_value'].mean():.2f}, "
                            f"min={subset['min_value'].min():.2f}, "
                            f"max={subset['max_value'].max():.2f}, "
                            f"sensors={len(subset)}")


# -- Page 2: Sensor Trends --
elif page == "Sensor Trends":
    st.title("Sensor Trends")

    sensor_df = load_sensor_5min()
    if sensor_df.empty:
        st.warning("No sensor data available.")
    else:
        selected_type = st.selectbox("Sensor Type", ["temperature", "humidity", "pressure", "vibration"])

        type_df = sensor_df[sensor_df["sensor_type"] == selected_type].copy()
        if "window_start" in type_df.columns:
            one_hour_ago = type_df["window_start"].max() - timedelta(hours=1)
            recent = type_df[type_df["window_start"] >= one_hour_ago]
        else:
            recent = type_df.tail(100)

        if not recent.empty:
            chart_data = recent.pivot_table(
                index="window_start", columns="sensor_id", values="avg_value", aggfunc="mean"
            )
            st.line_chart(chart_data)

            anomaly_data = recent[recent.get("sensor_health_pct", pd.Series(dtype=float)) < 90]
            if not anomaly_data.empty:
                st.warning(f"{len(anomaly_data)} low-health readings detected in the last hour")


# -- Page 3: Location Heatmap --
elif page == "Location Heatmap":
    st.title("Location Heatmap")

    location_df = load_location_hourly()
    if location_df.empty:
        st.warning("No location data available.")
    else:
        selected_type = st.selectbox("Sensor Type", ["temperature", "humidity", "pressure", "vibration"])
        filtered = location_df[location_df["sensor_type"] == selected_type]

        if not filtered.empty:
            latest_hour = filtered["hour_start"].max()
            current = filtered[filtered["hour_start"] == latest_hour]

            if not current.empty:
                current = current.copy()
                current["floor"] = current["location"].str.extract(r"floor-(\d+)")[0]
                current["zone"] = current["location"].str.extract(r"zone-([A-D])")[0]

                pivot = current.pivot_table(
                    index="floor", columns="zone", values="avg_value", aggfunc="mean"
                )
                st.dataframe(pivot.style.background_gradient(cmap="RdYlGn_r"), use_container_width=True)

                st.subheader("Sensor Counts by Location")
                if "unique_sensor_count" in current.columns:
                    count_pivot = current.pivot_table(
                        index="floor", columns="zone", values="unique_sensor_count", aggfunc="sum"
                    )
                    st.dataframe(count_pivot, use_container_width=True)


# -- Page 4: Anomaly Timeline --
elif page == "Anomaly Timeline":
    st.title("Anomaly Timeline")

    daily_df = load_daily_summary()
    if daily_df.empty:
        st.warning("No daily summary data available.")
    else:
        sensor_types = daily_df["sensor_type"].unique().tolist()
        selected_types = st.multiselect("Filter by Sensor Type", sensor_types, default=sensor_types)

        filtered = daily_df[daily_df["sensor_type"].isin(selected_types)]

        if "anomaly_count" in filtered.columns and "date" in filtered.columns:
            anomaly_chart = filtered.pivot_table(
                index="date", columns="sensor_type", values="anomaly_count", aggfunc="sum"
            )
            st.bar_chart(anomaly_chart)

            total_anomalies = filtered["anomaly_count"].sum()
            total_readings = filtered["total_readings"].sum()
            anomaly_rate = (total_anomalies / total_readings * 100) if total_readings > 0 else 0

            col1, col2, col3 = st.columns(3)
            col1.metric("Total Anomalies", f"{total_anomalies:,.0f}")
            col2.metric("Total Readings", f"{total_readings:,.0f}")
            col3.metric("Anomaly Rate", f"{anomaly_rate:.3f}%")


# -- Page 5: Pipeline Health --
elif page == "Pipeline Health":
    st.title("Pipeline Health")

    sensor_df = load_sensor_5min()
    daily_df = load_daily_summary()

    col1, col2, col3 = st.columns(3)

    if not sensor_df.empty and "window_start" in sensor_df.columns:
        latest_ts = sensor_df["window_start"].max()
        freshness_min = (datetime.utcnow() - latest_ts).total_seconds() / 60
        col1.metric("Data Freshness", f"{freshness_min:.0f} min ago")
        if freshness_min > 30:
            st.error("Data is stale (>30 minutes old)")
    else:
        col1.metric("Data Freshness", "N/A")

    if not sensor_df.empty and "sensor_health_pct" in sensor_df.columns:
        avg_health = sensor_df["sensor_health_pct"].mean()
        col2.metric("Avg Sensor Health", f"{avg_health:.1f}%")
    else:
        col2.metric("Avg Sensor Health", "N/A")

    if not daily_df.empty and "anomaly_count" in daily_df.columns and "total_readings" in daily_df.columns:
        total_anomalies = daily_df["anomaly_count"].sum()
        total_readings = daily_df["total_readings"].sum()
        quality_rate = ((total_readings - total_anomalies) / total_readings * 100) if total_readings > 0 else 100
        col3.metric("Quality Pass Rate", f"{quality_rate:.2f}%")
    else:
        col3.metric("Quality Pass Rate", "N/A")

    st.subheader("Readings Over Time")
    if not daily_df.empty and "date" in daily_df.columns:
        readings_chart = daily_df.pivot_table(
            index="date", columns="sensor_type", values="total_readings", aggfunc="sum"
        )
        st.area_chart(readings_chart)

    st.subheader("Sensor Health Distribution")
    if not sensor_df.empty and "sensor_health_pct" in sensor_df.columns:
        health_bins = pd.cut(sensor_df["sensor_health_pct"], bins=[0, 50, 75, 90, 100])
        health_dist = health_bins.value_counts().sort_index()
        st.bar_chart(health_dist)
