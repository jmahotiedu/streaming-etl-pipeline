"""Great Expectations integration for data quality validation.

Runs expectation suites against Bronze and Silver data layers,
raising on validation failure so Airflow tasks can catch issues.
"""

import json
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Default paths relative to project root
DEFAULT_GE_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "great_expectations")
BRONZE_SUITE_PATH = os.path.join(DEFAULT_GE_DIR, "expectations", "sensor_bronze.json")
SILVER_SUITE_PATH = os.path.join(DEFAULT_GE_DIR, "expectations", "sensor_silver.json")

SENSOR_TYPES = {"temperature", "humidity", "pressure", "vibration"}

VALUE_RANGES = {
    "temperature": (-50.0, 150.0),
    "humidity": (0.0, 100.0),
    "pressure": (300.0, 1100.0),
    "vibration": (0.0, 10.0),
}


def load_expectations(suite_path: str) -> dict:
    """Load an expectation suite from a JSON file."""
    with open(suite_path) as f:
        return json.load(f)


def validate_bronze_dataframe(df, suite_path: str | None = None) -> dict:
    """Validate a Bronze layer DataFrame against expectations.

    Performs structural and value-level checks:
    - No null sensor_id or timestamp
    - sensor_type in known set
    - value within physical bounds per sensor type
    - No more than 1% nulls in any column

    Args:
        df: pandas DataFrame of Bronze data.
        suite_path: Path to expectations JSON (unused for manual checks,
            reserved for GE context integration).

    Returns:
        Dictionary with 'success' bool and 'results' list of check outcomes.
    """
    results = []
    total_rows = len(df)

    if total_rows == 0:
        return {"success": False, "results": [{"check": "non_empty", "success": False}]}

    # Check: sensor_id not null
    null_sensor_ids = df["sensor_id"].isnull().sum()
    results.append({
        "check": "sensor_id_not_null",
        "success": null_sensor_ids == 0,
        "detail": f"{null_sensor_ids} null sensor_ids",
    })

    # Check: timestamp not null
    null_timestamps = df["timestamp"].isnull().sum()
    results.append({
        "check": "timestamp_not_null",
        "success": null_timestamps == 0,
        "detail": f"{null_timestamps} null timestamps",
    })

    # Check: sensor_type in known set
    unknown_types = set(df["sensor_type"].dropna().unique()) - SENSOR_TYPES
    results.append({
        "check": "sensor_type_valid",
        "success": len(unknown_types) == 0,
        "detail": f"unknown types: {unknown_types}" if unknown_types else "all valid",
    })

    # Check: value within physical bounds per sensor type
    out_of_range = 0
    for sensor_type, (vmin, vmax) in VALUE_RANGES.items():
        mask = df["sensor_type"] == sensor_type
        subset = df.loc[mask, "value"]
        violations = ((subset < vmin) | (subset > vmax)).sum()
        out_of_range += violations

    results.append({
        "check": "value_in_physical_range",
        "success": out_of_range == 0,
        "detail": f"{out_of_range} out-of-range values",
    })

    # Check: no more than 1% nulls in any column
    max_null_pct = 0.0
    for column in df.columns:
        null_pct = df[column].isnull().sum() / total_rows
        max_null_pct = max(max_null_pct, null_pct)

    results.append({
        "check": "null_rate_below_1pct",
        "success": max_null_pct <= 0.01,
        "detail": f"max null rate: {max_null_pct:.4f}",
    })

    overall_success = all(r["success"] for r in results)
    return {"success": overall_success, "results": results}


def validate_silver_dataframe(df, bronze_count: int | None = None) -> dict:
    """Validate a Silver layer DataFrame against expectations.

    Checks:
    - No duplicate (sensor_id, timestamp) pairs
    - is_anomaly column exists and is boolean
    - Row count within 5% of Bronze count (if provided)

    Args:
        df: pandas DataFrame of Silver data.
        bronze_count: Row count of corresponding Bronze data for coverage check.

    Returns:
        Dictionary with 'success' bool and 'results' list.
    """
    results = []
    total_rows = len(df)

    if total_rows == 0:
        return {"success": False, "results": [{"check": "non_empty", "success": False}]}

    # Check: no duplicates on (sensor_id, timestamp)
    dup_count = df.duplicated(subset=["sensor_id", "event_time"], keep=False).sum()
    results.append({
        "check": "no_duplicates",
        "success": dup_count == 0,
        "detail": f"{dup_count} duplicate rows",
    })

    # Check: is_anomaly column exists and is boolean-like
    has_anomaly = "is_anomaly" in df.columns
    results.append({
        "check": "is_anomaly_exists",
        "success": has_anomaly,
        "detail": "column present" if has_anomaly else "column missing",
    })

    if has_anomaly:
        valid_values = df["is_anomaly"].isin([True, False, 0, 1]).all()
        results.append({
            "check": "is_anomaly_boolean",
            "success": valid_values,
            "detail": "all boolean" if valid_values else "non-boolean values found",
        })

    # Check: row count within 5% of Bronze (if provided)
    if bronze_count is not None and bronze_count > 0:
        ratio = total_rows / bronze_count
        within_5pct = 0.95 <= ratio <= 1.05
        results.append({
            "check": "row_count_coverage",
            "success": within_5pct,
            "detail": f"silver/bronze ratio: {ratio:.4f}",
        })

    overall_success = all(r["success"] for r in results)
    return {"success": overall_success, "results": results}


def run_validation(
    layer: str,
    data_path: str,
    bronze_count: int | None = None,
) -> bool:
    """Run validation for a given data layer.

    Args:
        layer: Either 'bronze' or 'silver'.
        data_path: Path to Parquet data.
        bronze_count: Bronze row count (only needed for silver validation).

    Returns:
        True if validation passes.

    Raises:
        ValueError: If validation fails.
    """
    import pandas as pd

    logger.info("Validating %s data at %s", layer, data_path)
    df = pd.read_parquet(data_path)

    if layer == "bronze":
        result = validate_bronze_dataframe(df)
    elif layer == "silver":
        result = validate_silver_dataframe(df, bronze_count=bronze_count)
    else:
        raise ValueError(f"Unknown layer: {layer}")

    for check in result["results"]:
        status = "PASS" if check["success"] else "FAIL"
        logger.info("[%s] %s: %s", status, check["check"], check.get("detail", ""))

    if not result["success"]:
        failed = [r["check"] for r in result["results"] if not r["success"]]
        raise ValueError(f"Data quality validation failed for {layer}: {failed}")

    logger.info("Validation passed for %s layer", layer)
    return True
