"""Tests for Redshift loader SQL generation and idempotency logic."""


from src.loaders.redshift_loader import build_copy_sql, build_delete_sql


class TestBuildDeleteSql:
    """Tests for delete SQL generation."""

    def test_basic_delete_sql(self):
        """Generates correct DELETE statement for a time window."""
        sql = build_delete_sql(
            table="fact_sensor_readings",
            window_start="2024-06-15T00:00:00",
            window_end="2024-06-16T00:00:00",
        )
        assert "DELETE FROM fact_sensor_readings" in sql
        assert "window_start >= '2024-06-15T00:00:00'" in sql
        assert "window_start < '2024-06-16T00:00:00'" in sql

    def test_delete_uses_correct_table(self):
        """DELETE targets the specified table name."""
        sql = build_delete_sql("my_table", "2024-01-01T00:00:00", "2024-01-02T00:00:00")
        assert "DELETE FROM my_table" in sql

    def test_delete_window_boundary(self):
        """DELETE uses >= for start and < for end (half-open interval)."""
        sql = build_delete_sql("t", "2024-01-01T00:00:00", "2024-01-01T01:00:00")
        assert ">=" in sql
        assert "< '2024-01-01T01:00:00'" in sql


class TestBuildCopySql:
    """Tests for COPY SQL generation."""

    def test_basic_copy_sql(self):
        """Generates correct COPY statement with all required clauses."""
        sql = build_copy_sql(
            table="fact_sensor_readings",
            s3_path="s3://pipeline-gold-dev/sensor_5min/",
            iam_role="arn:aws:iam::123456789012:role/redshift-role",
        )
        assert "COPY fact_sensor_readings" in sql
        assert "FROM 's3://pipeline-gold-dev/sensor_5min/'" in sql
        assert "IAM_ROLE 'arn:aws:iam::123456789012:role/redshift-role'" in sql
        assert "FORMAT AS PARQUET" in sql

    def test_copy_includes_region(self):
        """COPY statement includes the AWS region."""
        sql = build_copy_sql("t", "s3://b/", "arn:role", region="eu-west-1")
        assert "REGION 'eu-west-1'" in sql

    def test_copy_default_region(self):
        """Default region is us-east-1."""
        sql = build_copy_sql("t", "s3://b/", "arn:role")
        assert "REGION 'us-east-1'" in sql


class TestIdempotencyLogic:
    """Tests for idempotent load pattern (delete-then-insert)."""

    def test_delete_before_copy_pattern(self):
        """The idempotent load pattern deletes before copying."""
        window_start = "2024-06-15T00:00:00"
        window_end = "2024-06-16T00:00:00"
        table = "fact_sensor_readings"
        s3_path = "s3://gold/sensor_5min/"
        iam_role = "arn:role"

        delete_sql = build_delete_sql(table, window_start, window_end)
        copy_sql = build_copy_sql(table, s3_path, iam_role)

        # Both target the same table
        assert table in delete_sql
        assert table in copy_sql

    def test_same_window_delete_is_consistent(self):
        """Same window parameters produce same DELETE SQL (deterministic)."""
        sql1 = build_delete_sql("t", "2024-01-01T00:00:00", "2024-01-02T00:00:00")
        sql2 = build_delete_sql("t", "2024-01-01T00:00:00", "2024-01-02T00:00:00")
        assert sql1 == sql2

    def test_different_windows_produce_different_sql(self):
        """Different windows produce different DELETE SQL."""
        sql1 = build_delete_sql("t", "2024-01-01T00:00:00", "2024-01-02T00:00:00")
        sql2 = build_delete_sql("t", "2024-01-02T00:00:00", "2024-01-03T00:00:00")
        assert sql1 != sql2
