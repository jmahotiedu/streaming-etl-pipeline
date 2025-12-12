resource "aws_s3_bucket" "mwaa_dags" {
  bucket = "streaming-etl-mwaa-dags-${var.environment}-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name = "streaming-etl-mwaa-dags"
  }
}

resource "aws_s3_bucket_versioning" "mwaa_dags" {
  bucket = aws_s3_bucket.mwaa_dags.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_public_access_block" "mwaa_dags" {
  bucket                  = aws_s3_bucket.mwaa_dags.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_mwaa_environment" "pipeline" {
  name              = "streaming-etl-airflow-${var.environment}"
  airflow_version   = "2.8.1"
  environment_class = "mw1.small"

  source_bucket_arn    = aws_s3_bucket.mwaa_dags.arn
  dag_s3_path          = "dags/"
  execution_role_arn   = aws_iam_role.mwaa.arn

  max_workers = 2
  min_workers = 1

  network_configuration {
    security_group_ids = [aws_security_group.default.id]
    subnet_ids         = slice(aws_subnet.private[*].id, 0, 2)
  }

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "WARNING"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "WARNING"
    }
    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }

  airflow_configuration_options = {
    "core.default_timezone" = "utc"
    "core.load_examples"    = "false"
  }

  tags = {
    Name = "streaming-etl-airflow"
  }
}

# IAM role for MWAA
resource "aws_iam_role" "mwaa" {
  name = "streaming-etl-mwaa-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action    = "sts:AssumeRole"
        Effect    = "Allow"
        Principal = { Service = "airflow.amazonaws.com" }
      },
      {
        Action    = "sts:AssumeRole"
        Effect    = "Allow"
        Principal = { Service = "airflow-env.amazonaws.com" }
      }
    ]
  })
}

resource "aws_iam_role_policy" "mwaa" {
  name = "streaming-etl-mwaa-policy"
  role = aws_iam_role.mwaa.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject",
        ]
        Resource = [
          aws_s3_bucket.mwaa_dags.arn,
          "${aws_s3_bucket.mwaa_dags.arn}/*",
          aws_s3_bucket.bronze.arn,
          "${aws_s3_bucket.bronze.arn}/*",
          aws_s3_bucket.silver.arn,
          "${aws_s3_bucket.silver.arn}/*",
          aws_s3_bucket.gold.arn,
          "${aws_s3_bucket.gold.arn}/*",
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:GetLogEvents",
          "logs:GetLogRecord",
          "logs:GetLogGroupFields",
          "logs:GetQueryResults",
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:airflow-*"
      },
      {
        Effect = "Allow"
        Action = [
          "elasticmapreduce:AddJobFlowSteps",
          "elasticmapreduce:DescribeStep",
          "elasticmapreduce:ListSteps",
        ]
        Resource = aws_emr_cluster.spark.arn
      },
      {
        Effect   = "Allow"
        Action   = ["airflow:PublishMetrics"]
        Resource = "arn:aws:airflow:${var.aws_region}:${data.aws_caller_identity.current.account_id}:environment/streaming-etl-airflow-${var.environment}"
      },
    ]
  })
}
