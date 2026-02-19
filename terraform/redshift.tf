resource "aws_redshiftserverless_namespace" "pipeline" {
  count               = var.enable_redshift ? 1 : 0
  namespace_name      = "pipeline-analytics-${var.environment}"
  admin_username      = "admin"
  admin_user_password = var.redshift_admin_password
  db_name             = "analytics"

  iam_roles = [aws_iam_role.redshift[0].arn]

  tags = {
    Name = "pipeline-analytics"
  }
}

resource "aws_redshiftserverless_workgroup" "pipeline" {
  count          = var.enable_redshift ? 1 : 0
  namespace_name = aws_redshiftserverless_namespace.pipeline[0].namespace_name
  workgroup_name = "pipeline-analytics-${var.environment}"
  base_capacity  = var.redshift_base_capacity

  subnet_ids         = aws_subnet.private[*].id
  security_group_ids = [aws_security_group.default.id]

  config_parameter {
    parameter_key   = "auto_mv"
    parameter_value = "true"
  }

  config_parameter {
    parameter_key   = "datestyle"
    parameter_value = "ISO, MDY"
  }

  tags = {
    Name = "pipeline-analytics-workgroup"
  }
}

resource "aws_redshiftserverless_usage_limit" "monthly_rpu_hours" {
  count        = var.enable_redshift ? 1 : 0
  resource_arn = aws_redshiftserverless_workgroup.pipeline[0].arn
  usage_type   = "serverless-compute"
  period       = "monthly"
  amount       = 64

  breach_action = "log"
}

# IAM role for Redshift to access S3
resource "aws_iam_role" "redshift" {
  count = var.enable_redshift ? 1 : 0
  name = "streaming-etl-redshift-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "redshift_s3" {
  count = var.enable_redshift ? 1 : 0
  name = "streaming-etl-redshift-s3-access"
  role = aws_iam_role.redshift[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:ListBucket"]
      Resource = [
        aws_s3_bucket.gold.arn,
        "${aws_s3_bucket.gold.arn}/*",
      ]
    }]
  })
}
