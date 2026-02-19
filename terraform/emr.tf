resource "aws_s3_object" "emr_bootstrap_install_deps" {
  count  = var.enable_emr ? 1 : 0
  bucket = aws_s3_bucket.bronze.id
  key    = "bootstrap/install-deps.sh"
  source = "${path.module}/bootstrap/install-deps.sh"
  etag   = filemd5("${path.module}/bootstrap/install-deps.sh")
}

resource "aws_emr_cluster" "spark" {
  count         = var.enable_emr ? 1 : 0
  name          = "streaming-etl-spark-${var.environment}"
  release_label = "emr-7.0.0"
  applications  = ["Spark", "Hadoop", "Livy"]
  service_role  = aws_iam_role.emr_service[0].arn

  ec2_attributes {
    instance_profile                  = aws_iam_instance_profile.emr_ec2[0].arn
    subnet_id                         = aws_subnet.private[0].id
    emr_managed_master_security_group = aws_security_group.default.id
    emr_managed_slave_security_group  = aws_security_group.default.id
  }

  master_instance_group {
    instance_type  = var.emr_master_instance_type
    instance_count = 1
    name           = "master"
  }

  core_instance_group {
    instance_type  = var.emr_core_instance_type
    instance_count = var.emr_core_instance_count
    name           = "core"

    ebs_config {
      size                 = 64
      type                 = "gp3"
      volumes_per_instance = 1
    }
  }

  configurations_json = jsonencode([
    {
      Classification = "spark-defaults"
      Properties = {
        "spark.sql.adaptive.enabled"          = "true"
        "spark.sql.streaming.schemaInference" = "true"
        "spark.jars.packages"                 = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        "spark.hadoop.fs.s3a.impl"            = "org.apache.hadoop.fs.s3a.S3AFileSystem"
      }
    },
    {
      Classification = "spark-env"
      Configurations = [
        {
          Classification = "export"
          Properties = {
            "PYSPARK_PYTHON" = "/usr/bin/python3"
          }
        }
      ]
    }
  ])

  bootstrap_action {
    name = "install-python-deps"
    path = "s3://${aws_s3_object.emr_bootstrap_install_deps[0].bucket}/${aws_s3_object.emr_bootstrap_install_deps[0].key}"
  }

  log_uri = "s3://${aws_s3_bucket.bronze.id}/emr-logs/"
  auto_termination_policy {
    idle_timeout = 3600
  }

  tags = {
    Name = "streaming-etl-spark"
  }
}

# IAM roles for EMR
resource "aws_iam_role" "emr_service" {
  count = var.enable_emr ? 1 : 0
  name = "streaming-etl-emr-service-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "elasticmapreduce.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "emr_service" {
  count      = var.enable_emr ? 1 : 0
  role       = aws_iam_role.emr_service[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

resource "aws_iam_role" "emr_ec2" {
  count = var.enable_emr ? 1 : 0
  name = "streaming-etl-emr-ec2-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "emr_ec2" {
  count      = var.enable_emr ? 1 : 0
  role       = aws_iam_role.emr_ec2[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

resource "aws_iam_role_policy" "emr_ec2_s3" {
  count = var.enable_emr ? 1 : 0
  name = "streaming-etl-emr-s3-access"
  role = aws_iam_role.emr_ec2[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"]
      Resource = [
        aws_s3_bucket.bronze.arn, "${aws_s3_bucket.bronze.arn}/*",
        aws_s3_bucket.silver.arn, "${aws_s3_bucket.silver.arn}/*",
        aws_s3_bucket.gold.arn, "${aws_s3_bucket.gold.arn}/*",
      ]
    }]
  })
}

resource "aws_iam_instance_profile" "emr_ec2" {
  count = var.enable_emr ? 1 : 0
  name = "streaming-etl-emr-ec2-${var.environment}"
  role = aws_iam_role.emr_ec2[0].name
}
