output "msk_bootstrap_servers" {
  description = "MSK bootstrap broker connection string"
  value       = aws_msk_cluster.main.bootstrap_brokers_tls
}

output "msk_cluster_arn" {
  description = "MSK cluster ARN"
  value       = aws_msk_cluster.main.arn
}

output "msk_zookeeper_connect" {
  description = "MSK Zookeeper connection string"
  value       = aws_msk_cluster.main.zookeeper_connect_string
}

output "emr_master_dns" {
  description = "EMR master node public DNS"
  value       = try(aws_emr_cluster.spark[0].master_public_dns, null)
}

output "emr_cluster_id" {
  description = "EMR cluster ID"
  value       = try(aws_emr_cluster.spark[0].id, null)
}

output "redshift_endpoint" {
  description = "Redshift Serverless workgroup endpoint"
  value       = try(aws_redshiftserverless_workgroup.pipeline[0].endpoint[0].address, null)
}

output "s3_bronze_bucket" {
  description = "S3 Bronze layer bucket name"
  value       = aws_s3_bucket.bronze.id
}

output "s3_silver_bucket" {
  description = "S3 Silver layer bucket name"
  value       = aws_s3_bucket.silver.id
}

output "s3_gold_bucket" {
  description = "S3 Gold layer bucket name"
  value       = aws_s3_bucket.gold.id
}

output "mwaa_webserver_url" {
  description = "MWAA Airflow webserver URL"
  value       = try(aws_mwaa_environment.pipeline[0].webserver_url, null)
}

output "producer_ecr_repository_url" {
  description = "ECR repository URL for producer image"
  value       = aws_ecr_repository.producer.repository_url
}

output "spark_ecr_repository_url" {
  description = "ECR repository URL for Spark image"
  value       = aws_ecr_repository.spark.repository_url
}
