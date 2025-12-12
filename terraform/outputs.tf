output "msk_bootstrap_servers" {
  description = "MSK bootstrap broker connection string"
  value       = aws_msk_cluster.main.bootstrap_brokers_tls
}

output "msk_zookeeper_connect" {
  description = "MSK Zookeeper connection string"
  value       = aws_msk_cluster.main.zookeeper_connect_string
}

output "emr_master_dns" {
  description = "EMR master node public DNS"
  value       = aws_emr_cluster.spark.master_public_dns
}

output "emr_cluster_id" {
  description = "EMR cluster ID"
  value       = aws_emr_cluster.spark.id
}

output "redshift_endpoint" {
  description = "Redshift Serverless workgroup endpoint"
  value       = aws_redshiftserverless_workgroup.pipeline.endpoint
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
  value       = aws_mwaa_environment.pipeline.webserver_url
}
