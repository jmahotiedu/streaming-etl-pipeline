variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name used for tags and naming"
  type        = string
  default     = "streaming-etl"
}

variable "force_destroy_buckets" {
  description = "Allow Terraform to destroy non-empty demo buckets"
  type        = bool
  default     = true
}

variable "enable_emr" {
  description = "Create EMR resources for managed Spark processing"
  type        = bool
  default     = true
}

variable "enable_mwaa" {
  description = "Create MWAA resources for managed Airflow orchestration"
  type        = bool
  default     = true
}

variable "enable_redshift" {
  description = "Create Redshift Serverless resources for analytics warehouse"
  type        = bool
  default     = true
}

variable "msk_instance_type" {
  description = "Instance type for MSK brokers"
  type        = string
  default     = "kafka.t3.small"
}

variable "msk_broker_count" {
  description = "Number of MSK broker nodes (must be multiple of AZ count)"
  type        = number
  default     = 3
}

variable "emr_master_instance_type" {
  description = "Instance type for EMR master node"
  type        = string
  default     = "m5.large"
}

variable "emr_core_instance_type" {
  description = "Instance type for EMR core nodes"
  type        = string
  default     = "m5.large"
}

variable "emr_core_instance_count" {
  description = "Number of EMR core nodes"
  type        = number
  default     = 1
}

variable "mwaa_environment_class" {
  description = "MWAA environment class"
  type        = string
  default     = "mw1.small"
}

variable "mwaa_min_workers" {
  description = "Minimum MWAA workers"
  type        = number
  default     = 1
}

variable "mwaa_max_workers" {
  description = "Maximum MWAA workers"
  type        = number
  default     = 1
}

variable "redshift_base_capacity" {
  description = "Redshift Serverless base capacity in RPUs"
  type        = number
  default     = 8
}

variable "redshift_admin_password" {
  description = "Admin password for Redshift Serverless namespace"
  type        = string
  default     = null
  nullable    = true
  sensitive   = true
}
