# ============================================
# Terraform Variables
# ============================================

# ----- GCP Variables -----
variable "gcp_project_id" {
  description = "Google Cloud project ID"
  type        = string
}

variable "gcp_region" {
  description = "GCP region for resource deployment"
  type        = string
  default     = "us-central1"
}

variable "gcp_keyfile_path" {
  description = "Path to the GCP service account JSON key file"
  type        = string
  default     = "../gcp-service-account-key.json"
}

# ----- AWS / LocalStack Variables -----
variable "aws_region" {
  description = "AWS region (used for LocalStack)"
  type        = string
  default     = "us-east-1"
}

variable "localstack_endpoint" {
  description = "LocalStack gateway endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ----- Cloud SQL Variables -----
variable "cloud_sql_password" {
  description = "Password for the Cloud SQL pipeline_user"
  type        = string
  sensitive   = true
}

variable "cloud_sql_tier" {
  description = "Cloud SQL machine tier"
  type        = string
  default     = "db-f1-micro"
}

# ----- S3 / SQS / DynamoDB Names -----
variable "s3_bucket_name" {
  description = "Name of the S3 bucket in LocalStack"
  type        = string
  default     = "hybrid-cloud-bucket"
}

variable "sqs_queue_name" {
  description = "Name of the SQS queue in LocalStack"
  type        = string
  default     = "data-processing-queue"
}

variable "dynamodb_table_name" {
  description = "Name of the DynamoDB table in LocalStack"
  type        = string
  default     = "processed-records"
}

# ----- Pub/Sub & Cloud Function -----
variable "pubsub_topic_name" {
  description = "Name of the GCP Pub/Sub topic"
  type        = string
  default     = "localstack-events"
}

variable "dynamodb_endpoint_for_function" {
  description = "DynamoDB endpoint URL accessible from GCP Cloud Function (requires tunnel for local dev)"
  type        = string
  default     = "http://localhost:4566"
}
