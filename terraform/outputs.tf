# ============================================
# Terraform Outputs
# ============================================

# ----- LocalStack Outputs -----
output "s3_bucket_name" {
  description = "Name of the S3 bucket in LocalStack"
  value       = aws_s3_bucket.data_bucket.id
}

output "sqs_queue_url" {
  description = "URL of the SQS processing queue"
  value       = aws_sqs_queue.processing_queue.url
}

output "sqs_dlq_url" {
  description = "URL of the SQS dead-letter queue"
  value       = aws_sqs_queue.processing_dlq.url
}

output "dynamodb_table_name" {
  description = "Name of the DynamoDB processed-records table"
  value       = aws_dynamodb_table.processed_records.name
}

# ----- GCP Outputs -----
output "pubsub_topic_name" {
  description = "Name of the GCP Pub/Sub topic"
  value       = google_pubsub_topic.localstack_events.name
}

output "cloud_sql_instance" {
  description = "Cloud SQL instance connection name"
  value       = google_sql_database_instance.pipeline_db.connection_name
}

output "cloud_sql_public_ip" {
  description = "Public IP of the Cloud SQL instance"
  value       = google_sql_database_instance.pipeline_db.public_ip_address
}

output "cloud_function_name" {
  description = "Name of the deployed Cloud Function"
  value       = google_cloudfunctions_function.processor.name
}

output "cloud_function_url" {
  description = "URL of the deployed Cloud Function"
  value       = google_cloudfunctions_function.processor.https_trigger_url
}
