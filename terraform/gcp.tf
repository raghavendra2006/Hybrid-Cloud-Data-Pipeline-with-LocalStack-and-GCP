# ============================================
# GCP Resources
# ============================================
# Provisions Pub/Sub topic, Cloud SQL (PostgreSQL),
# Cloud Function, and supporting resources.
# ============================================

# ----- Enable Required APIs -----
resource "google_project_service" "pubsub_api" {
  service            = "pubsub.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "sqladmin_api" {
  service            = "sqladmin.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "cloudfunctions_api" {
  service            = "cloudfunctions.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "cloudbuild_api" {
  service            = "cloudbuild.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "storage_api" {
  service            = "storage.googleapis.com"
  disable_on_destroy = false
}

# ============================================
# Pub/Sub
# ============================================
resource "google_pubsub_topic" "localstack_events" {
  name = var.pubsub_topic_name

  message_retention_duration = "86400s" # 24h retention

  labels = {
    project     = "hybrid-cloud-pipeline"
    environment = "production"
  }

  depends_on = [google_project_service.pubsub_api]
}

# Dead-letter topic for failed messages
resource "google_pubsub_topic" "localstack_events_dlq" {
  name = "${var.pubsub_topic_name}-dlq"

  labels = {
    project = "hybrid-cloud-pipeline"
    purpose = "dead-letter"
  }

  depends_on = [google_project_service.pubsub_api]
}

# Subscription with dead-letter policy for monitoring
resource "google_pubsub_subscription" "localstack_events_sub" {
  name  = "${var.pubsub_topic_name}-subscription"
  topic = google_pubsub_topic.localstack_events.name

  ack_deadline_seconds       = 120
  message_retention_duration = "604800s" # 7 days
  retain_acked_messages      = false

  expiration_policy {
    ttl = "" # never expire
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.localstack_events_dlq.id
    max_delivery_attempts = 5
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  depends_on = [google_project_service.pubsub_api]
}

# ============================================
# Cloud SQL (PostgreSQL)
# ============================================
resource "google_sql_database_instance" "pipeline_db" {
  name             = "hybrid-pipeline-db"
  database_version = "POSTGRES_14"
  region           = var.gcp_region

  settings {
    tier              = var.cloud_sql_tier
    availability_type = "ZONAL"
    disk_size         = 10
    disk_type         = "PD_HDD"

    ip_configuration {
      ipv4_enabled = true

      # NOTE: In production, restrict this to Cloud Function egress IPs
      # or use private IP with VPC connector. Open for evaluation purposes.
      authorized_networks {
        name  = "allow-all-for-evaluation"
        value = "0.0.0.0/0"
      }
    }

    database_flags {
      name  = "max_connections"
      value = "100"
    }

    backup_configuration {
      enabled    = true
      start_time = "03:00"
    }
  }

  deletion_protection = false

  depends_on = [google_project_service.sqladmin_api]
}

resource "google_sql_database" "pipelinedb" {
  name     = "pipelinedb"
  instance = google_sql_database_instance.pipeline_db.name
}

resource "google_sql_user" "pipeline_user" {
  name     = "pipeline_user"
  instance = google_sql_database_instance.pipeline_db.name
  password = var.cloud_sql_password
}

# ============================================
# Cloud Storage Bucket (for Cloud Function source)
# ============================================
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "google_storage_bucket" "function_source" {
  name     = "${var.gcp_project_id}-fn-src-${random_id.bucket_suffix.hex}"
  location = var.gcp_region

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      num_newer_versions = 5
    }
  }

  depends_on = [google_project_service.storage_api]
}

# Package the Cloud Function source code
data "archive_file" "function_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/processor_function"
  output_path = "${path.module}/../dist/processor_function.zip"
}

resource "google_storage_bucket_object" "function_source_zip" {
  name   = "processor_function-${data.archive_file.function_zip.output_md5}.zip"
  bucket = google_storage_bucket.function_source.name
  source = data.archive_file.function_zip.output_path
}

# ============================================
# Cloud Function
# ============================================
resource "google_cloudfunctions_function" "processor" {
  name        = "pipeline-processor"
  description = "Processes messages from Pub/Sub, writes to Cloud SQL and LocalStack DynamoDB"
  runtime     = "python311"
  region      = var.gcp_region

  available_memory_mb   = 256
  timeout               = 120
  max_instances         = 10
  min_instances         = 0
  entry_point           = "process_pubsub_event"
  source_archive_bucket = google_storage_bucket.function_source.name
  source_archive_object = google_storage_bucket_object.function_source_zip.name

  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = google_pubsub_topic.localstack_events.id
    failure_policy {
      retry = true
    }
  }

  environment_variables = {
    CLOUD_SQL_CONNECTION_NAME = google_sql_database_instance.pipeline_db.connection_name
    CLOUD_SQL_DATABASE        = "pipelinedb"
    CLOUD_SQL_USER            = "pipeline_user"
    CLOUD_SQL_PASSWORD        = var.cloud_sql_password
    DYNAMODB_ENDPOINT         = var.dynamodb_endpoint_for_function
    DYNAMODB_TABLE_NAME       = var.dynamodb_table_name
    AWS_ACCESS_KEY_ID         = "test"
    AWS_SECRET_ACCESS_KEY     = "test"
    AWS_DEFAULT_REGION        = var.aws_region
  }

  depends_on = [
    google_project_service.cloudfunctions_api,
    google_project_service.cloudbuild_api,
    google_sql_database.pipelinedb,
    google_sql_user.pipeline_user
  ]
}

# ============================================
# SQL Schema Initialization
# ============================================
# NOTE: The records table is created by the Cloud Function on first
# invocation via CREATE TABLE IF NOT EXISTS. This is deliberate:
# Terraform's google_sql_database_instance does not natively support
# DDL execution. The Cloud Function uses autocommit + idempotent DDL.
#
# For strict schema-as-code, use a provisioner + Cloud SQL Proxy:
# resource "null_resource" "init_schema" {
#   provisioner "local-exec" {
#     command = <<-EOT
#       PGPASSWORD=${var.cloud_sql_password} psql \
#         -h ${google_sql_database_instance.pipeline_db.public_ip_address} \
#         -U pipeline_user -d pipelinedb -c "
#         CREATE TABLE IF NOT EXISTS records (
#           id VARCHAR(255) PRIMARY KEY NOT NULL,
#           user_email VARCHAR(255) NOT NULL,
#           value INTEGER NOT NULL,
#           processed_at TIMESTAMP NOT NULL
#         );"
#     EOT
#   }
#   depends_on = [google_sql_database.pipelinedb, google_sql_user.pipeline_user]
# }
