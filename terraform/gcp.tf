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
      ipv4_enabled    = true
      authorized_networks {
        name  = "allow-all"
        value = "0.0.0.0/0"
      }
    }

    backup_configuration {
      enabled = false
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
resource "google_storage_bucket" "function_source" {
  name     = "${var.gcp_project_id}-function-source"
  location = var.gcp_region

  uniform_bucket_level_access = true

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
    google_project_service.cloudbuild_api
  ]
}
