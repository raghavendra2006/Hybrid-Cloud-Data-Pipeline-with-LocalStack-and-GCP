# ============================================
# LocalStack Resources (via AWS Provider)
# ============================================
# Provisions S3 bucket, SQS queue with DLQ,
# DynamoDB table, and S3→SQS event notification.
# ============================================

# ----- S3 Bucket -----
resource "aws_s3_bucket" "data_bucket" {
  bucket        = var.s3_bucket_name
  force_destroy = true

  tags = {
    Project     = "hybrid-cloud-pipeline"
    Environment = "localstack"
  }
}

# ----- SQS Dead Letter Queue -----
resource "aws_sqs_queue" "processing_dlq" {
  name                      = "${var.sqs_queue_name}-dlq"
  message_retention_seconds = 1209600 # 14 days

  tags = {
    Project     = "hybrid-cloud-pipeline"
    Environment = "localstack"
    Purpose     = "dead-letter-queue"
  }
}

# ----- SQS Processing Queue -----
resource "aws_sqs_queue" "processing_queue" {
  name                       = var.sqs_queue_name
  visibility_timeout_seconds = 60
  message_retention_seconds  = 86400  # 1 day
  receive_wait_time_seconds  = 20     # Long polling

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.processing_dlq.arn
    maxReceiveCount     = 3
  })

  tags = {
    Project     = "hybrid-cloud-pipeline"
    Environment = "localstack"
  }
}

# ----- SQS Queue Policy: Allow S3 to send messages -----
resource "aws_sqs_queue_policy" "allow_s3_notifications" {
  queue_url = aws_sqs_queue.processing_queue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowS3SendMessage"
        Effect    = "Allow"
        Principal = "*"
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.processing_queue.arn
        Condition = {
          ArnLike = {
            "aws:SourceArn" = aws_s3_bucket.data_bucket.arn
          }
        }
      }
    ]
  })
}

# ----- S3 → SQS Event Notification -----
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.data_bucket.id

  queue {
    queue_arn     = aws_sqs_queue.processing_queue.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".json"
  }

  depends_on = [aws_sqs_queue_policy.allow_s3_notifications]
}

# ----- DynamoDB Table -----
resource "aws_dynamodb_table" "processed_records" {
  name         = var.dynamodb_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "recordId"

  attribute {
    name = "recordId"
    type = "S"
  }

  tags = {
    Project     = "hybrid-cloud-pipeline"
    Environment = "localstack"
  }
}
