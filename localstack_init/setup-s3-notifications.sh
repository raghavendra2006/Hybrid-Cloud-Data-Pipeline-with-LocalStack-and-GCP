#!/bin/bash
# ============================================
# LocalStack Initialization Script
# ============================================
# This script runs automatically when LocalStack
# reaches the "ready" state. It creates the S3
# bucket, SQS queue, DynamoDB table, and configures
# the S3→SQS event notification.
#
# This provides a fallback in case Terraform has
# not been applied yet, ensuring the bridge app
# has resources to poll immediately.
# ============================================

set -euo pipefail

echo "================================================"
echo "  LocalStack Init: Setting up AWS resources..."
echo "================================================"

AWS_ENDPOINT="http://localhost:4566"
REGION="us-east-1"

# ----- Create S3 Bucket -----
echo "[1/5] Creating S3 bucket: hybrid-cloud-bucket"
awslocal s3 mb s3://hybrid-cloud-bucket --region $REGION 2>/dev/null || \
    echo "  → Bucket already exists, skipping."

# ----- Create SQS DLQ -----
echo "[2/5] Creating SQS dead-letter queue: data-processing-queue-dlq"
DLQ_URL=$(awslocal sqs create-queue \
    --queue-name data-processing-queue-dlq \
    --region $REGION \
    --output text --query 'QueueUrl' 2>/dev/null) || \
    DLQ_URL=$(awslocal sqs get-queue-url \
        --queue-name data-processing-queue-dlq \
        --output text --query 'QueueUrl')
echo "  → DLQ URL: $DLQ_URL"

DLQ_ARN=$(awslocal sqs get-queue-attributes \
    --queue-url "$DLQ_URL" \
    --attribute-names QueueArn \
    --output text --query 'Attributes.QueueArn')

# ----- Create SQS Queue -----
echo "[3/5] Creating SQS queue: data-processing-queue"
QUEUE_URL=$(awslocal sqs create-queue \
    --queue-name data-processing-queue \
    --attributes '{
        "VisibilityTimeout": "60",
        "MessageRetentionPeriod": "86400",
        "ReceiveMessageWaitTimeSeconds": "20",
        "RedrivePolicy": "{\"deadLetterTargetArn\":\"'"$DLQ_ARN"'\",\"maxReceiveCount\":\"3\"}"
    }' \
    --region $REGION \
    --output text --query 'QueueUrl' 2>/dev/null) || \
    QUEUE_URL=$(awslocal sqs get-queue-url \
        --queue-name data-processing-queue \
        --output text --query 'QueueUrl')
echo "  → Queue URL: $QUEUE_URL"

QUEUE_ARN=$(awslocal sqs get-queue-attributes \
    --queue-url "$QUEUE_URL" \
    --attribute-names QueueArn \
    --output text --query 'Attributes.QueueArn')

# ----- Set SQS Queue Policy -----
echo "[3.5/5] Setting SQS queue policy to allow S3 notifications"
awslocal sqs set-queue-attributes \
    --queue-url "$QUEUE_URL" \
    --attributes '{
        "Policy": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"AllowS3\",\"Effect\":\"Allow\",\"Principal\":\"*\",\"Action\":\"sqs:SendMessage\",\"Resource\":\"'"$QUEUE_ARN"'\"}]}"
    }'

# ----- Create DynamoDB Table -----
echo "[4/5] Creating DynamoDB table: processed-records"
awslocal dynamodb create-table \
    --table-name processed-records \
    --attribute-definitions AttributeName=recordId,AttributeType=S \
    --key-schema AttributeName=recordId,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region $REGION 2>/dev/null || \
    echo "  → Table already exists, skipping."

# ----- Configure S3 Event Notification -----
echo "[5/5] Configuring S3→SQS event notification"
awslocal s3api put-bucket-notification-configuration \
    --bucket hybrid-cloud-bucket \
    --notification-configuration '{
        "QueueConfigurations": [
            {
                "QueueArn": "'"$QUEUE_ARN"'",
                "Events": ["s3:ObjectCreated:*"],
                "Filter": {
                    "Key": {
                        "FilterRules": [
                            {
                                "Name": "suffix",
                                "Value": ".json"
                            }
                        ]
                    }
                }
            }
        ]
    }'

echo ""
echo "================================================"
echo "  LocalStack Init: Complete!"
echo "  S3 Bucket:     hybrid-cloud-bucket"
echo "  SQS Queue:     $QUEUE_URL"
echo "  SQS DLQ:       $DLQ_URL"
echo "  DynamoDB Table: processed-records"
echo "================================================"
