"""
GCP Cloud Function — Pipeline Processor
========================================
Triggered by messages on the 'localstack-events' Pub/Sub topic.

Processing flow:
  1. Decode and parse the Pub/Sub message (JSON)
  2. Add a processed_at timestamp
  3. Write the record to GCP Cloud SQL (PostgreSQL)
  4. Write the record back to LocalStack DynamoDB

Design:
  - Idempotent: Uses INSERT ... ON CONFLICT DO UPDATE for Cloud SQL
  - Idempotent: Uses put_item for DynamoDB (overwrites existing)
  - Configurable endpoints via environment variables
  - Structured error logging
"""

import base64
import json
import logging
import os
from datetime import datetime, timezone

import boto3
import pg8000
from botocore.config import Config as BotoConfig

# ============================================
# Configuration (from environment variables)
# ============================================
CLOUD_SQL_CONNECTION_NAME = os.environ.get("CLOUD_SQL_CONNECTION_NAME", "")
CLOUD_SQL_DATABASE = os.environ.get("CLOUD_SQL_DATABASE", "pipelinedb")
CLOUD_SQL_USER = os.environ.get("CLOUD_SQL_USER", "pipeline_user")
CLOUD_SQL_PASSWORD = os.environ.get("CLOUD_SQL_PASSWORD", "")
DYNAMODB_ENDPOINT = os.environ.get("DYNAMODB_ENDPOINT", "http://localhost:4566")
DYNAMODB_TABLE_NAME = os.environ.get("DYNAMODB_TABLE_NAME", "processed-records")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

# ============================================
# Logging
# ============================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("processor")


# ============================================
# Database: Cloud SQL (PostgreSQL)
# ============================================
def get_cloud_sql_connection():
    """
    Create a connection to Cloud SQL PostgreSQL.
    Uses Unix socket when running in GCP, TCP when running locally.
    """
    # Check if running in GCP (Cloud Functions environment)
    if os.environ.get("GAE_ENV", "") or os.environ.get("FUNCTION_TARGET", ""):
        # Connect via Unix socket (Cloud SQL Proxy)
        unix_socket = f"/cloudsql/{CLOUD_SQL_CONNECTION_NAME}"
        conn = pg8000.connect(
            user=CLOUD_SQL_USER,
            password=CLOUD_SQL_PASSWORD,
            database=CLOUD_SQL_DATABASE,
            unix_sock=f"{unix_socket}/.s.PGSQL.5432",
        )
    else:
        # Connect via TCP (local development)
        host = os.environ.get("CLOUD_SQL_HOST", "127.0.0.1")
        port = int(os.environ.get("CLOUD_SQL_PORT", "5432"))
        conn = pg8000.connect(
            user=CLOUD_SQL_USER,
            password=CLOUD_SQL_PASSWORD,
            database=CLOUD_SQL_DATABASE,
            host=host,
            port=port,
        )

    conn.autocommit = True
    return conn


def ensure_table_exists(conn):
    """Create the records table if it doesn't exist."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS records (
            id VARCHAR(255) PRIMARY KEY NOT NULL,
            user_email VARCHAR(255) NOT NULL,
            value INTEGER NOT NULL,
            processed_at TIMESTAMP NOT NULL
        )
    """)
    logger.info("Ensured 'records' table exists in Cloud SQL.")


def write_to_cloud_sql(record: dict):
    """
    Write a processed record to Cloud SQL.
    Uses INSERT ... ON CONFLICT for idempotency.
    """
    conn = None
    try:
        conn = get_cloud_sql_connection()
        ensure_table_exists(conn)

        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO records (id, user_email, value, processed_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                user_email = EXCLUDED.user_email,
                value = EXCLUDED.value,
                processed_at = EXCLUDED.processed_at
            """,
            (
                record["recordId"],
                record["userEmail"],
                int(record["value"]),
                record["processedAt"],
            ),
        )
        logger.info(f"✓ Cloud SQL: Upserted record '{record['recordId']}'")

    except Exception as e:
        logger.error(f"✗ Cloud SQL write failed: {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()


# ============================================
# Database: LocalStack DynamoDB
# ============================================
def get_dynamodb_client():
    """Create a boto3 DynamoDB client pointed at the configured endpoint."""
    boto_config = BotoConfig(
        retries={"max_attempts": 3, "mode": "standard"},
        connect_timeout=10,
        read_timeout=30,
    )

    return boto3.client(
        "dynamodb",
        endpoint_url=DYNAMODB_ENDPOINT,
        region_name=AWS_REGION,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        config=boto_config,
    )


def write_to_dynamodb(record: dict):
    """
    Write a processed record to LocalStack DynamoDB.
    Uses put_item which is inherently idempotent (overwrites).
    """
    try:
        client = get_dynamodb_client()
        client.put_item(
            TableName=DYNAMODB_TABLE_NAME,
            Item={
                "recordId": {"S": record["recordId"]},
                "userEmail": {"S": record["userEmail"]},
                "value": {"N": str(int(record["value"]))},
                "processedAt": {"S": record["processedAt"]},
            },
        )
        logger.info(f"✓ DynamoDB: Wrote record '{record['recordId']}'")

    except Exception as e:
        logger.error(f"✗ DynamoDB write failed: {e}", exc_info=True)
        raise


# ============================================
# Cloud Function Entry Point
# ============================================
def process_pubsub_event(event, context):
    """
    Cloud Function entry point triggered by Pub/Sub.

    Args:
        event (dict): The Pub/Sub message payload.
            - event['data']: Base64-encoded message data.
        context: Metadata about the event.
    """
    logger.info(f"Processing event. Context: {context.resource}")

    # 1. Decode and parse the message
    if "data" not in event:
        logger.error("No 'data' field in Pub/Sub event. Skipping.")
        return

    try:
        raw_data = base64.b64decode(event["data"]).decode("utf-8")
        record = json.loads(raw_data)
        logger.info(f"Parsed record: {json.dumps(record)}")
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.error(f"Failed to decode/parse message data: {e}")
        return  # Don't retry — message is malformed

    # 2. Validate required fields
    required_fields = ["recordId", "userEmail", "value"]
    missing = [f for f in required_fields if f not in record]
    if missing:
        logger.error(f"Missing required fields: {missing}. Skipping.")
        return

    # 3. Add processed timestamp
    record["processedAt"] = datetime.now(timezone.utc).isoformat()
    logger.info(f"Added processedAt: {record['processedAt']}")

    # 4. Write to Cloud SQL
    write_to_cloud_sql(record)

    # 5. Write to LocalStack DynamoDB
    write_to_dynamodb(record)

    logger.info(f"✓ Pipeline complete for record '{record['recordId']}'")
