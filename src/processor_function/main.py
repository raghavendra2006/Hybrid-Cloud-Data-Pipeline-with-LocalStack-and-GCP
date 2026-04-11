"""
GCP Cloud Function — Pipeline Processor
========================================
Triggered by messages on the 'localstack-events' Pub/Sub topic.

Processing flow:
  1. Decode and parse the Pub/Sub message (JSON)
  2. Validate all required fields and types
  3. Add a processed_at timestamp (ISO 8601 UTC)
  4. Write the record to GCP Cloud SQL (PostgreSQL) — idempotent upsert
  5. Write the record back to LocalStack DynamoDB — idempotent put_item

Design:
  - Idempotent: Uses INSERT ... ON CONFLICT DO UPDATE for Cloud SQL
  - Idempotent: Uses put_item for DynamoDB (overwrites existing)
  - Module-level connection pooling for Cloud SQL
  - Configurable endpoints via environment variables
  - Structured error logging with field validation
"""

import base64
import json
import logging
import os
from datetime import datetime, timezone

import boto3
import pg8000.native
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
# Module-level connection cache
# ============================================
# Cloud Functions reuse execution contexts. By keeping the connection
# at module level, subsequent invocations skip connection setup latency.
_db_connection = None
_table_created = False


# ============================================
# Database: Cloud SQL (PostgreSQL)
# ============================================
def _get_cloud_sql_connection():
    """
    Get or create a Cloud SQL connection.
    Reuses module-level connection for warm invocations.
    Uses pg8000.native (dbapi-less) for reliability.
    """
    global _db_connection

    if _db_connection is not None:
        try:
            # Test if the connection is still alive
            _db_connection.run("SELECT 1")
            return _db_connection
        except Exception:
            logger.warning("Cached DB connection is stale. Reconnecting...")
            _db_connection = None

    # Detect GCP Cloud Functions environment
    is_gcp = bool(os.environ.get("FUNCTION_TARGET", "")) or bool(
        os.environ.get("K_SERVICE", "")
    )

    if is_gcp and CLOUD_SQL_CONNECTION_NAME:
        # Connect via Unix socket (Cloud SQL Proxy built into Cloud Functions)
        unix_socket_path = f"/cloudsql/{CLOUD_SQL_CONNECTION_NAME}"
        logger.info(f"Connecting to Cloud SQL via Unix socket: {unix_socket_path}")
        _db_connection = pg8000.native.Connection(
            user=CLOUD_SQL_USER,
            password=CLOUD_SQL_PASSWORD,
            database=CLOUD_SQL_DATABASE,
            unix_sock=f"{unix_socket_path}/.s.PGSQL.5432",
        )
    else:
        # Connect via TCP (local development or testing)
        host = os.environ.get("CLOUD_SQL_HOST", "127.0.0.1")
        port = int(os.environ.get("CLOUD_SQL_PORT", "5432"))
        logger.info(f"Connecting to Cloud SQL via TCP: {host}:{port}")
        _db_connection = pg8000.native.Connection(
            user=CLOUD_SQL_USER,
            password=CLOUD_SQL_PASSWORD,
            database=CLOUD_SQL_DATABASE,
            host=host,
            port=port,
        )

    # Enable autocommit
    _db_connection.autocommit = True
    logger.info("Cloud SQL connection established.")
    return _db_connection


def _ensure_table_exists(conn):
    """Create the records table if it doesn't exist (only on first invocation)."""
    global _table_created
    if _table_created:
        return

    conn.run("""
        CREATE TABLE IF NOT EXISTS records (
            id VARCHAR(255) PRIMARY KEY NOT NULL,
            user_email VARCHAR(255) NOT NULL,
            value INTEGER NOT NULL,
            processed_at TIMESTAMP NOT NULL
        )
    """)
    _table_created = True
    logger.info("Ensured 'records' table exists in Cloud SQL.")


def write_to_cloud_sql(record: dict):
    """
    Write a processed record to Cloud SQL.
    Uses INSERT ... ON CONFLICT for idempotency.
    """
    try:
        conn = _get_cloud_sql_connection()
        _ensure_table_exists(conn)

        conn.run(
            """
            INSERT INTO records (id, user_email, value, processed_at)
            VALUES (:id, :email, :val, :ts)
            ON CONFLICT (id) DO UPDATE SET
                user_email = EXCLUDED.user_email,
                value = EXCLUDED.value,
                processed_at = EXCLUDED.processed_at
            """,
            id=record["recordId"],
            email=record["userEmail"],
            val=int(record["value"]),
            ts=record["processedAt"],
        )
        logger.info(f"Cloud SQL: Upserted record '{record['recordId']}'")

    except Exception as e:
        logger.error(f"Cloud SQL write failed: {e}", exc_info=True)
        # Reset cached connection so next invocation reconnects
        global _db_connection, _table_created
        _db_connection = None
        _table_created = False
        raise


# ============================================
# Database: LocalStack DynamoDB
# ============================================
_dynamodb_client = None


def _get_dynamodb_client():
    """Get or create a boto3 DynamoDB client (module-level cache)."""
    global _dynamodb_client
    if _dynamodb_client is not None:
        return _dynamodb_client

    boto_config = BotoConfig(
        retries={"max_attempts": 3, "mode": "standard"},
        connect_timeout=10,
        read_timeout=30,
    )

    _dynamodb_client = boto3.client(
        "dynamodb",
        endpoint_url=DYNAMODB_ENDPOINT,
        region_name=AWS_REGION,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        config=boto_config,
    )
    return _dynamodb_client


def write_to_dynamodb(record: dict):
    """
    Write a processed record to LocalStack DynamoDB.
    Uses put_item which is inherently idempotent (overwrites).
    """
    try:
        client = _get_dynamodb_client()
        client.put_item(
            TableName=DYNAMODB_TABLE_NAME,
            Item={
                "recordId": {"S": str(record["recordId"])},
                "userEmail": {"S": str(record["userEmail"])},
                "value": {"N": str(int(record["value"]))},
                "processedAt": {"S": str(record["processedAt"])},
            },
        )
        logger.info(f"DynamoDB: Wrote record '{record['recordId']}'")

    except Exception as e:
        logger.error(f"DynamoDB write failed: {e}", exc_info=True)
        # Reset cached client so next invocation creates fresh one
        global _dynamodb_client
        _dynamodb_client = None
        raise


# ============================================
# Validation
# ============================================
def validate_record(record: dict) -> list:
    """
    Validate required fields and types.
    Returns a list of error strings (empty = valid).
    """
    errors = []

    # Required field checks
    required_fields = ["recordId", "userEmail", "value"]
    for field in required_fields:
        if field not in record:
            errors.append(f"Missing required field: '{field}'")

    if errors:
        return errors

    # Type checks
    if not isinstance(record["recordId"], str) or not record["recordId"].strip():
        errors.append(f"'recordId' must be a non-empty string, got: {type(record['recordId']).__name__}")

    if not isinstance(record["userEmail"], str) or not record["userEmail"].strip():
        errors.append(f"'userEmail' must be a non-empty string, got: {type(record['userEmail']).__name__}")

    try:
        int(record["value"])
    except (TypeError, ValueError):
        errors.append(f"'value' must be numeric, got: '{record['value']}'")

    return errors


# ============================================
# Cloud Function Entry Point
# ============================================
def process_pubsub_event(event, context):
    """
    Cloud Function entry point triggered by Pub/Sub.

    Args:
        event (dict): The Pub/Sub message payload.
            - event['data']: Base64-encoded message data.
        context: Metadata about the event (resource, timestamp, event_id).

    Returns:
        None. Errors are raised to trigger Pub/Sub retry via failure_policy.
    """
    event_id = getattr(context, "event_id", "unknown")
    resource = getattr(context, "resource", "unknown")
    logger.info(f"Processing event {event_id} from {resource}")

    # 1. Decode and parse the message
    if "data" not in event:
        logger.error(f"Event {event_id}: No 'data' field in Pub/Sub event. Skipping.")
        return  # Don't retry — malformed event

    try:
        raw_data = base64.b64decode(event["data"]).decode("utf-8")
        record = json.loads(raw_data)
        logger.info(f"Event {event_id}: Parsed record: {json.dumps(record)}")
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as e:
        logger.error(f"Event {event_id}: Failed to decode/parse message data: {e}")
        return  # Don't retry — message is malformed

    # 2. Validate required fields and types
    validation_errors = validate_record(record)
    if validation_errors:
        logger.error(
            f"Event {event_id}: Validation failed: {'; '.join(validation_errors)}. "
            "Skipping (will not retry)."
        )
        return

    # 3. Add processed timestamp (ISO 8601 UTC)
    record["processedAt"] = datetime.now(timezone.utc).isoformat()
    logger.info(f"Event {event_id}: Added processedAt: {record['processedAt']}")

    # 4. Write to Cloud SQL (will raise on failure → triggers Pub/Sub retry)
    write_to_cloud_sql(record)

    # 5. Write to LocalStack DynamoDB (will raise on failure → triggers Pub/Sub retry)
    write_to_dynamodb(record)

    logger.info(
        f"Event {event_id}: Pipeline complete for record '{record['recordId']}'"
    )
