"""
Bridge Application — SQS → Pub/Sub Forwarder
=============================================
Continuously polls LocalStack SQS for messages triggered by S3 uploads,
then publishes the file content to a GCP Pub/Sub topic.

Features:
  - Long polling with configurable wait time
  - Exponential backoff retry for GCP Pub/Sub publishing
  - Graceful shutdown on SIGTERM/SIGINT
  - Structured JSON logging
  - Dead-letter queue support (via SQS redrive policy)
  - Message counter / health metrics
  - URL-decoded S3 key handling
"""

import json
import logging
import os
import signal
import sys
import time
import urllib.parse
from datetime import datetime, timezone
from typing import Optional

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, EndpointConnectionError
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import pubsub_v1

# ============================================
# Configuration
# ============================================
LOCALSTACK_ENDPOINT = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
SQS_QUEUE_NAME = os.environ.get("SQS_QUEUE_NAME", "data-processing-queue")
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC", "localstack-events")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "5"))
SQS_WAIT_TIME = int(os.environ.get("SQS_WAIT_TIME_SECONDS", "20"))
MAX_RETRIES = 5
BASE_BACKOFF = 1  # seconds
HEALTH_LOG_INTERVAL = 60  # seconds between health stats

# ============================================
# Logging
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
    stream=sys.stdout,
)
logger = logging.getLogger("bridge")

# ============================================
# Metrics
# ============================================
metrics = {
    "messages_received": 0,
    "messages_forwarded": 0,
    "messages_failed": 0,
    "errors": 0,
    "start_time": None,
    "last_message_at": None,
}

# ============================================
# Graceful Shutdown
# ============================================
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global shutdown_requested
    sig_name = signal.Signals(signum).name
    logger.info(f"Received {sig_name}. Initiating graceful shutdown...")
    shutdown_requested = True


signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


def log_health_stats():
    """Log periodic health/metrics information."""
    uptime = "N/A"
    if metrics["start_time"]:
        delta = datetime.now(timezone.utc) - metrics["start_time"]
        hours, remainder = divmod(int(delta.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        uptime = f"{hours}h {minutes}m {seconds}s"

    logger.info(
        f"[HEALTH] uptime={uptime} "
        f"received={metrics['messages_received']} "
        f"forwarded={metrics['messages_forwarded']} "
        f"failed={metrics['messages_failed']} "
        f"errors={metrics['errors']} "
        f"last_msg={metrics['last_message_at'] or 'never'}"
    )


# ============================================
# AWS / LocalStack Clients
# ============================================
def create_aws_clients():
    """Create boto3 clients for SQS and S3 pointed at LocalStack."""
    boto_config = BotoConfig(
        retries={"max_attempts": 3, "mode": "standard"},
        connect_timeout=10,
        read_timeout=30,
    )

    session = boto3.Session(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=AWS_REGION,
    )

    sqs_client = session.client(
        "sqs",
        endpoint_url=LOCALSTACK_ENDPOINT,
        config=boto_config,
    )

    s3_client = session.client(
        "s3",
        endpoint_url=LOCALSTACK_ENDPOINT,
        config=boto_config,
    )

    return sqs_client, s3_client


def get_queue_url(sqs_client) -> str:
    """Retrieve the SQS queue URL by name, with retries."""
    for attempt in range(MAX_RETRIES):
        try:
            response = sqs_client.get_queue_url(QueueName=SQS_QUEUE_NAME)
            url = response["QueueUrl"]
            logger.info(f"Resolved SQS queue URL: {url}")
            return url
        except (ClientError, EndpointConnectionError) as e:
            wait = BASE_BACKOFF * (2 ** attempt)
            logger.warning(
                f"Failed to get queue URL (attempt {attempt + 1}/{MAX_RETRIES}): "
                f"{e}. Retrying in {wait}s..."
            )
            time.sleep(wait)

    logger.error("Exhausted retries getting SQS queue URL. Exiting.")
    sys.exit(1)


# ============================================
# GCP Pub/Sub Client
# ============================================
def create_pubsub_publisher() -> pubsub_v1.PublisherClient:
    """Create a GCP Pub/Sub publisher client."""
    return pubsub_v1.PublisherClient()


def get_topic_path(publisher: pubsub_v1.PublisherClient) -> str:
    """Build the full Pub/Sub topic path."""
    return publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC)


# ============================================
# Message Processing
# ============================================
def extract_s3_file_content(sqs_body: dict, s3_client) -> Optional[str]:
    """
    Parse the SQS message body (S3 event notification) and download
    the uploaded file content from S3.

    Handles both S3 event notification format and direct JSON payloads.
    URL-decodes the S3 key to handle special characters.
    """
    try:
        # S3 event notifications wrap records in a list
        records = sqs_body.get("Records", [])
        if not records:
            # Fallback: treat the body itself as the payload (direct message)
            logger.warning(
                "No 'Records' found in SQS message body. "
                "Treating body as direct payload."
            )
            return json.dumps(sqs_body)

        record = records[0]
        bucket = record["s3"]["bucket"]["name"]
        # S3 event keys are URL-encoded
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        logger.info(f"Downloading s3://{bucket}/{key}")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        logger.info(f"Downloaded {len(content)} bytes from s3://{bucket}/{key}")

        # Validate it's valid JSON before forwarding
        json.loads(content)

        return content

    except json.JSONDecodeError as e:
        logger.error(f"Downloaded S3 content is not valid JSON: {e}")
        return None
    except (KeyError, IndexError) as e:
        logger.error(f"Malformed S3 event notification: {e}")
        return None
    except ClientError as e:
        logger.error(f"Failed to download S3 object: {e}")
        return None


def publish_to_pubsub(publisher, topic_path: str, data: str) -> bool:
    """
    Publish a message to GCP Pub/Sub with exponential backoff retries.
    Returns True on success, False on failure.
    """
    encoded_data = data.encode("utf-8")

    for attempt in range(MAX_RETRIES):
        try:
            future = publisher.publish(
                topic_path,
                encoded_data,
                source="localstack-bridge",
                content_type="application/json",
            )
            message_id = future.result(timeout=30)
            logger.info(f"Published to Pub/Sub. Message ID: {message_id}")
            return True

        except GoogleAPICallError as e:
            wait = BASE_BACKOFF * (2 ** attempt)
            logger.warning(
                f"Pub/Sub publish failed (attempt {attempt + 1}/{MAX_RETRIES}): "
                f"{e}. Retrying in {wait}s..."
            )
            time.sleep(wait)
        except Exception as e:
            logger.error(f"Unexpected error publishing to Pub/Sub: {e}", exc_info=True)
            return False

    logger.error("Exhausted retries publishing to Pub/Sub.")
    return False


def delete_sqs_message(sqs_client, queue_url: str, receipt_handle: str):
    """Delete a processed message from SQS to acknowledge processing."""
    try:
        sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        logger.info("Deleted message from SQS queue (acknowledged).")
    except ClientError as e:
        logger.error(f"Failed to delete SQS message: {e}")
        metrics["errors"] += 1


# ============================================
# Main Polling Loop
# ============================================
def poll_and_forward(sqs_client, s3_client, publisher, queue_url: str, topic_path: str):
    """
    Poll SQS for messages, download the S3 file content,
    publish to Pub/Sub, and delete the SQS message.
    """
    try:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=SQS_WAIT_TIME,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
        )
    except (ClientError, EndpointConnectionError) as e:
        logger.error(f"Error polling SQS: {e}")
        metrics["errors"] += 1
        time.sleep(POLL_INTERVAL)
        return

    messages = response.get("Messages", [])
    if not messages:
        return  # Normal long-poll timeout with no messages

    logger.info(f"Received {len(messages)} message(s) from SQS.")

    for message in messages:
        receipt_handle = message["ReceiptHandle"]
        message_id = message.get("MessageId", "unknown")

        try:
            body = json.loads(message["Body"])
        except json.JSONDecodeError:
            logger.error(
                f"Invalid JSON in SQS message {message_id}. "
                f"Deleting poison pill: {message['Body'][:200]}"
            )
            delete_sqs_message(sqs_client, queue_url, receipt_handle)
            metrics["messages_failed"] += 1
            continue

        metrics["messages_received"] += 1
        metrics["last_message_at"] = datetime.now(timezone.utc).isoformat()

        # Extract file content from S3
        file_content = extract_s3_file_content(body, s3_client)
        if file_content is None:
            logger.error(
                f"Failed to extract S3 file content for message {message_id}. "
                "Leaving in queue for retry via visibility timeout."
            )
            metrics["messages_failed"] += 1
            continue

        # Publish to Pub/Sub
        success = publish_to_pubsub(publisher, topic_path, file_content)
        if success:
            delete_sqs_message(sqs_client, queue_url, receipt_handle)
            metrics["messages_forwarded"] += 1
        else:
            logger.warning(
                f"Pub/Sub publish failed for message {message_id}. "
                "Will be retried after SQS visibility timeout."
            )
            metrics["messages_failed"] += 1


def wait_for_localstack(sqs_client):
    """Wait until LocalStack is reachable with exponential backoff."""
    logger.info("Waiting for LocalStack to become available...")
    for attempt in range(30):
        try:
            sqs_client.list_queues()
            logger.info("LocalStack is available and responding.")
            return
        except (EndpointConnectionError, ClientError, Exception) as e:
            wait = min(BASE_BACKOFF * (2 ** min(attempt, 5)), 30)
            logger.info(
                f"LocalStack not ready (attempt {attempt + 1}/30): "
                f"{type(e).__name__}. Retrying in {wait}s..."
            )
            time.sleep(wait)

    logger.error("LocalStack did not become available after 30 attempts. Exiting.")
    sys.exit(1)


def main():
    """Main entry point for the bridge application."""
    logger.info("=" * 60)
    logger.info("  Bridge Application — SQS → Pub/Sub Forwarder")
    logger.info("=" * 60)
    logger.info(f"  LocalStack Endpoint : {LOCALSTACK_ENDPOINT}")
    logger.info(f"  SQS Queue           : {SQS_QUEUE_NAME}")
    logger.info(f"  GCP Project         : {GCP_PROJECT_ID}")
    logger.info(f"  Pub/Sub Topic       : {PUBSUB_TOPIC}")
    logger.info(f"  Poll Interval       : {POLL_INTERVAL}s")
    logger.info(f"  SQS Wait Time       : {SQS_WAIT_TIME}s")
    logger.info(f"  Max Retries         : {MAX_RETRIES}")
    logger.info("=" * 60)

    if not GCP_PROJECT_ID:
        logger.error("FATAL: GCP_PROJECT_ID environment variable is not set. Exiting.")
        sys.exit(1)

    # Initialize clients
    sqs_client, s3_client = create_aws_clients()
    wait_for_localstack(sqs_client)
    queue_url = get_queue_url(sqs_client)

    publisher = create_pubsub_publisher()
    topic_path = get_topic_path(publisher)
    logger.info(f"Pub/Sub topic path: {topic_path}")

    # Track start time for health metrics
    metrics["start_time"] = datetime.now(timezone.utc)
    last_health_log = time.monotonic()

    # Main polling loop
    logger.info("Starting SQS polling loop...")
    while not shutdown_requested:
        poll_and_forward(sqs_client, s3_client, publisher, queue_url, topic_path)

        # Periodic health stats
        now = time.monotonic()
        if now - last_health_log >= HEALTH_LOG_INTERVAL:
            log_health_stats()
            last_health_log = now

    # Final stats on shutdown
    log_health_stats()
    logger.info("Bridge application shut down gracefully.")


if __name__ == "__main__":
    main()
