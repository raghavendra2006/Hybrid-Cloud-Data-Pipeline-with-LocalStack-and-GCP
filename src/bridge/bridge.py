"""
Bridge Application — SQS → Pub/Sub Forwarder
=============================================
Continuously polls LocalStack SQS for messages triggered by S3 uploads,
then publishes the file content to a GCP Pub/Sub topic.

Features:
  - Long polling with configurable wait time
  - Exponential backoff retry for GCP Pub/Sub publishing
  - Graceful shutdown on SIGTERM/SIGINT
  - Structured logging
  - Dead-letter queue support (via SQS redrive policy)
"""

import json
import logging
import os
import signal
import sys
import time
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

    sqs_client = boto3.client(
        "sqs",
        endpoint_url=LOCALSTACK_ENDPOINT,
        region_name=AWS_REGION,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        config=boto_config,
    )

    s3_client = boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_ENDPOINT,
        region_name=AWS_REGION,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
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
            logger.warning(f"Failed to get queue URL (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
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
    """
    try:
        # S3 event notifications wrap records in a list
        records = sqs_body.get("Records", [])
        if not records:
            logger.warning("No Records found in SQS message body. Treating body as direct payload.")
            return json.dumps(sqs_body)

        record = records[0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        logger.info(f"Downloading s3://{bucket}/{key}")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        logger.info(f"Downloaded {len(content)} bytes from s3://{bucket}/{key}")
        return content

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
            logger.warning(f"Pub/Sub publish failed (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
            time.sleep(wait)
        except Exception as e:
            logger.error(f"Unexpected error publishing to Pub/Sub: {e}")
            return False

    logger.error("Exhausted retries publishing to Pub/Sub.")
    return False


def delete_sqs_message(sqs_client, queue_url: str, receipt_handle: str):
    """Delete a processed message from SQS."""
    try:
        sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        logger.info("Deleted message from SQS queue.")
    except ClientError as e:
        logger.error(f"Failed to delete SQS message: {e}")


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
        time.sleep(POLL_INTERVAL)
        return

    messages = response.get("Messages", [])
    if not messages:
        logger.debug("No messages received from SQS.")
        return

    logger.info(f"Received {len(messages)} message(s) from SQS.")

    for message in messages:
        receipt_handle = message["ReceiptHandle"]
        try:
            body = json.loads(message["Body"])
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in SQS message body: {message['Body'][:200]}")
            # Delete poison pill messages to prevent infinite reprocessing
            delete_sqs_message(sqs_client, queue_url, receipt_handle)
            continue

        # Extract file content from S3
        file_content = extract_s3_file_content(body, s3_client)
        if file_content is None:
            logger.error("Failed to extract S3 file content. Skipping message (will be retried by SQS).")
            continue

        # Publish to Pub/Sub
        success = publish_to_pubsub(publisher, topic_path, file_content)
        if success:
            delete_sqs_message(sqs_client, queue_url, receipt_handle)
        else:
            logger.warning("Pub/Sub publish failed. Message will be retried by SQS visibility timeout.")


def wait_for_localstack(sqs_client):
    """Wait until LocalStack is reachable."""
    logger.info("Waiting for LocalStack to become available...")
    for attempt in range(30):
        try:
            sqs_client.list_queues()
            logger.info("LocalStack is available.")
            return
        except (EndpointConnectionError, ClientError) as e:
            wait = min(BASE_BACKOFF * (2 ** min(attempt, 5)), 30)
            logger.debug(f"LocalStack not ready (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
            time.sleep(wait)

    logger.error("LocalStack did not become available. Exiting.")
    sys.exit(1)


def main():
    """Main entry point for the bridge application."""
    logger.info("=" * 60)
    logger.info("Bridge Application Starting")
    logger.info(f"  LocalStack Endpoint: {LOCALSTACK_ENDPOINT}")
    logger.info(f"  SQS Queue: {SQS_QUEUE_NAME}")
    logger.info(f"  GCP Project: {GCP_PROJECT_ID}")
    logger.info(f"  Pub/Sub Topic: {PUBSUB_TOPIC}")
    logger.info(f"  Poll Interval: {POLL_INTERVAL}s")
    logger.info(f"  SQS Wait Time: {SQS_WAIT_TIME}s")
    logger.info("=" * 60)

    if not GCP_PROJECT_ID:
        logger.error("GCP_PROJECT_ID is not set. Exiting.")
        sys.exit(1)

    # Initialize clients
    sqs_client, s3_client = create_aws_clients()
    wait_for_localstack(sqs_client)
    queue_url = get_queue_url(sqs_client)

    publisher = create_pubsub_publisher()
    topic_path = get_topic_path(publisher)
    logger.info(f"Pub/Sub topic path: {topic_path}")

    # Main polling loop
    logger.info("Starting SQS polling loop...")
    while not shutdown_requested:
        poll_and_forward(sqs_client, s3_client, publisher, queue_url, topic_path)

    logger.info("Bridge application shut down gracefully.")


if __name__ == "__main__":
    main()
