import base64
import json
import logging
import os
import time
from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1

# Import the actual cloud function entry point
from main import process_pubsub_event

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("local_subscriber")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "localstack-493710")
TOPIC_ID = os.environ.get("PUBSUB_TOPIC", "localstack-events")
SUBSCRIPTION_ID = f"{TOPIC_ID}-sub"

def setup_pubsub():
    """Create the topic and subscription in the emulator if they don't exist."""
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    # 1. Create Topic
    try:
        publisher.create_topic(request={"name": topic_path})
        logger.info(f"Created topic: {topic_path}")
    except AlreadyExists:
        logger.info(f"Topic {topic_path} already exists.")
    except Exception as e:
        logger.warning(f"Failed to create topic (might already exist): {e}")

    # 2. Create Subscription
    try:
        with subscriber:
            subscriber.create_subscription(
                request={"name": subscription_path, "topic": topic_path}
            )
        logger.info(f"Created subscription: {subscription_path}")
    except AlreadyExists:
        logger.info(f"Subscription {subscription_path} already exists.")
    except Exception as e:
        logger.warning(f"Failed to create subscription: {e}")

    return subscription_path

def callback(message: pubsub_v1.subscriber.message.Message):
    """Handle incoming Pub/Sub messages and pass them to the cloud function."""
    logger.info(f"Received message ID: {message.message_id}")
    
    # Cloud functions expect 'event' dict with 'data' base64 encoded
    # The message.data is bytes, we need to base64 encode it for the payload
    event = {
        "data": base64.b64encode(message.data).decode("utf-8")
    }
    
    # Mocking context
    class Context:
        event_id = message.message_id
        resource = "projects/.../topics/localstack-events"
        
    try:
        # Call the actual processor function
        process_pubsub_event(event, Context())
        # If successfully processed without exceptions, ack the message
        message.ack()
        logger.info(f"Successfully processed and ACKed message {message.message_id}")
    except Exception as e:
        logger.error(f"Failed to process message {message.message_id}: {e}")
        # Nack to trigger retry
        message.nack()

def main():
    logger.info("Starting local Pub/Sub subscriber...")
    # Wait for the emulator to be ready
    time.sleep(5) 
    
    subscription_path = setup_pubsub()
    subscriber = pubsub_v1.SubscriberClient()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    logger.info(f"Listening for messages on {subscription_path}...\n")

    with subscriber:
        try:
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()
            streaming_pull_future.result()

if __name__ == "__main__":
    main()
