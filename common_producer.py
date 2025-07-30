#!/usr/bin/env python3
"""
Generalized Kafka Producer Script
Replace KAFKA_BROKER_IP and TOPIC_NAME with your actual values
"""

from kafka import KafkaProducer
import json
import logging
import sys
from typing import Optional

# Configuration - UPDATE THESE VALUES
KAFKA_BROKER_IP = "18.188.188.10:9092"  # Replace with your Kafka broker IP:port
TOPIC_NAME = "raw-event"                # Replace with your topic name

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_producer() -> Optional[KafkaProducer]:
    """
    Create and configure Kafka producer
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER_IP],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for leader and replicas to acknowledge
            retries=5
        )
        logger.info("Producer created successfully")
        return producer
    except Exception as e:
        logger.error(f"Failed to create producer: {e}")
        return None

def send_message(producer: KafkaProducer, value, key: str = None) -> None:
    """
    Send a message to Kafka
    """
    try:
        future = producer.send(TOPIC_NAME, value=value, key=key)
        result = future.get(timeout=10)
        logger.info(f"Message sent to {result.topic} partition {result.partition} offset {result.offset}")
    except Exception as e:
        logger.error(f"Failed to send message: {e}")

def main():
    """
    Entry point for the producer
    """
    logger.info("=" * 60)
    logger.info("KAFKA PRODUCER STARTING")
    logger.info("=" * 60)

    # Validate configuration
    if KAFKA_BROKER_IP == "localhost:9092":
        logger.warning("Using default broker IP. Update KAFKA_BROKER_IP variable.")
    if TOPIC_NAME == "your-topic-name":
        logger.warning("Using default topic name. Update TOPIC_NAME variable.")

    producer = create_producer()
    if not producer:
        logger.error("Failed to create producer. Exiting.")
        sys.exit(1)

    logger.info(f"Ready to send messages to topic: {TOPIC_NAME}")
    logger.info("Type your message and press Enter. Type 'exit' to quit.")

    try:
        while True:
            user_input = input("Enter message (or 'exit' to quit): ")
            if user_input.strip().lower() == 'exit':
                break
            # You can customize the key and value as needed
            send_message(producer, value={"message": user_input})
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user")
    finally:
        logger.info("Flushing and closing producer...")
        producer.flush()
        producer.close()
        logger.info("Producer closed successfully")

if __name__ == "__main__":
    main()
