#!/usr/bin/env python3
"""
Generalized Kafka Consumer Script
Replace KAFKA_BROKER_IP and TOPIC_NAME with your actual values
"""

from kafka import KafkaConsumer
import json
import logging
from typing import Optional

# Configuration - UPDATE THESE VALUES
KAFKA_BROKER_IP = "3.20.237.63:9092"  
TOPIC_NAME = "DLQ-Topic"    

# Optional configuration
CONSUMER_GROUP = "my-consumer-group"  # Consumer group ID
AUTO_OFFSET_RESET = "latest"       # 'earliest' or 'latest'
ENABLE_AUTO_COMMIT = True            # Auto commit offsets
AUTO_COMMIT_INTERVAL_MS = 1000       # Auto commit interval

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_consumer() -> Optional[KafkaConsumer]:
    """
    Create and configure Kafka consumer
    """
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[KAFKA_BROKER_IP],
            group_id=CONSUMER_GROUP,
            auto_offset_reset=AUTO_OFFSET_RESET,
            enable_auto_commit=ENABLE_AUTO_COMMIT,
            auto_commit_interval_ms=AUTO_COMMIT_INTERVAL_MS,
            value_deserializer=lambda x: x.decode('utf-8') if x else None,
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
            consumer_timeout_ms=1000  # Timeout for polling
        )
        logger.info(f"Consumer created successfully for topic: {TOPIC_NAME}")
        return consumer
    except Exception as e:
        logger.error(f"Failed to create consumer: {e}")
        return None

def process_message(message) -> None:
    """
    Process individual message - customize this function based on your needs
    """
    try:
        # Basic message info
        logger.info(f"Received message:")
        logger.info(f"  Topic: {message.topic}")
        logger.info(f"  Partition: {message.partition}")
        logger.info(f"  Offset: {message.offset}")
        logger.info(f"  Key: {message.key}")
        logger.info(f"  Value: {message.value}")
        logger.info(f"  Timestamp: {message.timestamp}")
        
        # Try to parse as JSON if possible
        try:
            if message.value:
                json_data = json.loads(message.value)
                logger.info(f"  Parsed JSON: {json_data}")
        except json.JSONDecodeError:
            logger.info(f"  Raw message (not JSON): {message.value}")
        
        # Add your custom message processing logic here
        # For example:
        # - Store in database
        # - Send to another service
        # - Transform and forward to another topic
        # - Trigger business logic
        
        logger.info("-" * 50)
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def consume_messages():
    """
    Main consumer loop
    """
    consumer = create_consumer()
    if not consumer:
        logger.error("Failed to create consumer. Exiting.")
        return
    
    logger.info(f"Starting to consume messages from topic: {TOPIC_NAME}")
    logger.info(f"Broker: {KAFKA_BROKER_IP}")
    logger.info(f"Consumer Group: {CONSUMER_GROUP}")
    logger.info("Press Ctrl+C to stop the consumer")
    
    try:
        # Main consumption loop
        for message in consumer:
            process_message(message)
            
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        # Clean up
        logger.info("Closing consumer...")
        consumer.close()
        logger.info("Consumer closed successfully")

def main():
    """
    Entry point
    """
    logger.info("=" * 60)
    logger.info("KAFKA CONSUMER STARTING")
    logger.info("=" * 60)
    
    # Validate configuration
    if KAFKA_BROKER_IP == "localhost:9092":
        logger.warning("Using default broker IP. Update KAFKA_BROKER_IP variable.")
    if TOPIC_NAME == "your-topic-name":
        logger.warning("Using default topic name. Update TOPIC_NAME variable.")
    
    consume_messages()

if __name__ == "__main__":
    main()