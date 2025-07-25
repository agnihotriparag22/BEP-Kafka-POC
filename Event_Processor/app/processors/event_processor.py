import json
import logging
import threading
from typing import Dict, Any
from confluent_kafka import Consumer, Producer, KafkaError

from app.config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_APPLICATION_ID,
    KAFKA_AUTO_OFFSET_RESET,
    DYNAMODB_EVENTS_TOPIC,
    FILTER_RULES_TOPIC,
    OUTPUT_TOPIC_PREFIX
)
from app.models.models import DynamoDBEvent, FilterRule

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EventProcessor:
    def __init__(self):
        self.filter_rules = {}  # In-memory KTable equivalent
        self.producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        
    def process_filter_rules(self):
        """Consumer for filter rules"""
        logger.info("Starting to process filter rules")
        consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': f'{KAFKA_APPLICATION_ID}-rules',
            'auto.offset.reset': KAFKA_AUTO_OFFSET_RESET
        })
        consumer.subscribe([FILTER_RULES_TOPIC])
        logger.info("Subscribed to filter rules topic")
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer error: {msg.error()}")
                continue
                
            try:
                rule_data = json.loads(msg.value().decode('utf-8'))
                # Handle both individual rules and arrays of rules
                if isinstance(rule_data, list):
                    for rule_item in rule_data:
                        rule = FilterRule(**rule_item)
                        self.filter_rules[rule.attribute_path] = rule
                        logger.info(f"Updated rule: {rule.attribute_path} -> {rule.target_topic}")
                else:
                    rule = FilterRule(**rule_data)
                    self.filter_rules[rule.attribute_path] = rule
                    logger.info(f"Updated rule: {rule.attribute_path} -> {rule.target_topic}")
            except Exception as e:
                logger.error(f"Error processing rule: {e}")
    
    def process_events(self):
        """Consumer for DynamoDB events"""
        logger.info("Starting to process events")
        consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': f'{KAFKA_APPLICATION_ID}-events',
            'auto.offset.reset': KAFKA_AUTO_OFFSET_RESET
        })
        consumer.subscribe([DYNAMODB_EVENTS_TOPIC])
        logger.info("Subscribed to DynamoDB events topic")
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer error: {msg.error()}")
                continue
                
            try:
                logger.info("Processing event message")
                event_data = json.loads(msg.value().decode('utf-8'))
                event = DynamoDBEvent(**event_data)
                logger.info(f"Received event: {event.metadata.event_id}")
                # Apply filter rules
                if not self.filter_rules:
                    logger.info("No filter rules available, skipping event processing")
                    continue
                for rule in self.filter_rules.values():
                    logger.info(f"Evaluating rule: {rule.attribute_path} with value: {rule.value}")
                    if rule.evaluate(event):
                        target_topic = rule.target_topic
                        self.producer.produce(
                            target_topic,
                            json.dumps(event.model_dump()).encode('utf-8')
                        )
                        logger.info(f"Event {event.metadata.event_id} sent to {target_topic}")
                        
                self.producer.flush()
            except Exception as e:
                logger.error(f"Error processing event: {e}")
    
    def start(self):
        """Start both consumers in separate threads"""
        rules_thread = threading.Thread(target=self.process_filter_rules)
        events_thread = threading.Thread(target=self.process_events)
        
        rules_thread.daemon = True
        events_thread.daemon = True
        
        rules_thread.start()
        events_thread.start()
        
        # Keep main thread alive
        try:
            while True:
                rules_thread.join(1)
                events_thread.join(1)
        except KeyboardInterrupt:
            logger.info("Shutting down...")

if __name__ == "__main__":
    processor = EventProcessor()
    processor.start()