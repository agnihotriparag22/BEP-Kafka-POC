import json
import logging
import time
from typing import Dict, Any
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition
from app.models.models import DynamoDBEvent, FilterRule
from app.utils.dynamodb_parser import extract_event_from_dynamodb_stream

from app.config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_APPLICATION_ID,
    KAFKA_AUTO_OFFSET_RESET,
    DYNAMODB_EVENTS_TOPIC,
    FILTER_RULES_TOPIC,
    OUTPUT_TOPIC_PREFIX
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EventProcessor:
    def __init__(self):
        self.filter_rules_ktable = {}  
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
    def build_filter_rules_ktable_from_compacted_topic(self):
        """Build KTable from compacted filter rules topic"""
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=f'{KAFKA_APPLICATION_ID}-rules-ktable-builder',
            auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m is not None else None,
            consumer_timeout_ms=5000  # Timeout to avoid infinite loop
        )
        
        partitions = consumer.partitions_for_topic(FILTER_RULES_TOPIC)
        if partitions:
            topic_partitions = [TopicPartition(FILTER_RULES_TOPIC, p) for p in partitions]
            consumer.assign(topic_partitions)
            consumer.seek_to_beginning()
            
            try:
                for message in consumer:
                    self.update_filter_rules_ktable_state(message.key, message.value)
            except Exception as e:
                logger.info(f"Finished reading KTable: {e}")
                
        logger.info(f"Filter rules KTable built with {len(self.filter_rules_ktable)} rules")
        for path, rule in self.filter_rules_ktable.items():
            logger.info(f"  Rule: {path} = {rule.value} -> {rule.target_topic}")
        consumer.close()
    
    def update_filter_rules_ktable_state(self, key: bytes, value: Any):
        """Update KTable state with changelog semantics"""
        if key is None:
            return
            
        rule_key = key.decode('utf-8') if isinstance(key, bytes) else str(key)
        
        if value is None: 
            self.filter_rules_ktable.pop(rule_key, None)
            logger.info(f"Deleted rule from KTable: {rule_key}")
        else:
            if isinstance(value, list):
                for rule_item in value:
                    rule = FilterRule(**rule_item)
                    self.filter_rules_ktable[rule.attribute_path] = rule
            else:
                rule = FilterRule(**value)
                self.filter_rules_ktable[rule.attribute_path] = rule
            logger.info(f"Updated rule in KTable: {rule_key} -> {rule.attribute_path} = {rule.value} -> {rule.target_topic}")
            logger.info(f"KTable now has {len(self.filter_rules_ktable)} rules total")
    
    def process_events_with_ktable_stream_join(self):
        """Process events using KTable stream-table join pattern"""
        events_consumer = KafkaConsumer(
            DYNAMODB_EVENTS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=f'{KAFKA_APPLICATION_ID}-events-stream',
            auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        rules_consumer = KafkaConsumer(
            FILTER_RULES_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=f'{KAFKA_APPLICATION_ID}-rules-ktable-updates',
            auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m is not None else None
        )
        
        self.build_filter_rules_ktable_from_compacted_topic()
        logger.info("Starting KTable stream-table join processing")
        
        while True:

            rule_msgs = rules_consumer.poll(timeout_ms=100, max_records=10)
            for tp, messages in rule_msgs.items():
                for message in messages:
                    self.update_filter_rules_ktable_state(message.key, message.value)
            
            event_msgs = events_consumer.poll(timeout_ms=1000, max_records=100)
            
            for tp, messages in event_msgs.items():
                logger.info(f"Processing {len(messages)} messages from {tp}")
                for message in messages:
                    logger.info(f"Received message: {message.key}")
                    self.join_event_with_filter_rules_ktable(message.value)
    
    def join_event_with_filter_rules_ktable(self, event_data: Dict[str, Any]):
        """Join stream event with KTable state"""
        try:

            parsed_event = extract_event_from_dynamodb_stream(event_data)
            if not parsed_event:
                logger.warning("Could not parse DynamoDB stream record")
                return
                
            event = DynamoDBEvent(**parsed_event)
            logger.info(f"Joining event {event.metadata.event_id} with filter rules KTable")
            logger.info(f"KTable has {len(self.filter_rules_ktable)} rules")
            
            if not self.filter_rules_ktable:
                logger.info("Filter rules KTable empty, skipping event")
                return
                
            matches = 0
            for rule in self.filter_rules_ktable.values():
                logger.info(f"Evaluating rule: {rule.attribute_path} = {rule.value}")
                if rule.evaluate(event):
                    matches += 1
                    self.producer.send(
                        rule.target_topic,
                        value=event_data  
                    )
                    logger.info(f"Event {event.metadata.event_id} sent to {rule.target_topic} via KTable join")
                else:
                    logger.info(f"Rule {rule.attribute_path} did not match")

            self.producer.flush()
            
        except Exception as e:
            logger.error(f"Error in KTable stream-table join: {e}")
    
    def start(self):
        """Start KTable-based event processor"""
        logger.info("Starting KTable-based event processor")
        try:
            self.process_events_with_ktable_stream_join()
        except KeyboardInterrupt:
            logger.info("Shutting down KTable processor...")
            self.producer.close()

if __name__ == "__main__":
    processor = EventProcessor()
    processor.start()