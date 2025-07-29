import json
import boto3
import logging
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')


KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'event-rules')


def publish_to_kafka_with_key(producer, topic, key, data):
    try:
        producer.send(topic, key=key.encode('utf-8'), value=data)
        producer.flush()
        logger.info(f"Successfully published message to Kafka topic {topic} with key {key}")
    except KafkaError as e:
        logger.error(f"Failed to publish to Kafka: {str(e)}")
        raise

def lambda_handler(event, context):
    try:
        if 'Records' not in event or not event['Records']:
            logger.error("No records found in the event")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'No records found in the event'})
            }

        producer = None
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except KafkaError as e:
            logger.error(f"Failed to initialize Kafka producer: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Failed to initialize Kafka producer'})
            }

        # Process each record
        for record in event['Records']:
            if 's3' not in record:
                logger.error("Invalid S3 event structure")
                continue
                
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']

            # Verify the file is a JSON file
            if not key.endswith('.json'):
                logger.warning(f"Skipping non-JSON file: s3://{bucket}/{key}")
                continue

            try:
                response = s3_client.get_object(Bucket=bucket, Key=key)
                data = response['Body'].read().decode('utf-8')
                
                try:
                    json_data = json.loads(data)
                    # Handle JSON arrays by publishing each item with keys for KTable
                    if isinstance(json_data, list):
                        for i, item in enumerate(json_data):
                            rule_key = f"rule-{i}"
                            publish_to_kafka_with_key(producer, KAFKA_TOPIC, rule_key, item)
                    else:
                        # Single rule with default key
                        publish_to_kafka_with_key(producer, KAFKA_TOPIC, "rule-0", json_data)
                    logger.info(f"Successfully processed S3 object: s3://{bucket}/{key}")
                except json.JSONDecodeError:
                    logger.error(f"S3 object content is not valid JSON: s3://{bucket}/{key}")
                    continue

            except Exception as e:
                logger.error(f"Failed to process S3 object s3://{bucket}/{key}: {str(e)}")
                continue

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Successfully processed S3 events'})
        }

    except Exception as e:
        logger.error(f"Unexpected error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'})
        }
    finally:
        if producer:
            try:
                producer.close()
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {str(e)}")
