
    # The following code assumes you have a Kafka producer set up and accessible.
    # If using the 'kafka-python' library, you would typically import KafkaProducer and set it up outside the handler.
    # Here, we'll instantiate it inside the handler for demonstration, but in production, you should reuse the producer.

import json
from kafka import KafkaProducer
import base64
import decimal

KAFKA_BROKER = '3.15.46.32:9092'  # Change to your Kafka broker address
KAFKA_TOPIC = 'raw-event'  # Change to your Kafka topic

def send_to_kafka(producer, topic, message):
    producer.send(topic, value=message.encode('utf-8'))

def convert_decimals(obj):
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, decimal.Decimal):
        return float(obj)
    else:
        return obj

def lambda_handler(event, context):
    
    producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER])
    records = event.get('Records', [])
    
    for record in records:
        # DynamoDB Streams encode the new image in 'NewImage'
        if record.get('eventSource') == 'aws:dynamodb':
            new_image = record['dynamodb'].get('NewImage')
            if new_image:
                # Convert DynamoDB JSON to normal JSON
                def deserialize(dynamo_obj):
                    # Simple deserializer for DynamoDB JSON to Python dict
                    from boto3.dynamodb.types import TypeDeserializer
                    deserializer = TypeDeserializer()
                    return {k: deserializer.deserialize(v) for k, v in dynamo_obj.items()}
                event_data = deserialize(new_image)
                event_data = convert_decimals(event_data)
                message = json.dumps(event_data)
                send_to_kafka(producer, KAFKA_TOPIC, message)
    producer.flush()
    
    return {
        'statusCode': 200,
        'body': json.dumps('Processed DynamoDB stream and sent to Kafka.')
    }
