    # The following code assumes you have a Kafka producer set up and accessible.
    # If using the 'kafka-python' library, you would typically import KafkaProducer and set it up outside the handler.
    # Here, we'll instantiate it inside the handler for demonstration, but in production, you should reuse the producer.

import json
from kafka import KafkaProducer

KAFKA_BROKER = '3.15.46.32:9092'  # Change to your Kafka broker address
KAFKA_TOPIC = 'raw-event'  # Change to your Kafka topic

def send_to_kafka(producer, topic, message):
    producer.send(topic, value=message.encode('utf-8'))

def lambda_handler(event, context):
    producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER])
    records = event.get('Records', [])
    
    for record in records:
        # DynamoDB Streams encode the new image in 'NewImage'
        if record.get('eventSource') == 'aws:dynamodb':
            new_image = record['dynamodb'].get('NewImage')
            if new_image:
                
                message = json.dumps(new_image)
                send_to_kafka(producer, KAFKA_TOPIC, message)
    producer.flush()
    
    return {
        'statusCode': 200,
        'body': json.dumps('Processed DynamoDB stream and sent to Kafka.')
    }
