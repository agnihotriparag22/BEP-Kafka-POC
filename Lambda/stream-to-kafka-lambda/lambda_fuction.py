import json
from kafka import KafkaProducer, errors as kafka_errors

KAFKA_BROKER = '3.15.46.32:9092'  
KAFKA_TOPIC = 'raw-event'  

def send_to_kafka(producer, topic, message):
    producer.send(topic, value=message.encode('utf-8'))

def lambda_handler(event, context):
    try:
        producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER])
    except kafka_errors.NoBrokersAvailable as e:
        # Log a clear error message for troubleshooting
        return {
            'statusCode': 500,
            'body': json.dumps(f'Kafka broker not available: {str(e)}. '
                               f'Check broker address, network connectivity, and security groups.')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error initializing Kafka producer: {str(e)}')
        }

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
