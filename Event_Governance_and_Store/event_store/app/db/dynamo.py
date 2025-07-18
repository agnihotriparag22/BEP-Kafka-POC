import boto3
import os

# In production, use boto3 to store events in DynamoDB

def store_event(event: dict):
    # Example stub
    print(f"Storing event: {event}") 

def batch_store_events(events: list, table_name: str = None):
    table_name = table_name or os.getenv("EVENT_STORE_TABLE", "EventStore")
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    with table.batch_writer() as batch:
        for event in events:
            batch.put_item(Item=event) 