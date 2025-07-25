import boto3
import os
from app.core.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, EVENT_STORE_TABLE, DYNAMODB_ENDPOINT_URL
# In production, use boto3 to store events in DynamoDB


def store_event(event: dict):
    # Example stub
    print(f"Storing event: {event}") 

def batch_store_events(events: list, table_name: str = None):
    table_name = table_name or os.getenv("EVENT_STORE_TABLE", "event-store-table-local")
    
    dynamodb = boto3.resource(
        "dynamodb",
        region_name="us-east-2",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url=DYNAMODB_ENDPOINT_URL
    )
    
    table = dynamodb.Table(table_name)
    
    with table.batch_writer() as batch:
        for event in events:
            batch.put_item(Item=event) 