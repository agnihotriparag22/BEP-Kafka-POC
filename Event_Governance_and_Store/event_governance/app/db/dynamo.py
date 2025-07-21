import boto3
import os
from app.core.config import GOVERNANCE_TABLE, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

def aggregate_id_exists(aggregate_id: str) -> bool:
    dynamodb = boto3.resource(
        "dynamodb",
        region_name="us-east-2",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY   
    )

    table = dynamodb.Table(GOVERNANCE_TABLE)
    
    response = table.get_item(Key={"id": aggregate_id})
    return "Item" in response 