import boto3
import os
import logging
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError
from fastapi import HTTPException
from app.core.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, EVENT_STORE_TABLE

logger = logging.getLogger("event_store.db.dynamo")
logger.setLevel(logging.INFO)

def store_event(event: dict):
    try:
        dynamodb = boto3.resource(
            "dynamodb",
            region_name="us-east-2",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        table = dynamodb.Table(EVENT_STORE_TABLE)
        table.put_item(Item=event)
        logger.info(f"Successfully stored event: {event}")
    except NoCredentialsError as e:
        logger.error(f"AWS credentials not found: {e}")
        raise HTTPException(status_code=401, detail="AWS credentials not found.")
    except EndpointConnectionError as e:
        logger.error(f"Could not connect to DynamoDB endpoint: {e}")
        raise HTTPException(status_code=503, detail="Could not connect to DynamoDB endpoint.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        logger.error(f"DynamoDB ClientError [{error_code}]: {e}")
        if error_code == 'ResourceNotFoundException':
            raise HTTPException(status_code=404, detail="DynamoDB table not found.")
        else:
            raise HTTPException(status_code=500, detail=f"DynamoDB ClientError: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in store_event: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

def batch_store_events(events: list, table_name: str = None):
    table_name = table_name or os.getenv("EVENT_STORE_TABLE", "kafka-event-store-table")
    try:
        dynamodb = boto3.resource(
            "dynamodb",
            region_name="us-east-2",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY    
        )
        table = dynamodb.Table(table_name)
        with table.batch_writer() as batch:
            for event in events:
                batch.put_item(Item=event)
        logger.info(f"Successfully batch stored {len(events)} events to table {table_name}")
    except NoCredentialsError as e:
        logger.error(f"AWS credentials not found: {e}")
        raise HTTPException(status_code=401, detail="AWS credentials not found.")
    except EndpointConnectionError as e:
        logger.error(f"Could not connect to DynamoDB endpoint: {e}")
        raise HTTPException(status_code=503, detail="Could not connect to DynamoDB endpoint.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        logger.error(f"DynamoDB ClientError [{error_code}]: {e}")
        if error_code == 'ResourceNotFoundException':
            raise HTTPException(status_code=404, detail="DynamoDB table not found.")
        else:
            raise HTTPException(status_code=500, detail=f"DynamoDB ClientError: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in batch_store_events: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")