import boto3
import os
import logging
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError
from fastapi import HTTPException
from app.core.config import GOVERNANCE_TABLE, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

logger = logging.getLogger("event_governance.db.dynamo")
logger.setLevel(logging.INFO)

def aggregate_id_exists(aggregate_id: str) -> bool:
    try:
        dynamodb = boto3.resource(
            "dynamodb",
            region_name="us-east-2",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY   
        )

        table = dynamodb.Table(GOVERNANCE_TABLE)
        response = table.get_item(Key={"id": aggregate_id})
        return "Item" in response 

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
        logger.error(f"Unexpected error in aggregate_id_exists: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")