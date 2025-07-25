import os
import csv
import boto3
from botocore.exceptions import ClientError

GOVERNANCE_TABLE = os.environ.get("GOVERNANCE_TABLE", "governance-table-local")
EVENT_STORE_TABLE = os.environ.get("EVENT_STORE_TABLE", "event-store-table-local")
DYNAMODB_ENDPOINT_URL = os.environ.get("DYNAMODB_ENDPOINT_URL", "http://localhost:8000")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "fakeMyKeyId")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "fakeSecretKey")

# Table schemas (adjust as needed)
governance_table_schema = {
    'TableName': GOVERNANCE_TABLE,
    'KeySchema': [
        {'AttributeName': 'id', 'KeyType': 'HASH'},
    ],
    'AttributeDefinitions': [
        {'AttributeName': 'id', 'AttributeType': 'S'},
    ],
    'ProvisionedThroughput': {
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
}

event_store_table_schema = {
    'TableName': EVENT_STORE_TABLE,
    'KeySchema': [
        {'AttributeName': 'tenant_aggregate_id', 'KeyType': 'HASH'},
        {'AttributeName': 'event_id', 'KeyType': 'RANGE'},
    ],
    'AttributeDefinitions': [
        {'AttributeName': 'tenant_aggregate_id', 'AttributeType': 'S'},
        {'AttributeName': 'event_id', 'AttributeType': 'S'},
    ],
    'ProvisionedThroughput': {
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
}

def get_dynamodb():
    return boto3.resource(
        'dynamodb',
        region_name='us-east-2',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url=DYNAMODB_ENDPOINT_URL
    )

def create_table_if_not_exists(dynamodb, schema):
    try:
        table = dynamodb.create_table(**schema)
        print(f"Creating table {schema['TableName']}...")
        table.wait_until_exists()
        print(f"Table {schema['TableName']} created.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"Table {schema['TableName']} already exists.")
        else:
            raise

def import_governance_csv(dynamodb, table_name, csv_path):
    table = dynamodb.Table(table_name)
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Convert metadata from string to list if needed
            if 'metadata' in row and row['metadata'].startswith('['):
                import ast
                row['metadata'] = ast.literal_eval(row['metadata'])
            table.put_item(Item=row)
    print(f"Imported data from {csv_path} into {table_name}.")

def main():
    dynamodb = get_dynamodb()
    create_table_if_not_exists(dynamodb, governance_table_schema)
    create_table_if_not_exists(dynamodb, event_store_table_schema)
    import_governance_csv(dynamodb, GOVERNANCE_TABLE, os.path.join(os.path.dirname(__file__), '..', 'results.csv'))

if __name__ == "__main__":
    main() 