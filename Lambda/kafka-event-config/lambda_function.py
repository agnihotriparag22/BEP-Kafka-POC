import boto3
import os
import json

def is_valid_event_detail(data):
    required_fields = [
        'event_description', 'event_name', 'event_entity_type', 'data_attribute',
        'metadata', 'sender', 'tenant_id', 'version'
    ]
    if not isinstance(data, dict):
        print("Invalid: Not a dict")
        return False
    for field in required_fields:
        if field not in data:
            print(f"Invalid: Missing field {field}")
            return False
    if not isinstance(data['data_attribute'], list) or not data['data_attribute']:
        print("Invalid: data_attribute not a non-empty list")
        return False
    if not isinstance(data['data_attribute'][0], dict):
        print("Invalid: data_attribute[0] not a dict")
        return False

    for k, v in data['data_attribute'][0].items():
        if not isinstance(v, bool):
            print(f"Invalid: data_attribute[0]['{k}'] is not a boolean, got {type(v)}")
            return False
    if not isinstance(data['metadata'], list):
        print("Invalid: metadata not a list")
        return False
    return True

def lambda_handler(event, context):
    bucket_name = os.environ.get('BUCKET_NAME', 'kafka-event-config-bucket')
    prefix = 'event_config/'
    dynamodb_table = os.environ.get('DYNAMODB_TABLE', 'kafka-event-config-table')

    s3 = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(dynamodb_table)
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        items = []
        valid_json_files = []
        table_items = []
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                if key != prefix :
                    items.append(key)
                    try:
                        file_obj = s3.get_object(Bucket=bucket_name, Key=key)
                        file_content = file_obj['Body'].read().decode('utf-8')
                        data = json.loads(file_content)
                        if is_valid_event_detail(data):
                            valid_json_files.append(key)
                            tenant_id = data.get('tenant_id')
                            sender = data.get('sender')
                            event_name = data.get('event_name')
                            table_item = {
                                'id': f"{tenant_id}#{sender}#{event_name}",
                                'version': data.get('version'),
                                'tenant_id': tenant_id,
                                'event_name': event_name,
                                'event_description': data.get('event_description'),
                                'sender': sender,
                                'metadata': data.get('metadata')
                            }
                            table_items.append(table_item)
                            try:
                                table.put_item(Item=table_item)
                                print(f"Inserted item into DynamoDB: {table_item['id']}")
                            except Exception as db_e:
                                print(f"Error inserting into DynamoDB for {table_item['id']}: {db_e}")
                        else:
                            print(f"File {key} is invalid.")
                    except Exception as e:
                        print(f"Error reading {key}: {e}")
                        continue
        print(f"Valid files: {valid_json_files}")
        return {
            'statusCode': 200,
            'body': {
                'valid_json_files': valid_json_files,
                'total_valid_json_files': len(valid_json_files),
                'total_objects_in_event_config': len(items),
                'table_items': table_items
            }
        }
    except Exception as e:
        print(f"Lambda error: {e}")
        return {
            'statusCode': 500,
            'body': str(e)
        }
