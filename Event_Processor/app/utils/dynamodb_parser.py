from typing import Dict, Any, Optional

def parse_dynamodb_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """Parse DynamoDB item format to Python dict"""
    if not isinstance(item, dict):
        return item
    
    result = {}
    for key, value in item.items():
        if isinstance(value, dict) and len(value) == 1:
            type_key = list(value.keys())[0]
            type_value = value[type_key]
            
            if type_key == 'S':  
                result[key] = type_value
            elif type_key == 'N':  
                result[key] = int(type_value) if type_value.isdigit() else float(type_value)
            elif type_key == 'M':  
                result[key] = parse_dynamodb_item(type_value)
            elif type_key == 'L': 
                result[key] = [parse_dynamodb_item(item) for item in type_value]
            else:
                result[key] = type_value
        else:
            result[key] = value
    
    return result

def extract_event_from_dynamodb_stream(stream_record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # Accept both full stream record and just NewImage
    if 'dynamodb' in stream_record and 'NewImage' in stream_record['dynamodb']:
        new_image = parse_dynamodb_item(stream_record['dynamodb']['NewImage'])
    elif 'event_id' in stream_record and 'event_data' in stream_record:
        # Looks like a NewImage dict directly
        new_image = parse_dynamodb_item(stream_record)
    else:
        return None

    if 'event_data' not in new_image:
        return None

    event_data_list = new_image['event_data']
    if not event_data_list:
        return None

    first_event_wrapper = event_data_list[0]

    if 'M' in first_event_wrapper:
        nested_map = first_event_wrapper['M']
        if 'M' in nested_map:
            event_details_raw = nested_map['M']
            event_details = parse_dynamodb_item(event_details_raw)
        else:
            event_details = parse_dynamodb_item(nested_map)
    else:
        event_id = list(first_event_wrapper.keys())[0]
        event_details = first_event_wrapper[event_id]

    # Transform to expected format with proper string conversion
    return {
        "metadata": {
            "event_id": str(event_details.get("event_id", "")),
            "event_type": str(event_details.get("event_type", "")),
            "source": str(event_details.get("event_metadata", {}).get("source", "")),
            "timestamp": str(new_image.get("published_epoch_time", "")),
            "flags": event_details.get("event_metadata", {}).get("flags", {})
        }
    }