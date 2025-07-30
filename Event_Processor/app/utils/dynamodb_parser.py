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
            elif type_key == 'BOOL':
                result[key] = type_value
            elif type_key == 'NULL':
                result[key] = None
            else:
                result[key] = type_value
        else:
            result[key] = value
    
    return result

def extract_event_from_dynamodb_stream(stream_record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extracts and parses the event from a DynamoDB stream record (or NewImage dict).
    Returns a dict with the metadata field that matches DynamoDBEvent model.
    """
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
    if not event_data_list or not isinstance(event_data_list, list):
        return None

    # The first element is a dict with a single key (event_id) mapping to the event details
    first_event = event_data_list[0]
    if isinstance(first_event, dict):
        # If the dict is {"M": {...}}, extract the map
        if "M" in first_event:
            event_map = first_event["M"]
            # The event_map is {event_id: {event_details}}
            if isinstance(event_map, dict) and len(event_map) == 1:
                event_id_key = next(iter(event_map))
                event_details = event_map[event_id_key]
                if "M" in event_details:
                    event_details = event_details["M"]
            else:
                event_details = event_map
        else:
            # Sometimes the dict is {event_id: {M: {...}}}
            event_id_key = next(iter(first_event))
            event_details = first_event[event_id_key]
            if "M" in event_details:
                event_details = event_details["M"]
    else:
        return None

    # Parse the event_details to convert DynamoDB types to Python types
    event_details = parse_dynamodb_item(event_details)
    
    # Extract metadata from event_metadata
    event_metadata = event_details.get("event_metadata", {})
    
    # Return the format expected by DynamoDBEvent model
    return {
        "metadata": {
            "event_id": str(event_details.get("event_id", "")),
            "event_type": str(event_details.get("event_type", "")),
            "source": str(event_metadata.get("source", "")),
            "timestamp": str(new_image.get("published_epoch_time", "")),
            "flags": event_metadata.get("flags", {})
        }
    }