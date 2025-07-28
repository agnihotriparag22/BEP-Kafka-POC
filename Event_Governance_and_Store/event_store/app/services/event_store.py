import requests
from fastapi import HTTPException
from app.db.dynamo import batch_store_events
from app.models.event import Event
from typing import List
import time

GOVERNANCE_URL = "http://localhost:8001/validate-events"  # Adjust port as needed

# Batch process events

def process_events(events: list):
    # Forward batch to Governance API for validation
    try:
        response = requests.post(GOVERNANCE_URL, json=[event.dict() if hasattr(event, 'dict') else event for event in events])
        response.raise_for_status()
        result = response.json()
        valid_indices = result.get("valid_indices", [])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Governance validation failed: {str(e)}")

    valid_events = [events[idx] for idx in valid_indices]
    transformed_events = []
    for event in valid_events:
        # Use dict if it's a Pydantic model, else assume dict
        event_dict = event.dict() if hasattr(event, 'dict') else event
        event_id = event_dict.get("event_id")
        tenant = event_dict.get("event_tenant")
        sender = event_dict.get("event_sender")
        event_name = event_dict.get("event_name")
        aggregate_id = f"{tenant}#{sender}#{event_name}"
        tenant_event_name = f"{tenant}#{sender}.{event_name}"
        published_epoch_time = int(time.time())
        tenant_aggregate_id = f"{tenant}#{aggregate_id}"
        event_data = [{event_id: event_dict}]
        transformed_events.append({
            "event_id": event_id,
            "tenant_event_name": tenant_event_name,
            "published_epoch_time": published_epoch_time,
            "tenant_aggregate_id": tenant_aggregate_id,
            "event_data": event_data
        })
    if transformed_events:
        batch_store_events(transformed_events)
    return {
        "status": "batch processed",
        "received": len(events),
        "stored": len(transformed_events),
        "invalid": len(events) - len(transformed_events)
    } 