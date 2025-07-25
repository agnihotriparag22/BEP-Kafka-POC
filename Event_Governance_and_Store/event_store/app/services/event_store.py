import requests
from fastapi import HTTPException
from app.db.dynamo import batch_store_events
from app.models.event import Event
from typing import List

GOVERNANCE_URL = "http://localhost:8001/validate-event"  # Adjust port as needed

# Batch process events

def process_events(events: list):
    # Convert Pydantic models to dicts for DynamoDB
    batch_store_events([event.dict() for event in events])
    return {"status": "all events stored", "count": len(events)} 