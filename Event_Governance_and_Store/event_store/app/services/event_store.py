import requests
from fastapi import HTTPException
from app.db.dynamo import batch_store_events
from app.models.event import Event
from typing import List

GOVERNANCE_URL = "http://localhost:8001/validate-event"  # Adjust port as needed

# Batch process events

def process_events(events: List[Event]):
    errors = []
    for idx, event in enumerate(events):
        resp = requests.post(GOVERNANCE_URL, json=event.dict())
        if resp.status_code != 200:
            errors.append({"index": idx, "error": resp.json()})
    if errors:
        raise HTTPException(status_code=422, detail=errors)
    # All valid, batch insert
    batch_store_events([event.dict() for event in events])
    return {"status": "all events stored", "count": len(events)} 