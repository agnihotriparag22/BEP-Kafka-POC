from fastapi import APIRouter, HTTPException
from app.models.event import Event
from app.services.governance import validate_event
from typing import List
from fastapi import Body

router = APIRouter()

@router.post("/validate-event")
def validate_event_endpoint(event: Event):
    if validate_event(event):
        return {"status": "ok"}
    raise HTTPException(status_code=404, detail="Aggregate ID not found")

@router.post("/validate-events")
def batch_validate_events(events: List[Event] = Body(...)):
    valid_events = []
    invalid_events = []
    for idx, event in enumerate(events):
        if validate_event(event):
            valid_events.append(idx)
        else:
            invalid_events.append(idx)
    return {"valid_indices": valid_events, "invalid_indices": invalid_events} 
    