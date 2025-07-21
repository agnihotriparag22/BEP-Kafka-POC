from fastapi import APIRouter, HTTPException
from app.models.event import Event
from app.services.governance import validate_event

router = APIRouter()

@router.post("/validate-event")
def validate_event_endpoint(event: Event):
    if validate_event(event):
        return {"status": "ok"}
    raise HTTPException(status_code=404, detail="Aggregate ID not found") 
    