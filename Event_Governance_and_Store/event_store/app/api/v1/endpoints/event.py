from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any
from app.services.event_store import process_events

router = APIRouter()

@router.post("/event")
def receive_events(events: List[Dict[str, Any]]):
    return process_events(events)

@router.get("/event/status")
def get_status():
    return {"message":"endpoint running"}