from fastapi import APIRouter, HTTPException
from typing import List
from app.models.event import Event
from app.services.event_store import process_events

router = APIRouter()

@router.post("/event")
def receive_events(events: List[Event]):
    return process_events(events) 

@router.get("/event/status")
def get_status():
    return {"message":"endpoint running"}