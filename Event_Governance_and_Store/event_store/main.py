from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
from .dynamo import store_event

app = FastAPI()

GOVERNANCE_URL = "http://localhost:8001/validate-event"  # Adjust port as needed

class Event(BaseModel):
    event_type: str
    event_name: str
    event_sender: str
    event_tenant: str
    event_timestamp: str
    # ... other fields as needed

@app.post("/event")
def receive_event(event: Event):
    # Call governance API
    resp = requests.post(GOVERNANCE_URL, json=event.dict())
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.json())
    # Store event (mocked)
    store_event(event.dict())
    return {"status": "stored"} 