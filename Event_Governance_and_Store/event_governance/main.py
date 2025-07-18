from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any

app = FastAPI()

# Mocked function to get required attributes for an event type from DynamoDB
# In production, replace with boto3 call
REQUIRED_ATTRIBUTES = {
    "Policy": [ "AONE.Completed", "AONE.Initiated", "AONE.Updated", "AONE.Cancelled", "AONE.Renewed" ],
    "User": [ "BTWO.Initiated", "BTWO.Registered", "BTWO.Updated", "BTWO.Deleted", "BTWO.Locked" ],
    "Claim": [ "CTHREE.Failed", "CTHREE.Submitted", "CTHREE.Approved", "CTHREE.Rejected", "CTHREE.Closed" ],
    "Document": [ "DFOUR.Updated", "DFOUR.Uploaded", "DFOUR.Deleted", "DFOUR.Verified", "DFOUR.Shared" ],
    "Account": [ "EFIVE.Created", "EFIVE.Updated", "EFIVE.Deleted", "EFIVE.Suspended", "EFIVE.Reactivated" ]
}

def get_required_attributes(event_type: str):
    return REQUIRED_ATTRIBUTES.get(event_type, [])

class Event(BaseModel):
    event_type: str
    event_name: str
    event_sender: str
    event_tenant: str
    event_timestamp: str
    # ... other fields as needed

@app.post("/validate-event")
def validate_event(event: Dict[str, Any]):
    event_type = event.get("event_type")
    required_attrs = get_required_attributes(event_type)
    missing = [attr for attr in required_attrs if not event.get(attr)]
    if missing:
        raise HTTPException(status_code=422, detail=f"Missing attributes: {missing}")
    return {"status": "ok"} 