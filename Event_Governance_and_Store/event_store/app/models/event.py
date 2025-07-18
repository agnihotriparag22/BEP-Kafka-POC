from pydantic import BaseModel

class Event(BaseModel):
    event_type: str
    event_name: str
    event_sender: str
    event_tenant: str
    event_timestamp: str
    # ... other fields as needed 