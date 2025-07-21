from pydantic import BaseModel

class Event(BaseModel):
    event_tenant: str
    event_sender: str
    event_name: str
    # ... other fields as needed 