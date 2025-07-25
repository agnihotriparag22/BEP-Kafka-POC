from pydantic import BaseModel
from typing import List, Dict, Any

class Event(BaseModel):
    event_type: str
    event_name: str
    event_sender: str
    event_tenant: str
    event_timestamp: str
    # ... other fields as needed 

class EventStoreItem(BaseModel):
    tenant_aggregate_id: str
    event_id: str
    tenant_event_name: str
    # published_time_utc: str
    published_epoch_time: int  # Make this optional for backward compatibility
    event_data: List[Dict[str, Any]] 