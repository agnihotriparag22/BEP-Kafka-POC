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
    event_id: str
    tenant_event_name: str
    published_epoch_time: int
    tenant_aggregate_id: str
    event_data: List[Dict[str, Any]] 