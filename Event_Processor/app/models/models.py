from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field


class EventMetadata(BaseModel):
    """Model representing event metadata with filtering flags"""
    event_id: str
    event_type: str
    source: str
    timestamp: str
    flags: Dict[str, Any] = Field(default_factory=dict)


class DynamoDBEvent(BaseModel):
    """Model representing an event from DynamoDB Streams"""
    metadata: EventMetadata


class FilterRule(BaseModel):
    """Model representing a filter rule"""
    attribute_path: str  
    value: Any  
    target_topic: str  
    
    def evaluate(self, event: DynamoDBEvent) -> bool:
        """Evaluate if the event matches this rule"""

        parts = self.attribute_path.split('.')
        current = event.model_dump()
        
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            elif isinstance(current, list) and part.isdigit():
                index = int(part)
                if 0 <= index < len(current):
                    current = current[index]
                else:
                    return False
            else:
                return False
        
        return current == self.value