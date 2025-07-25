from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field


class EventMetadata(BaseModel):
    """Model representing event metadata with filtering flags"""
    event_id: str
    event_type: str
    source: str
    timestamp: str
    flags: Dict[str, Any] = Field(default_factory=dict)
    # Additional metadata fields can be added as needed


class DynamoDBEvent(BaseModel):
    """Model representing an event from DynamoDB Streams"""
    metadata: EventMetadata
    payload: Dict[str, Any]


class FilterRule(BaseModel):
    """Model representing a filter rule"""
    attribute_path: str  # Path to the attribute in event metadata (e.g., "metadata.flags.is_priority")
    value: Any  # Value to compare against
    target_topic: str  # Topic to route the event to if rule matches
    
    def evaluate(self, event: DynamoDBEvent) -> bool:
        """Evaluate if the event matches this rule"""
        # Navigate to the attribute using the path
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
        
        # Simple equality check
        return current == self.value