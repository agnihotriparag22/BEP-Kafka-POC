import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


def extract_attribute(data: Dict[str, Any], path: str) -> Optional[Any]:
    """
    Extract an attribute from a nested dictionary using a dot-notation path
    
    Args:
        data: The dictionary to extract from
        path: Dot-notation path (e.g., "metadata.flags.is_priority")
        
    Returns:
        The value at the path or None if not found
    """
    parts = path.split('.')
    current = data
    
    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    
    return current


def safe_json_loads(data: bytes) -> Dict[str, Any]:
    """
    Safely parse JSON data
    
    Args:
        data: JSON bytes data
        
    Returns:
        Parsed dictionary or empty dict on error
    """
    try:
        return json.loads(data)
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {e}")
        return {}