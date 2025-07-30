import requests
from fastapi import HTTPException, status
from app.db.dynamo import batch_store_events
from app.models.event import Event
from typing import List
import time
import logging

logger = logging.getLogger("event_store.services.event_store")
logger.setLevel(logging.INFO)

GOVERNANCE_URL = "http://localhost:8001/validate-events"  # Adjust port as needed

def process_events(events: list):
    # Forward batch to Governance API for validation
    try:
        logger.info(f"Sending {len(events)} events to Governance API for validation.")
        response = requests.post(
            GOVERNANCE_URL,
            json=[event.dict() if hasattr(event, 'dict') else event for event in events],
            timeout=30
        )
        response.raise_for_status()
        result = response.json()
        valid_indices = result.get("valid_indices", [])
        logger.info(f"Received validation result from Governance API: {result}")
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout while connecting to Governance API: {e}")
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Timeout while connecting to Governance API."
        )
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error to Governance API: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not connect to Governance API."
        )
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error from Governance API: {e} - Response: {getattr(e.response, 'text', None)}")
        status_code = e.response.status_code if e.response else status.HTTP_502_BAD_GATEWAY
        raise HTTPException(
            status_code=status_code,
            detail=f"Governance API returned error: {getattr(e.response, 'text', str(e))}"
        )
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception during Governance API call: {e}")
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Error communicating with Governance API: {str(e)}"
        )
    except Exception as e:
        logger.exception(f"Unexpected error during governance validation: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Governance validation failed: {str(e)}"
        )

    valid_events = [events[idx] for idx in valid_indices]
    transformed_events = []
    for event in valid_events:
        try:
            # Use dict if it's a Pydantic model, else assume dict
            event_dict = event.dict() if hasattr(event, 'dict') else event
            event_id = event_dict.get("event_id")
            tenant = event_dict.get("event_tenant")
            sender = event_dict.get("event_sender")
            event_name = event_dict.get("event_name")
            aggregate_id = f"{tenant}#{sender}#{event_name}"
            tenant_event_name = f"{tenant}#{sender}.{event_name}"
            published_epoch_time = int(time.time())
            tenant_aggregate_id = f"{tenant}#{aggregate_id}"
            event_data = [{event_id: event_dict}]
            transformed_events.append({
                "event_id": event_id,
                "tenant_event_name": tenant_event_name,
                "published_epoch_time": published_epoch_time,
                "tenant_aggregate_id": tenant_aggregate_id,
                "event_data": event_data
            })
        except Exception as e:
            logger.error(f"Error transforming event {event}: {e}", exc_info=True)
            continue  # Skip this event

    if transformed_events:
        try:
            batch_store_events(transformed_events)
            logger.info(f"Successfully stored {len(transformed_events)} events.")
        except HTTPException as http_exc:
            logger.error(f"HTTPException during batch_store_events: {http_exc.detail}")
            raise
        except Exception as e:
            logger.exception(f"Unexpected error during batch_store_events: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to store events: {str(e)}"
            )
    else:
        logger.warning("No valid events to store after validation and transformation.")

    return {
        "status": "batch processed",
        "received": len(events),
        "stored": len(transformed_events),
        "invalid": len(events) - len(transformed_events)
    }