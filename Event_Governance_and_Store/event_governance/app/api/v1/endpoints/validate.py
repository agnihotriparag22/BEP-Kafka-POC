from fastapi import APIRouter, HTTPException, status
from app.models.event import Event
from app.services.governance import validate_event
from typing import List
from fastapi import Body
from fastapi.responses import JSONResponse
import logging

router = APIRouter()
logger = logging.getLogger("event_validation")

# @router.post("/validate-event", status_code=status.HTTP_200_OK)
# def validate_event_endpoint(event: Event):
#     try:
#         if validate_event(event):
#             return JSONResponse(status_code=status.HTTP_200_OK, content={"status": "ok"})
#         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Aggregate ID not found")
#     except HTTPException as http_exc:
#         logger.warning(f"Validation failed for event: {event.dict() if hasattr(event, 'dict') else str(event)} - {http_exc.detail}")
#         raise http_exc
#     except Exception as exc:
#         logger.error(f"Unexpected error during event validation: {exc}", exc_info=True)
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error during event validation")

@router.post("/validate-events", status_code=status.HTTP_200_OK)
def batch_validate_events(events: List[Event] = Body(...)):
    valid_events = []
    invalid_events = []
    try:
        for idx, event in enumerate(events):
            try:
                if validate_event(event):
                    valid_events.append(idx)
                else:
                    invalid_events.append(idx)
            except Exception as exc:
                logger.error(f"Error validating event at index {idx}: {exc}", exc_info=True)
                invalid_events.append(idx)
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"valid_indices": valid_events, "invalid_indices": invalid_events}
        )
    except Exception as exc:
        logger.error(f"Unexpected error during batch event validation: {exc}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error during batch event validation")