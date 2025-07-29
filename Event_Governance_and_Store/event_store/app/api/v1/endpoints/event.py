from fastapi import APIRouter, HTTPException, status, Request
from typing import List, Dict, Any
from app.services.event_store import process_events
import logging

router = APIRouter()
logger = logging.getLogger("event_store.api.event")

@router.post("/event")
def receive_events(events: List[Dict[str, Any]]):
    try:
        result = process_events(events)
        return result
    except HTTPException as http_exc:
        logger.warning(f"HTTPException while processing events: {http_exc.detail}")
        raise http_exc
    except ValueError as ve:
        logger.error(f"ValueError while processing events: {ve}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid input: {str(ve)}"
        )
    except Exception as exc:
        logger.error(f"Unexpected error while processing events: {exc}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error while processing events"
        )

@router.get("/event/status")
def get_status():
    try:
        return {"message": "endpoint running"}
    except Exception as exc:
        logger.error(f"Error in status endpoint: {exc}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error in status endpoint"
        )