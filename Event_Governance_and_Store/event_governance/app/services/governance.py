from app.db.dynamo import aggregate_id_exists
from fastapi import HTTPException, status
import logging

logger = logging.getLogger(__name__)

def validate_event(event):
    try:
        aggregate_id = f"{event.event_tenant}#{event.event_sender}#{event.event_name}"
        exists = aggregate_id_exists(aggregate_id)
        if not exists:
            logger.warning(f"Aggregate ID '{aggregate_id}' not found for event: {event}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Aggregate ID '{aggregate_id}' does not exist."
            )
        return True
    except HTTPException as http_exc:
        logger.error(f"HTTPException during event validation: {http_exc.detail}")
        raise
    except Exception as e:
        logger.exception(f"Internal server error during event validation for event: {event}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error during event validation: {str(e)}"
        )