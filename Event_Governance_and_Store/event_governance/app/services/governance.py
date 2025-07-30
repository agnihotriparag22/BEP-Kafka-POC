from app.db.dynamo import aggregate_id_exists
from fastapi import HTTPException, status
import logging
from functools import lru_cache
 
logger = logging.getLogger(__name__)
 
@lru_cache(maxsize=None)
def cached_aggregate_id_exists(aggregate_id: str) -> bool:
    return aggregate_id_exists(aggregate_id)
 
def validate_event(event):
    try:
        aggregate_id = f"{event.event_tenant}#{event.event_sender}#{event.event_name}"
        # Get cache stats before call
        cache_stats_before = cached_aggregate_id_exists.cache_info()
        exists = cached_aggregate_id_exists(aggregate_id)
        cache_stats_after = cached_aggregate_id_exists.cache_info()
        if cache_stats_after.hits > cache_stats_before.hits:
            print(f"Cache hit: aggregate_id '{aggregate_id}' found in cache.")
            logger.info(f"Cache hit: aggregate_id '{aggregate_id}' found in cache.")
        elif cache_stats_after.misses > cache_stats_before.misses:
            print((f"Cache miss: aggregate_id '{aggregate_id}' not present in cache. Queried DB and cached result: {exists}"))
            logger.info(f"Cache miss: aggregate_id '{aggregate_id}' not present in cache. Queried DB and cached result: {exists}")
        if exists:
            print((f"Aggregate ID '{aggregate_id}' exists."))
            logger.info(f"Aggregate ID '{aggregate_id}' exists.")
        else:
            print((f"Aggregate ID '{aggregate_id}' not found for event: {event}"))
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