from app.db.dynamo import aggregate_id_exists

def validate_event(event):
    aggregate_id = f"{event.event_tenant}#{event.event_sender}#{event.event_name}"
    return aggregate_id_exists(aggregate_id) 