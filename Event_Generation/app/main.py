import json
from app.repository.event_generator_repo import generate_event_data
import requests
import time
import datetime

BATCH_SIZE = 10
TOTAL_EVENTS = 99

def batch(iterable, n):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

if __name__ == "__main__":
    # Generate 150 events
    all_events = []
    while len(all_events) < TOTAL_EVENTS:
        all_events.extend(generate_event_data())
    all_events = all_events[:TOTAL_EVENTS]

    governance_url = "http://localhost:8001/validate-events"  # Assuming governance API runs on 8001
    event_store_url = "http://localhost:8002/event"
    headers = {"Content-Type": "application/json"}

    for i, event_batch in enumerate(batch(all_events, BATCH_SIZE)):
        print(f"\nProcessing batch {i+1}")
        # Validate batch
        response = requests.post(governance_url, data=json.dumps(event_batch, default=str), headers=headers)
        print(f"Validation response for batch {i+1}: {response.status_code}")
        try:
            print("Validation response JSON:", response.json())
        except Exception:
            print("Validation response Text:", response.text)
        if response.status_code != 200:
            print(f"Validation failed for batch {i+1}: {response.text}")
            continue
        result = response.json()
        valid_indices = result.get("valid_indices", [])
        valid_events = [event_batch[idx] for idx in valid_indices]
        print(f"Valid events in batch {i+1}: {len(valid_events)}")
        # Transform and send valid events
        transformed_events = []
        for event in valid_events:
            event_id = event.get("event_id")
            tenant = event.get("event_tenant")
            sender = event.get("event_sender")
            event_name = event.get("event_name")
            aggregate_id = f"{tenant}#{sender}#{event_name}"
            tenant_event_name = f"{tenant}#{sender}.{event_name}"
            published_epoch_time = int(time.time())
            # published_time_utc = datetime.datetime.utcnow().isoformat() + 'Z'
            tenant_aggregate_id = f"{tenant}#{tenant}#{sender}#{event_name}"
            # Use the provided structure for the event store
            transformed_events.append({
                "tenant_aggregate_id": tenant_aggregate_id,
                "event_id": event_id,
                "tenant_event_name": tenant_event_name,
                # "published_time_utc": published_time_utc,
                "published_epoch_time": published_epoch_time,
                "event_data": [
                    {event_id: event}
                ]
            })
        if transformed_events:
            store_response = requests.post(event_store_url, data=json.dumps(transformed_events, default=str), headers=headers)
            print(f"Store response: {store_response.status_code}")
            try:
                print("Store response JSON:", store_response.json())
            except Exception:
                print("Store response Text:", store_response.text)
        else:
            print(f"No valid events to store in batch {i+1}.")
