import json
from app.repository.event_generator_repo import generate_event_data
import requests
import time

BATCH_SIZE = 4
TOTAL_EVENTS = 10

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

    event_store_url = "http://localhost:8000/event"
    headers = {"Content-Type": "application/json"}

    for i, event_batch in enumerate(batch(all_events, BATCH_SIZE)):
        print(f"\nSending batch {i+1} to Event Store API")
        store_response = requests.post(event_store_url, data=json.dumps(event_batch, default=str), headers=headers)
        print(f"Store response: {store_response.status_code}")
        try:
            print("Store response JSON:", store_response.json())
        except Exception:
            print("Store response Text:", store_response.text)
