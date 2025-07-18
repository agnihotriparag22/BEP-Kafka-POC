import json
from app.repository.event_generator_repo import generate_event_data
import requests

if __name__ == "__main__":

    # Code to create a json file of events in Event_Generation folder 
    # comment out the below code to not create a file.

    # event = generate_event_data()
    
    # with open("generated_event.json", "w") as f:
    #     json.dump(event, f, indent=2, default=str)
    
    # Un-Comment the below line to see the result on terminal
    # print(json.dumps(generate_event_data(), indent=2, default=str))
    

    event_store_url = "http://localhost:8000/event"
    event_data = generate_event_data()
    headers = {"Content-Type": "application/json"}
    response = requests.post(event_store_url, data=json.dumps(event_data, default=str), headers=headers)
    print("POST Response:", response.status_code)
    try:
        print("Response JSON:", response.json())
    except Exception:
        print("Response Text:", response.text)
