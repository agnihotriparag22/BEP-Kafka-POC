import os
from dotenv import load_dotenv

load_dotenv()

EVENT_STORE_TABLE = os.environ.get("EVENT_STORE_TABLE") 
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
