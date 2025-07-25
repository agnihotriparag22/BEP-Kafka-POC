import os
from dotenv import load_dotenv, find_dotenv

# Try to load .env from current and parent directories
load_dotenv(find_dotenv())

EVENT_STORE_TABLE = os.environ.get("EVENT_STORE_TABLE", "event-store-table-local")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "fakeMyKeyId")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "fakeSecretKey")
DYNAMODB_ENDPOINT_URL = os.environ.get("DYNAMODB_ENDPOINT_URL", "http://localhost:8000")
