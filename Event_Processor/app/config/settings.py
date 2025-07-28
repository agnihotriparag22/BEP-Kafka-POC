import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file in project root
env_path = Path(__file__).parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)
# Fallback: try loading from current directory
load_dotenv()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_APPLICATION_ID = os.getenv("KAFKA_APPLICATION_ID", "event-processor")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

# Topic configuration
DYNAMODB_EVENTS_TOPIC = os.getenv("DYNAMODB_EVENTS_TOPIC", "dynamodb-events")
FILTER_RULES_TOPIC = os.getenv("FILTER_RULES_TOPIC", "filter-rules")
OUTPUT_TOPIC_PREFIX = os.getenv("OUTPUT_TOPIC_PREFIX", "filtered-events-")

# Debug print
print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")