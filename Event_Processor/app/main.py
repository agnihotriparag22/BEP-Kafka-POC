import logging
from app.processors.event_processor import EventProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting Event Processor application")
    processor = EventProcessor()
    processor.start()

if __name__ == "__main__":
    main()