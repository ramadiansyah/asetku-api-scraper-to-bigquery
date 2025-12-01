# fp % python -m scripts.scraping1.extractor_runner

from services.scraping1.extractor import extract
from utils.logger import setup_logger

logger = setup_logger()

if __name__ == "__main__":
    logger.info(f"📥 Running Extractor")
    extract()



