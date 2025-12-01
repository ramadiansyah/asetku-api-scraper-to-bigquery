# fp % python -m scripts.scraping1.loader_runner
import yaml
import sys

from services.scraping1.loader import load_local_csv_to_bq
from utils.logger import setup_logger

logger = setup_logger()

def run_loader(config_path: str): 
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        logger.error(f"❌ Config file not found: {config_path}")
        sys.exit(1)
    except yaml.YAMLError as e:
        logger.error(f"❌ Error parsing the YAML file: {e}")
        sys.exit(1)

    load_local_csv_to_bq(
        csv_path=config["csv_path"],
        dataset_id=config["dataset_id"],
        table_id=config["table_id"],
        project_id=config["project_id"],
        credential_path=config["credential_path"]
    )

if __name__ == "__main__":
    config_path="config/config_asetku.yaml"
    run_loader(config_path)




