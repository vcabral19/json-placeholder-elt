import requests
import time
import json
import os
import logging
import urllib3
from datetime import datetime

from etl_pipeline.models import User

# Configure logging format to guarantee parsability in observability tools
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# Hardcoding configs for simplicity for now...
API_URL = "https://jsonplaceholder.typicode.com/users"
RAW_DATA_DIR = "data/raw"

# Disable warnings about unverified HTTPS requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
def fetch_data() -> dict:
    try:
        response = requests.get(API_URL, verify=False)
        response.raise_for_status()
        logging.info("Data fetched successfully from API.")
        return response.json()
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return None

def validate_data(data: dict) -> list[dict]:
    valid_users = []
    for record in data:
        try:
            user = User.parse_obj(record)
            valid_users.append(user)
        except Exception as e:
            logging.warning(f"Validation error for record {record.get('id', 'unknown')}: {e}")
    return valid_users

def save_raw_data(data: dict) -> str:
    timestamp = int(time.time())
    # Partition the raw data by date and hour (e.g., data/raw/2025-04-05/14)
    date_path = datetime.now().strftime("%Y-%m-%d/%H")
    dir_path = os.path.join(RAW_DATA_DIR, date_path)
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, f"raw_data_{timestamp}.json")
    try:
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        logging.info(f"Saved raw data to {file_path}")
        return file_path
    except Exception as e:
        logging.error(f"Failed to save raw data: {e}")
        return None

if __name__ == "__main__":
    # You can still run the extractor stand-alone if needed.
    data = fetch_data()
    if data:
        save_raw_data(data)