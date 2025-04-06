import requests
import time
import json
import os
import logging
from datetime import datetime
import urllib3

from etl_pipeline.models import User

# Configure logging format to guarantee parsability in observability tools
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# Disable warnings about unverified HTTPS requests (development only)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

API_URL = "https://jsonplaceholder.typicode.com/users"

def fetch_data():
    try:
        response = requests.get(API_URL, verify=False)
        response.raise_for_status()
        logging.info("Data fetched successfully from API.")
        return response.json()
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return None

def validate_data(data, extraction_ts: int = 0):
    valid_users = []
    for record in data:
        try:
            # Pass the extraction timestamp to the from_api method.
            user = User.from_api(record, extraction_ts)
            valid_users.append(user)
        except Exception as e:
            logging.warning(f"Validation error for record {record.get('id', 'unknown')}: {e}")
    return valid_users

def save_raw_data(data, extraction_ts: int):
    from datetime import datetime
    # Use the provided extraction_ts to build the file path
    date_path = datetime.fromtimestamp(extraction_ts).strftime("%Y-%m-%d/%H")
    dir_path = os.path.join("data", "raw", date_path)
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, f"raw_data_{extraction_ts}.json")
    try:
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        logging.info(f"Saved raw data to {file_path}")
        return file_path
    except Exception as e:
        logging.error(f"Failed to save raw data: {e}")
        return None


if __name__ == "__main__":
    data = fetch_data()
    if data:
        save_raw_data(data)
