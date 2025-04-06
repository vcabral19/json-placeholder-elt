import requests
import time
import json
import os
import logging
from datetime import datetime
import urllib3

from etl_pipeline.logger import get_logger
from etl_pipeline.metrics import API_REQUESTS_SUCCESS, API_REQUESTS_FAILURE
from etl_pipeline.models import User

logger = get_logger(__name__)

# Disable warnings about unverified HTTPS requests (development only)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

API_URL = "https://jsonplaceholder.typicode.com/users"

def fetch_data():
    try:
        response = requests.get(API_URL, verify=False)
        byte_size = len(response.content)
        if response.status_code == 200:
            logger.info(f"GET {API_URL} returned {response.status_code} with {byte_size} bytes")
            API_REQUESTS_SUCCESS.inc()
            return response.json()
        else:
            logger.error(f"GET {API_URL} failed with status {response.status_code} and {byte_size} bytes")
            API_REQUESTS_FAILURE.inc()
            return None
    except Exception as e:
        logger.error(f"Error fetching data from {API_URL}: {e}")
        API_REQUESTS_FAILURE.inc()
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

def save_raw_data(data, extraction_ts: int) -> str:
    date_path = datetime.fromtimestamp(extraction_ts).strftime("%Y-%m-%d/%H")
    dir_path = os.path.join("data", "raw", date_path)
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, f"raw_data_{extraction_ts}.json")
    try:
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        file_size = os.path.getsize(file_path)
        logger.info(f"Saved raw data to {file_path} (Partition: {date_path}, {file_size} bytes)")
        from etl_pipeline.metrics import DATALAKE_WRITES
        DATALAKE_WRITES.inc()
        return file_path
    except Exception as e:
        logger.error(f"Failed to save raw data to {file_path}: {e}")
        return None


if __name__ == "__main__":
    data = fetch_data()
    if data:
        save_raw_data(data)
