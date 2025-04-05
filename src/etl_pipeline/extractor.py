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

def fetch_data():
    try:
        # In a production env we expect our server to have a valid cert
        response = requests.get(API_URL, verify=False)
        response.raise_for_status()
        logging.info("Data fetched successfully from API.")
        return response.json()
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return None

def validate_data(data):
    valid_users = []
    for record in data:
        try:
            user = User.parse_obj(record)
            valid_users.append(user)
        except Exception as e:
            logging.warning(f"Validation error for record {record.get('id', 'unknown')}: {e}")
    return valid_users

def save_raw_data(data):
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
    except Exception as e:
        logging.error(f"Failed to save raw data: {e}")

def insert_into_db(valid_users):
    # Placeholder for future PostgreSQL insertion logic.
    # Later, we will connect to the DB (via Docker container) and insert the validated data.
    logging.info(f"Inserting {len(valid_users)} records into the database (not implemented yet).")

def run_extraction():
    while True:
        data = fetch_data()
        if data:
            valid_users = validate_data(data)
            insert_into_db(valid_users)  # Future DB insertion
            save_raw_data(data)
        time.sleep(30)

if __name__ == "__main__":
    run_extraction()
