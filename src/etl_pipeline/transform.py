import os
import glob
import json
import csv
import time
from datetime import datetime, timezone

from etl_pipeline.logger import get_logger
from etl_pipeline.metrics import TRANSFORM_SUCCESS, TRANSFORM_FAILURE

logger = get_logger(__name__)

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
POLL_INTERVAL = 30  # seconds between scans

def extract_timestamp(file_path):
    """Extracts the epoch timestamp from a raw file name."""
    base = os.path.basename(file_path)
    try:
        ts_str = base[len("raw_data_"):-len(".json")]
        return int(ts_str)
    except Exception as e:
        logger.error("Failed to extract timestamp from %s: %s", file_path, e)
        return None

def get_unprocessed_raw_files(raw_dir=RAW_DIR, processed_dir=PROCESSED_DIR):
    """
    Scans recursively under raw_dir for raw_data_*.json files and returns a list of
    (file_path, timestamp) tuples for those files that don't have corresponding processed CSVs.
    """
    pattern = os.path.join(raw_dir, "**", "raw_data_*.json")
    files = glob.glob(pattern, recursive=True)
    unprocessed = []
    for file in files:
        ts = extract_timestamp(file)
        if ts is None:
            continue
        # Determine partition (same as transformation logic)
        partition = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d/%H")
        # Expected output files in processed folder for both company and user CSVs:
        company_file = os.path.join(processed_dir, "company", partition, f"processed_company_{ts}.csv")
        user_file = os.path.join(processed_dir, "user", partition, f"processed_user_{ts}.csv")
        # If either file is missing, consider the raw file unprocessed.
        if not (os.path.exists(company_file) and os.path.exists(user_file)):
            unprocessed.append((file, ts))
    return unprocessed

def transform_raw_file(raw_file, extraction_ts):
    """
    Transforms a single raw file into CSV files.
    Writes:
      - Company data to data/processed/company/YYYY-MM-DD/HH/processed_company_{timestamp}.csv
      - User data to data/processed/user/YYYY-MM-DD/HH/processed_user_{timestamp}.csv
    Timestamps are converted to ISO 8601 UTC.
    """
    # Determine partition folder from extraction_ts using timezone-aware conversion
    partition = datetime.fromtimestamp(extraction_ts, tz=timezone.utc).strftime("%Y-%m-%d/%H")
    company_dir = os.path.join(PROCESSED_DIR, "company", partition)
    user_dir = os.path.join(PROCESSED_DIR, "user", partition)
    os.makedirs(company_dir, exist_ok=True)
    os.makedirs(user_dir, exist_ok=True)
    
    # Load raw data from JSON (unchanged)
    try:
        with open(raw_file, "r") as f:
            data = json.load(f)
    except Exception as e:
        logger.error("Error loading JSON from %s: %s", raw_file, e)
        TRANSFORM_FAILURE.inc()
        return

    companies = {}  # key: company name, value: dict with company data
    users = []      # list of user records
    company_id_counter = 1

    # Convert extraction_ts to ISO 8601 UTC format
    extraction_iso = datetime.fromtimestamp(extraction_ts, tz=timezone.utc).isoformat()

    for record in data:
        comp = record.get("company", {})
        comp_name = comp.get("name", "")
        if comp_name and comp_name not in companies:
            companies[comp_name] = {
                "company_id": company_id_counter,
                "name": comp.get("name", ""),
                "catchPhrase": comp.get("catchPhrase", ""),
                "bs": comp.get("bs", ""),
                "extraction_ts": extraction_iso
            }
            company_id_counter += 1

        user_record = {
            "user_id": record.get("id"),
            "username": record.get("username", ""),
            "phone": record.get("phone", ""),
            "email": record.get("email", ""),
            "website": record.get("website", ""),
            "company_id": companies.get(comp_name, {}).get("company_id"),
            "extraction_ts": extraction_iso
        }
        users.append(user_record)

    company_csv = os.path.join(company_dir, f"processed_company_{extraction_ts}.csv")
    user_csv = os.path.join(user_dir, f"processed_user_{extraction_ts}.csv")

    def write_csv(file_path, fieldnames, rows):
        file_exists = os.path.exists(file_path)
        try:
            with open(file_path, "a", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                if not file_exists:
                    writer.writeheader()
                for row in rows:
                    writer.writerow(row)
        except Exception as e:
            logger.error("Error writing CSV to %s: %s", file_path, e)
            TRANSFORM_FAILURE.inc()

    company_fieldnames = ["company_id", "name", "catchPhrase", "bs", "extraction_ts"]
    user_fieldnames = ["user_id", "username", "phone", "email", "website", "company_id", "extraction_ts"]

    write_csv(company_csv, company_fieldnames, list(companies.values()))
    write_csv(user_csv, user_fieldnames, users)

    logger.info("Transformed raw file %s into processed files: %s (company) and %s (user)",
                raw_file, company_csv, user_csv)
    TRANSFORM_SUCCESS.inc()

def run_transformer():
    """
    Continuously polls the raw data directory for unprocessed raw files,
    transforms them into CSV files in the processed directory, and logs progress.
    """
    logger.info("Starting continuous transformation process.")
    while True:
        unprocessed_files = get_unprocessed_raw_files()
        if unprocessed_files:
            logger.info("Found %d unprocessed raw file(s).", len(unprocessed_files))
            for raw_file, ts in unprocessed_files:
                logger.info("Processing raw file: %s", raw_file)
                transform_raw_file(raw_file, ts)
        else:
            logger.info("No new raw files to process.")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    run_transformer()
