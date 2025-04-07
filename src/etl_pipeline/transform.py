import os
import glob
import json
import csv
import time
from datetime import datetime, timezone

from etl_pipeline.logger import get_logger
from etl_pipeline.metrics import TRANSFORM_SUCCESS, TRANSFORM_FAILURE
from etl_pipeline.models import User, ProcessedCompany, ProcessedUser

logger = get_logger(__name__)

# Base directories and polling interval.
RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
POLL_INTERVAL = 30  # seconds

# Define a mapping from keys (returned by User.transform()) to output model classes.
# Also, each processed model has an attribute "path_name" which we use for output folder.
OUTPUT_MODEL_MAPPING = {
    "processed_company": ProcessedCompany,
    "processed_user": ProcessedUser
}

for key, model_cls in OUTPUT_MODEL_MAPPING.items():
    if not hasattr(model_cls, "path_name"):
        setattr(model_cls, "path_name", model_cls.__name__.lower())

def extract_timestamp(file_path):
    """Extracts the epoch timestamp from a raw file name of the form 'raw_data_{ts}.json'."""
    base = os.path.basename(file_path)
    try:
        ts_str = base[len("raw_data_"):-len(".json")]
        return int(ts_str)
    except Exception as e:
        logger.error("Failed to extract timestamp from %s: %s", file_path, e)
        return None

def get_unprocessed_raw_files(raw_dir=RAW_DIR, processed_dir=PROCESSED_DIR):
    """
    Scans raw_dir recursively for raw_data_*.json files.
    For each file, it checks that for every output model in OUTPUT_MODEL_MAPPING,
    a corresponding CSV file exists in:
        processed_dir / <model.path_name> / <partition>/processed_<model.path_name>_<ts>.csv
    Returns a list of (file_path, ts) for files that are not yet fully processed.
    """
    pattern = os.path.join(raw_dir, "**", "raw_data_*.json")
    files = glob.glob(pattern, recursive=True)
    unprocessed = []
    for file in files:
        ts = extract_timestamp(file)
        if ts is None:
            continue
        # Partition based on UTC: YYYY-MM-DD/HH
        partition = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d/%H")
        all_exist = True
        for model_cls in OUTPUT_MODEL_MAPPING.values():
            output_file = os.path.join(
                processed_dir,
                model_cls.path_name,
                partition,
                f"processed_{model_cls.path_name}_{ts}.csv"
            )
            if not os.path.exists(output_file):
                all_exist = False
                break
        if not all_exist:
            unprocessed.append((file, ts))
    return unprocessed

def generic_write_csv(model_cls, instances, extraction_ts, processed_dir=PROCESSED_DIR):
    """
    Writes a list of model instances (processed) to a CSV file.
    The output folder is determined by:
      processed_dir / model_cls.path_name / <partition>
    where partition is derived from extraction_ts (formatted as YYYY-MM-DD/HH in UTC).
    The file is named:
      processed_<model_cls.path_name>_<extraction_ts>.csv
    The CSV headers are determined from the model's __fields__.
    """
    partition = datetime.fromtimestamp(extraction_ts, tz=timezone.utc).strftime("%Y-%m-%d/%H")
    folder = os.path.join(processed_dir, model_cls.path_name, partition)
    os.makedirs(folder, exist_ok=True)
    file_path = os.path.join(folder, f"processed_{model_cls.path_name}_{extraction_ts}.csv")
    fieldnames = list(model_cls.__fields__.keys())
    file_exists = os.path.exists(file_path)
    try:
        with open(file_path, "a", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            for instance in instances:
                writer.writerow(instance.dict())
        logger.info("Wrote %d records to %s", len(instances), file_path)
    except Exception as e:
        logger.error("Error writing CSV for %s: %s", model_cls.__name__, e)
        TRANSFORM_FAILURE.inc()

def generic_transform(raw_file, extraction_ts, transformation_fn):
    """
    Generic transformation process:
      - Reads a raw JSON file.
      - For each record, applies transformation_fn(record, extraction_iso)
        to obtain a dict mapping output keys (e.g. "processed_company", "processed_user")
        to processed model instances.
      - Aggregates processed instances by key.
      - For each key in the aggregated dict, writes the output to CSV using generic_write_csv.
    """
    try:
        with open(raw_file, "r") as f:
            data = json.load(f)
    except Exception as e:
        logger.error("Error reading raw file %s: %s", raw_file, e)
        TRANSFORM_FAILURE.inc()
        return

    # Create a consistent extraction timestamp string in ISO 8601 UTC.
    extraction_iso = datetime.fromtimestamp(extraction_ts, tz=timezone.utc).isoformat()

    # key -> list of processed model instances.
    # "processed_company" -> ProcessedCompany instance
    aggregated = {}
    for record in data:
        try:
            result = transformation_fn(record, extraction_iso)
            # transformation_fn returns a dict mapping keys to processed instances.
            for key, instance in result.items():
                aggregated.setdefault(key, []).append(instance)
        except Exception as e:
            logger.error("Error transforming record %s: %s", record.get("id"), e)
            TRANSFORM_FAILURE.inc()
            continue

    for key, instances in aggregated.items():
        if key not in OUTPUT_MODEL_MAPPING:
            logger.error("No output mapping for key: %s", key)
            continue
        model_cls = OUTPUT_MODEL_MAPPING[key]
        generic_write_csv(model_cls, instances, extraction_ts, processed_dir=PROCESSED_DIR)

    logger.info("Transformed raw file %s with timestamp %d", raw_file, extraction_ts)
    TRANSFORM_SUCCESS.inc()

def run_transformer(transformation_fn, raw_dir=RAW_DIR, processed_dir=PROCESSED_DIR, poll_interval=POLL_INTERVAL):
    """
    Continuous polling: every poll_interval seconds, it scans raw_dir for new raw files
    that have not been processed (based on the output files for all output models),
    and applies generic_transform to each.
    The transformation_fn is a function with signature:
         f(record: dict, extraction_iso: str) -> dict
    which returns a mapping from output keys to processed model instances.
    """
    logger.info("Starting continuous transformer process.")
    while True:
        unprocessed = get_unprocessed_raw_files(raw_dir, processed_dir)
        if unprocessed:
            logger.info("Found %d unprocessed raw file(s).", len(unprocessed))
            for raw_file, ts in unprocessed:
                logger.info("Processing raw file: %s", raw_file)
                generic_transform(raw_file, ts, transformation_fn)
        else:
            logger.info("No new raw files to process.")
        time.sleep(poll_interval)

def default_transformation_fn(record, extraction_iso):
    """
    Uses the centralized User model to parse the raw record (via User.from_api)
    and then calls its transform() method.
    The transform() method should return a dict mapping keys to processed instances.
    """
    # We need the epoch timestamp; we can parse extraction_iso back to a timestamp.
    ts = int(datetime.fromisoformat(extraction_iso).timestamp())
    user_obj = User.from_api(record, ts)
    return user_obj.transform(extraction_iso)

if __name__ == "__main__":
    run_transformer(default_transformation_fn)
