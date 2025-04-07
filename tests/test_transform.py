import os
import json
import csv
from datetime import datetime, timezone

import pytest

from etl_pipeline import transform
from etl_pipeline.models import ProcessedCompany, ProcessedUser

# Sample raw record matching the expected API schema.
sample_raw_data = [
    {
        "id": 1,
        "name": "Leanne Graham",
        "username": "Bret",
        "email": "Sincere@april.biz",
        "address": {
            "street": "Kulas Light",
            "suite": "Apt. 556",
            "city": "Gwenborough",
            "zipcode": "92998-3874",
            "geo": {
                "lat": "-37.3159",
                "lng": "81.1496"
            }
        },
        "phone": "1-770-736-8031 x56442",
        "website": "hildegard.org",
        "company": {
            "name": "Romaguera-Crona",
            "catchPhrase": "Multi-layered client-server neural-net",
            "bs": "harness real-time e-markets"
        }
    }
]

@pytest.fixture
def setup_dirs(tmp_path, monkeypatch):
    """
    Create temporary directories for raw and processed data and override the transform module's
    RAW_DIR and PROCESSED_DIR values.
    """
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    raw_dir.mkdir()
    processed_dir.mkdir()
    monkeypatch.setattr(transform, "RAW_DIR", str(raw_dir))
    monkeypatch.setattr(transform, "PROCESSED_DIR", str(processed_dir))
    return raw_dir, processed_dir

def test_extract_timestamp(tmp_path):
    file_path = tmp_path / "raw_data_1234567890.json"
    file_path.write_text("[]")
    ts = transform.extract_timestamp(str(file_path))
    assert ts == 1234567890

def test_generic_transform_and_write(setup_dirs):
    raw_dir, processed_dir = setup_dirs
    extraction_ts = 1234567890
    # Partition based on UTC (format: YYYY-MM-DD/HH)
    partition = datetime.fromtimestamp(extraction_ts, tz=timezone.utc).strftime("%Y-%m-%d/%H")
    
    # Write sample raw data into a file.
    raw_file = raw_dir / f"raw_data_{extraction_ts}.json"
    with raw_file.open("w") as f:
        json.dump(sample_raw_data, f)
    
    # Run generic transformation using the default transformation function.
    transform.generic_transform(str(raw_file), extraction_ts, transform.default_transformation_fn)
    
    # Expected output paths:
    company_output_file = os.path.join(
        str(processed_dir),
        ProcessedCompany.path_name,  # Expected to be "processedcompany"
        partition,
        f"processed_{ProcessedCompany.path_name}_{extraction_ts}.csv"
    )
    user_output_file = os.path.join(
        str(processed_dir),
        ProcessedUser.path_name,  # Expected to be "processeduser"
        partition,
        f"processed_{ProcessedUser.path_name}_{extraction_ts}.csv"
    )
    
    assert os.path.exists(company_output_file), f"Expected {company_output_file} to exist"
    assert os.path.exists(user_output_file), f"Expected {user_output_file} to exist"
    
    # Verify CSV headers for company output.
    with open(company_output_file, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        expected_headers = list(ProcessedCompany.__fields__.keys())
        assert reader.fieldnames == expected_headers, f"Company CSV headers mismatch: {reader.fieldnames} != {expected_headers}"
    
    # Verify CSV headers for user output.
    with open(user_output_file, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        expected_headers = list(ProcessedUser.__fields__.keys())
        assert reader.fieldnames == expected_headers, f"User CSV headers mismatch: {reader.fieldnames} != {expected_headers}"

def test_get_unprocessed_raw_files(setup_dirs):
    raw_dir, processed_dir = setup_dirs
    extraction_ts1 = 1234567890
    extraction_ts2 = 1234567891
    file1 = raw_dir / f"raw_data_{extraction_ts1}.json"
    file2 = raw_dir / f"raw_data_{extraction_ts2}.json"
    file1.write_text("[]")
    file2.write_text("[]")
    
    partition1 = datetime.fromtimestamp(extraction_ts1, tz=timezone.utc).strftime("%Y-%m-%d/%H")
    # Simulate processed outputs for extraction_ts1 for both models.
    for model_cls in [ProcessedCompany, ProcessedUser]:
        out_dir = os.path.join(str(processed_dir), model_cls.path_name, partition1)
        os.makedirs(out_dir, exist_ok=True)
        dummy_file = os.path.join(out_dir, f"processed_{model_cls.path_name}_{extraction_ts1}.csv")
        with open(dummy_file, "w") as f:
            f.write("dummy")
    
    unprocessed = transform.get_unprocessed_raw_files(str(raw_dir), str(processed_dir))
    ts_values = [ts for _, ts in unprocessed]
    assert extraction_ts2 in ts_values, "File2 should be unprocessed"
    assert extraction_ts1 not in ts_values, "File1 should be marked as processed"
