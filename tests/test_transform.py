import json
import csv
from datetime import datetime, timezone

import pytest

from etl_pipeline import transform

@pytest.fixture
def sample_raw_data():
    # A sample raw record matching our API schema
    return [{
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
    }]

def test_transform_raw_file(tmp_path, monkeypatch, sample_raw_data):
    # Create temporary directories for raw and processed data.
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    raw_dir.mkdir()
    processed_dir.mkdir()
    
    # Override the module-level RAW_DIR and PROCESSED_DIR variables.
    monkeypatch.setattr(transform, "RAW_DIR", str(raw_dir))
    monkeypatch.setattr(transform, "PROCESSED_DIR", str(processed_dir))
    
    # Use a dummy extraction timestamp.
    extraction_ts = 1234567890
    raw_filename = f"raw_data_{extraction_ts}.json"
    raw_file = raw_dir / raw_filename
    
    # Write the sample raw data to the raw file.
    with raw_file.open("w") as f:
        json.dump(sample_raw_data, f)
    
    # Invoke the transformation on the raw file.
    transform.transform_raw_file(str(raw_file), extraction_ts)
    
    # Determine the partition folder based on extraction timestamp (YYYY-MM-DD/HH in UTC)
    partition = datetime.fromtimestamp(extraction_ts, tz=timezone.utc).strftime("%Y-%m-%d/%H")
    
    company_csv = processed_dir / "company" / partition / f"processed_company_{extraction_ts}.csv"
    user_csv = processed_dir / "user" / partition / f"processed_user_{extraction_ts}.csv"
    
    # Check that both processed CSV files exist.
    assert company_csv.exists(), f"Expected company CSV file at {company_csv}"
    assert user_csv.exists(), f"Expected user CSV file at {user_csv}"
    
    # Check that the company CSV file has the expected headers.
    with company_csv.open() as csvfile:
        reader = csv.DictReader(csvfile)
        expected_headers = ["company_id", "name", "catchPhrase", "bs", "extraction_ts"]
        assert reader.fieldnames == expected_headers, f"Company CSV headers mismatch. Expected {expected_headers}, got {reader.fieldnames}"
    
    # Check that the user CSV file has the expected headers.
    with user_csv.open() as csvfile:
        reader = csv.DictReader(csvfile)
        expected_headers = ["user_id", "username", "phone", "email", "website", "company_id", "extraction_ts"]
        assert reader.fieldnames == expected_headers, f"User CSV headers mismatch. Expected {expected_headers}, got {reader.fieldnames}"
