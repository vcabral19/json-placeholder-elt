import json
from datetime import datetime
from pathlib import Path

import pytest

from etl_pipeline import extractor
from etl_pipeline.models import User

# A sample valid user record, as described in our schema.
valid_user = {
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

# A sample invalid record (e.g., invalid type for 'id').
invalid_user = {
    "id": "not a number",
    "name": "Invalid User"
}


def test_validate_data():
    """
    Test that the validation function correctly processes valid data
    and logs a warning (or skips) for invalid records.
    """
    sample_data = [valid_user, invalid_user]
    valid_users = extractor.validate_data(sample_data)
    
    # Only one valid user should pass the validation.
    assert len(valid_users) == 1
    
    # Check that the valid user's id matches the expected value.
    user: User = valid_users[0]
    assert user.id == 1


def test_save_raw_data(tmp_path, monkeypatch):
    """
    Test that the raw data is saved in the proper directory structure and
    that the file content matches the input data.
    """
    # Set up a temporary directory to override RAW_DATA_DIR
    test_raw_dir = tmp_path / "raw_data"
    monkeypatch.setattr(extractor, "RAW_DATA_DIR", str(test_raw_dir))
    
    sample_data = [valid_user]  # Simple sample list of user records.
    extractor.save_raw_data(sample_data)
    
    # Build the expected partition path (date/hour) based on current time.
    date_hour = datetime.now().strftime("%Y-%m-%d/%H")
    expected_dir = test_raw_dir / date_hour
    assert expected_dir.is_dir(), f"Expected directory {expected_dir} does not exist."
    
    # Look for the file matching our naming pattern.
    raw_files = list(expected_dir.glob("raw_data_*.json"))
    assert len(raw_files) == 1, "Expected one raw data file in the partition directory."
    
    # Read the file content and verify it matches our sample data.
    with open(raw_files[0], "r") as file:
        data = json.load(file)
    assert data == sample_data, "The file content does not match the input data."
