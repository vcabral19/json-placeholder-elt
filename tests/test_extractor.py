from etl_pipeline import extractor
from etl_pipeline.models import User

# A sample valid user record.
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

def test_validate_data():
    sample_data = [valid_user]
    # Optionally pass a dummy extraction timestamp (default is 0 if not specified).
    valid_users = extractor.validate_data(sample_data, extraction_ts=0)
    assert len(valid_users) == 1, f"Expected 1 valid user, got {len(valid_users)}"
    user: User = valid_users[0]
    assert user.user_id == 1
