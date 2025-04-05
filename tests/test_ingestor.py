import pytest
from sqlmodel import Session, create_engine, select
from etl_pipeline.ingestor import process_and_insert
from etl_pipeline.models import User

# A valid user record resembling data from the API.
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

@pytest.fixture
def session():
    engine = create_engine("sqlite://", echo=False)
    from sqlmodel import SQLModel
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session

def test_process_and_insert(session):
    process_and_insert(session, valid_user)
    session.commit()
    
    statement = select(User).where(User.id == valid_user["id"])
    result = session.exec(statement)
    user = result.first()
    
    assert user is not None, "User should be inserted into the DB."
    assert user.name == valid_user["name"], "User name should match."
    
    # Verify nested objects.
    assert user.address is not None, "Address should be inserted."
    assert user.address.street == valid_user["address"]["street"], "Street should match."
    
    assert user.address.geo is not None, "Geo should be inserted."
    assert user.address.geo.lat == valid_user["address"]["geo"]["lat"], "Geo latitude should match."
    
    assert user.company is not None, "Company should be inserted."
    assert user.company.name == valid_user["company"]["name"], "Company name should match."
