import time
import logging
from sqlmodel import Session, create_engine
from etl_pipeline.models import User, Address, Company, Geo
from etl_pipeline.extractor import fetch_data, save_raw_data

# TODO assembly this based on config
DATABASE_URL = "postgresql://postgres:postgres@postgres:5432/etl_db"
engine = create_engine(DATABASE_URL, echo=True)

def create_db_and_tables():
    from sqlmodel import SQLModel
    SQLModel.metadata.create_all(engine)

def process_and_insert(session: Session, record: dict):
    try:
        # Extract nested data
        geo_data = record["address"]["geo"]
        geo = Geo(**geo_data)
        address = Address(**record["address"], geo=geo)
        company = Company(**record["company"])
        user = User(
            id=record["id"],
            name=record["name"],
            username=record["username"],
            email=record["email"],
            phone=record["phone"],
            website=record["website"],
            address=address,
            company=company,
            raw=record  # Store the full record for auditing purposes
        )
        session.add(user)
    except Exception as e:
        logging.warning(f"Skipping invalid record: {e}")

def run_ingestor():
    create_db_and_tables()
    with Session(engine) as session:
        while True:
            data = fetch_data()
            if data:
                file_path = save_raw_data(data)
                if file_path:
                    for record in data:
                        process_and_insert(session, record)
                    session.commit()
            else:
                logging.error("No data fetched from API.")
            # fetchs every 30 seconds
            time.sleep(30)

if __name__ == "__main__":
    run_ingestor()
