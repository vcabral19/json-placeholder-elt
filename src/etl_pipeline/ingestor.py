import time
import logging
from sqlmodel import Session, create_engine
from etl_pipeline.models import User
from etl_pipeline.extractor import fetch_data, save_raw_data

DATABASE_URL = "postgresql://postgres:postgres@postgres:5432/etl_db"
engine = create_engine(DATABASE_URL, echo=True)

def create_db_and_tables():
    from sqlmodel import SQLModel
    SQLModel.metadata.create_all(engine)

def process_and_insert(session: Session, record: dict):
    try:
        # Use the from_api method to transform the record into a User with nested objects.
        user = User.from_api(record)
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
            time.sleep(30)

if __name__ == "__main__":
    run_ingestor()
