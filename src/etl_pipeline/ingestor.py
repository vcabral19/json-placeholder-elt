import time
import logging
from sqlmodel import Session, create_engine
from etl_pipeline.models import User
from etl_pipeline.extractor import fetch_data, save_raw_data

DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/etl_db"
engine = create_engine(DATABASE_URL, echo=True)

def create_db_and_tables():
    from sqlmodel import SQLModel
    SQLModel.metadata.create_all(engine)

def process_and_insert(session: Session, record: dict, extraction_ts: int):
    try:
        # Use the new from_api method that accepts the extraction_ts.
        user = User.from_api(record, extraction_ts)
        session.add(user)
    except Exception as e:
        logging.warning(f"Skipping invalid record: {e}")

def run_ingestor():
    from sqlmodel import SQLModel
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        while True:
            data = fetch_data()
            if data:
                # Capture the timestamp once
                extraction_ts = int(time.time())
                # Pass the same timestamp to the raw data writer
                file_path = save_raw_data(data, extraction_ts)
                if file_path:
                    for record in data:
                        process_and_insert(session, record, extraction_ts)
                    session.commit()
            else:
                logging.error("No data fetched from API.")
            time.sleep(30)

if __name__ == "__main__":
    run_ingestor()
