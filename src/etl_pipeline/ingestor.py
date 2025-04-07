import os
import time
from sqlmodel import Session, create_engine
from etl_pipeline.models import User
from etl_pipeline.extractor import fetch_data, save_raw_data
from etl_pipeline.logger import get_logger
from etl_pipeline.metrics import DB_INSERT_SUCCESS, DB_INSERT_FAILURE, DB_CONNECTIONS, APP_STARTS, SERVICE_ERRORS
from etl_pipeline.metrics import start_metrics_server

logger = get_logger(__name__)


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/etl_db")
engine = create_engine(DATABASE_URL, echo=True)

# Start the metrics HTTP server on port 8000
start_metrics_server(port=8000)

def create_db_and_tables():
    from sqlmodel import SQLModel
    SQLModel.metadata.create_all(engine)
    logger.info("Database tables created (or verified existing).")

def process_and_insert(session: Session, record: dict, extraction_ts: int):
    try:
        user = User.from_api(record, extraction_ts)
        session.add(user)
        logger.info(f"Attempting to insert record for user_id {record['id']} with extraction_ts {extraction_ts}")
    except Exception as e:
        logger.error(f"Error processing record for user_id {record.get('id')}: {e}")
        DB_INSERT_FAILURE.inc()

def run_ingestor():
    APP_STARTS.inc()
    logger.info("ETL Application started.")
    create_db_and_tables()
    logger.info("Connecting to the database...")
    with Session(engine) as session:
        DB_CONNECTIONS.inc()
        while True:
            data = fetch_data()
            if data:
                extraction_ts = int(time.time())
                file_path = save_raw_data(data, extraction_ts)
                if file_path:
                    for record in data:
                        process_and_insert(session, record, extraction_ts)
                    try:
                        session.commit()
                        logger.info("Database commit successful.")
                        DB_INSERT_SUCCESS.inc(len(data))
                    except Exception as e:
                        logger.error(f"Database commit failed: {e}")
                        DB_INSERT_FAILURE.inc()
            else:
                logger.error("No data fetched from API.")
            time.sleep(30)

if __name__ == "__main__":
    run_ingestor()
    logger.info("Application killed")
