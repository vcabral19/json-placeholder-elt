import argparse
import sys
import atexit
import signal
from etl_pipeline.logger import get_logger
from etl_pipeline.metrics import SERVICE_ERRORS, APP_STARTS
from etl_pipeline.ingestor import run_ingestor
from etl_pipeline.transform import default_transformation_fn, run_transformer

logger = get_logger(__name__)
service_error_flag = False

def main():
    global service_error_flag
    parser = argparse.ArgumentParser(description="ETL Pipeline Main Entrypoint")
    parser.add_argument(
        '--mode',
        choices=['ingestor', 'transformer'],
        default='ingestor',
        help="Mode to run the application: 'ingestor' for data ingestion or 'transformer' for data transformation."
    )
    args = parser.parse_args()
    
    APP_STARTS.inc()
    logger.info("ETL Application started in %s mode.", args.mode)
    
    try:
        if args.mode == "ingestor":
            run_ingestor()
        elif args.mode == "transformer":
            run_transformer(default_transformation_fn)
    except Exception as e:
        service_error_flag = True
        logger.error("Unhandled exception in %s mode: %s", args.mode, e)
        raise

def on_service_exit():
    if service_error_flag:
        logger.error("Service ended execution due to an error.")
        SERVICE_ERRORS.inc()
    else:
        logger.info("Service ended execution normally.")

def handle_signal(signum, frame):
    logger.info("Received termination signal (%s); shutting down gracefully.", signum)
    sys.exit(0)


atexit.register(on_service_exit)
signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)

if __name__ == "__main__":
    main()
