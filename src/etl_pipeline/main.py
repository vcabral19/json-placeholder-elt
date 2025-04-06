import sys
import atexit
import signal
from etl_pipeline.logger import get_logger
from etl_pipeline.metrics import SERVICE_ERRORS, APP_STARTS
from etl_pipeline.ingestor import run_ingestor

logger = get_logger(__name__)

# Global flag to track if an error has occurred.
service_error_flag = False

def main():
    global service_error_flag
    try:
        APP_STARTS.inc()
        logger.info("ETL Application started.")
        # run_ingestor contains the main ingestion loop.
        run_ingestor()
    except Exception as e:
        service_error_flag = True
        logger.error("Unhandled exception occurred: %s", e)
        raise

def on_service_exit():
    if service_error_flag:
        logger.error("Service ended execution")
        SERVICE_ERRORS.inc()
    else:
        logger.info("Service ended execution normally.")

def handle_signal(signum, frame):
    # TODO we can play with other logics for this
    global service_error_flag
    logger.error("Received termination signal (%s), shutting down.", signum)
    service_error_flag = True
    sys.exit(1)

# Register exit and signal handlers.
atexit.register(on_service_exit)
signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)

if __name__ == "__main__":
    main()
