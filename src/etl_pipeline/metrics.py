from prometheus_client import Counter, start_http_server

API_REQUESTS_SUCCESS = Counter("api_requests_success", "Number of successful API requests")
API_REQUESTS_FAILURE = Counter("api_requests_failure", "Number of failed API requests")
TRANSFORMATION_ERRORS = Counter("transformation_errors", "Number of transformation errors")
DB_INSERT_SUCCESS = Counter("db_insert_success", "Number of successful database inserts")
DB_INSERT_FAILURE = Counter("db_insert_failure", "Number of failed database inserts")
SERVICE_ERRORS = Counter("service_errors", "Number of service-level errors")
APP_STARTS = Counter("app_starts", "Number of times the application has started")
DB_CONNECTIONS = Counter("db_connections", "Number of times a connection to the database was established")
DATALAKE_WRITES = Counter("datalake_writes", "Number of datalake file writes")


def start_metrics_server(port: int = 8000):
    """
    Start an HTTP server that exposes Prometheus metrics on the given port.
    """
    start_http_server(port)
