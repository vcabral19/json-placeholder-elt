# ETL Pipeline

A basic ETL pipeline built with Python that:
- **Extracts** data from an external REST API ([JSONPlaceholder](https://jsonplaceholder.typicode.com/users))
- **Transforms** and validates data using SQLModel (integrated with Pydantic)
- **Loads** data into both a datalake (local JSON files) and a PostgreSQL database
- Provides **observability** through structured logging and Prometheus metrics

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Running Locally](#running-locally)
- [Running with Docker](#running-with-docker)
- [Testing](#testing)
- [Monitoring & Logs](#monitoring--logs)
- [Project Structure](#project-structure)
- [Final Notes](#final-notes)

## Requirements

- **Python:** 3.12 (or a compatible version)
- **Poetry:** For dependency management and packaging  
  [Installation instructions](https://python-poetry.org/docs/#installation)
- **Docker:** (optional) for containerized development and deployment
- **PostgreSQL:** (optional for local dev) if you want to run the DB outside Docker

## Installation

1. **Clone the repository:**

```bash
git clone https://github.com/vcabral19/json-placeholder-elt.git
cd json-placeholder-elt
```
2. **Install dependencies using Poetry:**

Make sure Poetry is installed. Then run:
```bash
poetry install
```

## Running Locally
### Without Docker
1. Set up your PostgreSQL database:

If running PostgreSQL locally, ensure it’s running (e.g., on localhost:5432).

Adjust the DATABASE_URL in src/etl_pipeline/ingestor.py or your configuration file accordingly.

2. Run the application:

Use the provided Makefile target:
```bash
make run
```
3. Logs and Metrics:

- Logs are written to logs/etl.log and printed to the console.
- Metrics are exposed on http://localhost:8000/metrics.

## Running with Docker
### Using Docker Compose
1. Start the PostgreSQL container:

```bash
make docker-postgres
```

2. Build and run the application container:

```bash
make docker-app
```

3. Alternatively you can run the whole application with a single compose:

```bash
make docker-all
```

## Testing
Run the test suite with pytest:

```bash
make test
```

This will run all unit and integration tests.

## Monitoring & Logs
### Logs:
- Logs are written to both the console and logs/etl.log
- The logging system is configured to avoid dumping raw JSON data while still reporting key events (such as status codes, file paths, and sizes).

### Metrics:
- Prometheus metrics are exposed at http://localhost:8000/metrics.
Key metrics include:

- api_requests_success_total and api_requests_failure_total
- db_insert_success_total and db_insert_failure_total
- datalake_writes_total
- service_errors_total
- app_starts_total and db_connections_total

## Project Structure
```
etl-pipeline/
├── pyproject.toml
├── poetry.lock
├── Dockerfile
├── docker-compose.yml
├── Makefile
├── README.md
├── config.yaml
├── logs/                # Log files (e.g., etl.log)
├── data/
│   └── raw/             # Partitioned raw JSON files
└── src/
    └── etl_pipeline/
         ├── __init__.py
         ├── extractor.py      # Data extraction & raw file writing
         ├── ingestor.py       # Database ingestion logic
         ├── main.py           # Application entrypoint with graceful shutdown handling
         ├── logger.py         # Logger configuration module
         └── metrics.py        # Prometheus metrics definition & server starter
```
## Final Notes

For production, consider using a migration tool (like Alembic) and externalizing configuration (e.g., via environment variables).

Monitor the /metrics endpoint with Prometheus and build dashboards (e.g., in Grafana) to alert on API, database, and service-level issues.

There is a bunch of non-configurable hardcoded variables around the code, of course refactor this to make seamless the experience of working in dev and moving things in a configurable way to production.