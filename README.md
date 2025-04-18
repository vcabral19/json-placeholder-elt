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

3. Build and run transform:

```bash
make docker-transform
```

4. Alternatively you can run the whole application with a single compose:

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
### Design choices
I've tried to leave the tier1 (raw) data layer as safe as free of logic as possible so in the scenario of incidents we minimize data loss. Data can always be reprocessed from raw as soon as we have it in a accesible organized persistent data layer (such as a blob storage).

That's part of the reason why I separated tier2 (processed) data application from it. The other part is because we can other transformations consuming from this same extractions. Decoupling makes sense for maintaiance of the logic and of the applications operations.

The logic shared between those 2 applications can be changed in a more organized way like this as well. This way I tried to make use of pydantic to maintain the models of the raw data, the tables in postgres and the processed .csvs.

Finally for the transformations I brough the users and the companies to the datalake separately to minimize "PII" exposure. People can play with the company data for marketing campaign but for the actual "user" table we can restrict access to the .csv/data to the systems/people that really needs to see it.

### Dataflow
```pgsql
[API: jsonplaceholder]
         │
         ▼
[Extractor/Ingestor Module]
  • Fetches & validates API data (using SQLModel/Pydantic)
  • Writes raw JSON files to data/raw (partitioned by UTC date/hour)
  • Inserts data into PostgreSQL
         │
         ▼
   [Raw Data Storage]
         │
         ▼
[Transformer Module (continuous)]
  • Polls data/raw for new files
  • Reads raw JSON and calls User.from_api() then User.transform()
  • Aggregates output into ProcessedCompany and ProcessedUser objects
  • Writes processed CSV files to data/processed (partitioned by UTC date/hour)
```
## Cloud migration

- Blob storage such as s3 or GCS instead of the local file system for raw and processed data storage
- Processed layer could use more sophisticated tecnologies such as iceberg or deltalake for getting out of the box ACID capabilities to the datalake
- We can orchestrate the containers in kubernetes, or any other container deployment service in cloud like Amazon ECS
- s3 could for example write new write events to a SQS so transfomer can consume files to guarantee processed exactly once from there
- Data can be consumed from the tier2 blob layer from spark cluster, bigquery, dashboards etc

## Final Notes

For production we need something to externalize configurations (I added the config.yaml here but never used) (e.g., via environment variables).

Monitor the /metrics endpoint with Prometheus and build dashboards (e.g., in Grafana) to alert on API, database, and service-level issues.

There is a bunch of non-configurable hardcoded variables around the code, of course refactor this to make seamless the experience of working in dev and moving things in a configurable way to production.

Transform can be refactored to use a queue system to get events of new written files from it instead of pooling and listing all files on both sides every 30 seconds (which is obviously not scalable).

We can investigate good strategies to deal with postgres in the future based on usage pattern, most likely partition the tables by the timestamp and index it by the id of the entities.
