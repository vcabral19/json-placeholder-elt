.PHONY: install
install:
	poetry install

.PHONY: test
test:
	poetry run pytest

.PHONY: run docker-up docker-app docker-transform docker-all

# Run the application locally (without Docker)
run:
	poetry run python -m etl_pipeline.main --mode ingestor

# Start only the PostgreSQL container via Docker Compose
docker-postgres:
	docker-compose up -d postgres

# Build and run the app container standalone (uses --network=host)
docker-app:
	docker build -t etl-pipeline .
	docker run --rm --network=host etl-pipeline

# Run the transformer container via Docker Compose
docker-transform:
	docker-compose up etl_transformer

# Start the entire application (PostgreSQL and the ETL app) via Docker Compose
docker-all:
	docker-compose up --build
