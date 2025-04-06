.PHONY: install
install:
	poetry install

.PHONY: test
test:
	poetry run pytest
.PHONY: run docker-up docker-app docker-all

# Run the application locally (without Docker)
run:
	poetry run python -m etl_pipeline.main

# Start only the PostgreSQL container via Docker Compose
docker-up:
	docker-compose up -d postgres

# Build and run the app container standalone (uses --network=host)
docker-app:
	docker build -t etl-pipeline .
	docker run --rm --network=host etl-pipeline

# Start the entire application (PostgreSQL and the ETL app) via Docker Compose
docker-all:
	docker-compose up --build
