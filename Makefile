.PHONY: install
install:
	poetry install

.PHONY: run-ingestor
run-ingestor:
	poetry run python -m etl_pipeline.ingestor

.PHONY: run-extractor
run:
	poetry run python -m etl_pipeline.extractor

.PHONY: test
test:
	poetry run pytest

.PHONY: run docker-up docker-app
docker-up:
	docker-compose up -d

docker-app:
	docker build -t etl-pipeline .
	docker run --rm --network=host etl-pipeline