.PHONY: install
install:
	poetry install

.PHONY: run
run:
	poetry run python -m etl_pipeline.extractor

.PHONY: test
test:
	poetry run pytest
