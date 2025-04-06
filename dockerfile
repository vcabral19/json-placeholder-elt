FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Poetry via pip
RUN pip install poetry

WORKDIR /app

# Copy source code and other files
COPY src/ ./src/
COPY data/ ./data/
COPY config.yaml ./

# Copy dependency files
COPY pyproject.toml poetry.lock* ./

# Set PYTHONPATH so that the source directory is included.
ENV PYTHONPATH="/app/src:$PYTHONPATH"

# Install production dependencies
RUN poetry install --without dev --no-root

CMD ["poetry", "run", "python", "-m", "etl_pipeline.main"]
