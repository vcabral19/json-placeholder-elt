# Dockerfile
FROM python:3.12-slim

# Set environment variables for Poetry
ENV POETRY_VERSION=1.5.1

# Install Poetry and build tools
RUN apt-get update && apt-get install -y curl build-essential libpq-dev && \
    curl -sSL https://install.python-poetry.org | python3 - && \
    apt-get clean

# Add Poetry to PATH
ENV PATH="/root/.local/bin:$PATH"

WORKDIR /app

# Copy the source code and other necessary files
COPY src/ ./src/
COPY data/ ./data/
COPY config.yaml ./
# Copy project files
COPY pyproject.toml poetry.lock ./

# Install only production dependencies
RUN poetry install --no-dev

# Run the extractor by default (can be adjusted as needed)
CMD ["poetry", "run", "python", "-m", "etl_pipeline.ingestor"]
