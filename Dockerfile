FROM python:3.12-slim

WORKDIR /app

# Install system dependencies for psycopg
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir . 2>/dev/null || true

# Copy source code
COPY migrations/ migrations/
COPY src/ src/

# Install the package in editable mode
RUN pip install --no-cache-dir -e ".[dev]"

# Default entrypoint — overridden by docker-compose per service
CMD ["python", "-m", "relay.api.main"]
