FROM --platform=linux/amd64 python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY containers/ingestion_runner/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy entrypoint
COPY containers/ingestion_runner/entrypoint.py .

# Copy ingestion agent package (runner + models) from Lambda source
COPY lambdas/ingestion_agent/agents/ agents/

ENTRYPOINT ["python", "entrypoint.py"]
