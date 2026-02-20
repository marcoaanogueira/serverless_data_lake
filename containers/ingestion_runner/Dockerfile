FROM --platform=linux/amd64 python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies (dlt + httpx only — no LLM packages needed)
COPY containers/ingestion_runner/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy entrypoint
COPY containers/ingestion_runner/entrypoint.py .

# Copy only the files needed for dlt pipeline execution.
# The LLM agents (agent.py, pk_agent.py, description_agent.py) run in the
# ingestion_agent Lambda and are NOT needed here.
RUN mkdir -p agents/ingestion_agent
COPY lambdas/ingestion_agent/agents/__init__.py agents/__init__.py
COPY lambdas/ingestion_agent/agents/ingestion_agent/__init__.py agents/ingestion_agent/__init__.py
COPY lambdas/ingestion_agent/agents/ingestion_agent/models.py agents/ingestion_agent/models.py
COPY lambdas/ingestion_agent/agents/ingestion_agent/runner.py agents/ingestion_agent/runner.py

ENTRYPOINT ["python", "entrypoint.py"]
