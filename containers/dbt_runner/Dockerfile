FROM --platform=linux/amd64 python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install Python dependencies
COPY requirements.txt .
RUN uv pip install --system -r requirements.txt

# Copy entrypoint, plugin, and custom materializations
COPY entrypoint.py .
COPY glue_iceberg_plugin.py .
COPY macros/ macros/

ENTRYPOINT ["python", "entrypoint.py"]
