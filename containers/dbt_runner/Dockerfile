FROM --platform=linux/amd64 python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy entrypoint, plugin, and custom materializations
COPY entrypoint.py .
COPY glue_iceberg_plugin.py .
COPY macros/ macros/

ENTRYPOINT ["python", "entrypoint.py"]
