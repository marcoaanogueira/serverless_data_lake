# Use Python 3.12 Lambda image
FROM public.ecr.aws/lambda/python:3.12

# Install dependencies
COPY lambdas/ingestion_agent/requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy agents package (shared across agent lambdas)
COPY agents/ agents/

# Copy the main Lambda handler
COPY lambdas/ingestion_agent/main.py .

# Set the CMD to handler
CMD [ "main.handler" ]
