# Use Python 3.12 Lambda image
FROM public.ecr.aws/lambda/python:3.12

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install dependencies
COPY requirements.txt  .
RUN uv pip install --system --no-build -r requirements.txt

# Copy the main Lambda handler
COPY main.py  .

# Copy shared module
COPY shared/ shared/

# Set the CMD to handler
CMD [ "main.handler" ]
