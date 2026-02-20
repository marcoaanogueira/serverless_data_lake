# Use Python 3.12 Lambda image (has newer glibc)
FROM public.ecr.aws/lambda/python:3.12

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install the function's dependencies
COPY requirements.txt  .
RUN uv pip install --system -r requirements.txt

# Copy the main Lambda handler function
COPY main.py  .

# Copy shared module
COPY shared/ shared/

# Set the CMD to your handler
CMD [ "main.handler" ]
