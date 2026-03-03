# Use Python 3.12 Lambda image
FROM public.ecr.aws/lambda/python:3.12

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the main Lambda handler
COPY main.py .

# Copy agents package
COPY agents/ agents/

# Set the CMD to handler
CMD [ "main.handler" ]
