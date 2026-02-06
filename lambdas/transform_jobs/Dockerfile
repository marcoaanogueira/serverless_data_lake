# Use Python 3.12 Lambda image
FROM public.ecr.aws/lambda/python:3.12

# Install dependencies
COPY requirements.txt  .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy the main Lambda handler
COPY main.py  .

# Copy shared module
COPY shared/ shared/

# Set the CMD to handler
CMD [ "main.handler" ]
