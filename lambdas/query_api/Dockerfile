# Use Python 3.12 Lambda image (has newer glibc)
FROM public.ecr.aws/lambda/python:3.12

# Install the function's dependencies
COPY requirements.txt  .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy the main Lambda handler function
COPY main.py  .

# Copy shared module
COPY shared/ shared/

# Set the CMD to your handler
CMD [ "main.handler" ]
