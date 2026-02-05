# build a image for Python AWS Lambda
FROM public.ecr.aws/lambda/python:3.10

# Install build dependencies for DuckDB compilation
RUN yum install -y gcc-c++ cmake make && yum clean all

# Install the function's dependencies
COPY requirements.txt  .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy the main Lambda handler function
COPY main.py  .

# Set the CMD to your handler
CMD [ "main.handler" ]
