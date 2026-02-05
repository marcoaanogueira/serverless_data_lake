# build a image for Python AWS Lambda
FROM public.ecr.aws/lambda/python:3.10

# Install the function's dependencies using file requirements.txt
# from your project folder.
COPY requirements.txt  .
RUN pip install -r requirements.txt
# RUN pip install --no-deps --platform manylinux2014_x86_64 deltalake==0.20.0


# Copy the main Lambda handler function
COPY main.py  .

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "main.handler" ]