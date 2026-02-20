# build a image for Python AWS Lambda
FROM public.ecr.aws/lambda/python:3.12

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install the function's dependencies using file requirements.txt
# from your project folder.
COPY requirements.txt  .
RUN uv pip install --system -r requirements.txt
# RUN uv pip install --system --no-deps --platform manylinux2014_x86_64 deltalake==0.20.0


# Copy the main Lambda handler function
COPY main.py  .

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "main.handler" ]