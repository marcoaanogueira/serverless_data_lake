# Use a imagem base do AWS Lambda para Python 3.12
FROM public.ecr.aws/lambda/python:3.12

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY requirements.txt .
RUN uv pip install --system --no-build -r requirements.txt

# Copiar o código da Lambda
COPY main.py .
COPY shared/ ./shared/

# Definir o handler da Lambda
CMD [ "main.handler" ]
