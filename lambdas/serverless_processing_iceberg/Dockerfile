# Use a imagem base do AWS Lambda para Python 3.12
FROM public.ecr.aws/lambda/python:3.12

# Instalar Git e ferramentas de compilação (necessários para instalar pacotes do GitHub e compilar extensões C)
RUN dnf install -y git gcc gcc-c++ python3-devel && dnf clean all

# Copiar o requirements.txt
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY requirements.txt .

# Instalar todos os pacotes normalmente (sem --no-deps)
RUN uv pip install --system --no-build -r requirements.txt

# Instalar o PyIceberg do Git
RUN uv pip install --system --no-deps "git+https://github.com/apache/iceberg-python.git#egg=pyiceberg"

# Resolver todas as dependências do PyIceberg
RUN uv pip install --system "pyiceberg[pyarrow,s3fs,hive,glue]"

# Copiar o código da Lambda
COPY main.py .
COPY shared/ ./shared/

# Definir o handler da Lambda
CMD [ "main.handler" ]
