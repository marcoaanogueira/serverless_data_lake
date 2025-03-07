# Use a imagem base do AWS Lambda para Python 3.10
FROM public.ecr.aws/lambda/python:3.10

# Instalar Git (necessário para instalar pacotes do GitHub)
RUN yum update -y && yum install -y git

# Copiar o requirements.txt
COPY requirements.txt .

# Instalar todos os pacotes normalmente (sem --no-deps)
RUN pip install -r requirements.txt

# Instalar o PyIceberg do Git
RUN pip install --no-deps "git+https://github.com/apache/iceberg-python.git#egg=pyiceberg"

# Resolver todas as dependências do PyIceberg
RUN pip install --no-cache-dir "pyiceberg[pyarrow,s3fs,hive,glue]"

# Copiar o código da Lambda
COPY main.py .

# Definir o handler da Lambda
CMD [ "main.handler" ]
