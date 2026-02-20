FROM public.ecr.aws/lambda/python:3.12

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY main.py .

CMD ["main.handler"]
