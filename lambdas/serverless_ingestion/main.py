from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
from mangum import Mangum
import json
import boto3
import yaml
from datetime import datetime

app = FastAPI()

# Configurando boto3 para usar a AWS real
firehose_client = boto3.client("firehose")

s3_client = boto3.client("s3")

LAYER_SILVER = "silver"
LAYER_ARTIFACTS = "artifacts"
YAML_FILE_KEY = "yaml/tables.yaml"


def get_valid_tables(tenant):

    # Baixar o arquivo YAML do S3
    yaml_file = s3_client.get_object(
        Bucket=f"{tenant}-{LAYER_ARTIFACTS}",
        Key=f"{tenant}/{YAML_FILE_KEY}",
    )
    yaml_content = yaml_file["Body"].read().decode("utf-8")

    # Carregar o conteúdo do YAML
    data = yaml.safe_load(yaml_content)

    # Achar as primary keys da tabela especificada
    valid_table_names = [
        table["table_name"] for tenant in data["tenants"] for table in tenant["tables"]
    ]

    return valid_table_names  # Retornar None se não encontrar


def send_to_firehose(tenant: str, table_name: str, data: Dict[str, Any]):
    if isinstance(data, dict):
        data = [data]  # Se for um dict, transforma em uma lista com um único elemento

    for record in data:
        firehose_client.put_record(
            DeliveryStreamName=f"{tenant.capitalize()}{table_name}Firehose",
            Record={"Data": json.dumps(record).encode("utf-8")},
        )


class RawDataModel(BaseModel):
    data: Dict[str, Any]


@app.post("/send_data_bronze/{tenant}/{data_model_name}")
async def process_data(tenant: str, data_model_name: str, data_model: RawDataModel):
    valid_table_names = get_valid_tables(tenant)
    if data_model_name not in valid_table_names:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid data model name: {data_model_name}. Must be one of: {valid_table_names}",
        )

    data_model.data["insert_date"] = datetime.now().isoformat()

    send_to_firehose(tenant, data_model_name, data_model.data)
    return "Record sent to Firehose"


handler = Mangum(app, lifespan="off")
