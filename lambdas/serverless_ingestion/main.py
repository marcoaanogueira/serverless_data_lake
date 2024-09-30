from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict, Any
from mangum import Mangum
import json
import boto3

app = FastAPI()

# Configurando boto3 para usar a AWS real
firehose_client = boto3.client("firehose")


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
    send_to_firehose(tenant, data_model_name, data_model.data)
    return "Record sent to Firehose"


handler = Mangum(app, lifespan="off")
