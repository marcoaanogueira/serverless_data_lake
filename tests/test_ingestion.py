import pytest
import json
from fastapi.testclient import TestClient
from unittest.mock import patch
from lambdas.serverless_ingestion.main import app

client = TestClient(app)

# Mock para os dados do S3
@pytest.fixture
def mock_s3_get_object(mocker):
    mock_s3 = mocker.patch("boto3.client")
    mock_s3.return_value.get_object.return_value = {
        "Body": mocker.MagicMock(read=lambda: b"tenants:\n  - tables:\n      - table_name: vendas")
    }
    return mock_s3

# Mock para o Firehose
@pytest.fixture
def mock_firehose_put_record(mocker):
    mock_firehose = mocker.patch("boto3.client")
    mock_firehose.return_value.put_record.return_value = {}
    return mock_firehose

# Teste para uma tabela válida
def test_process_data_valid():
    response = client.post(
        "/send_data_bronze/decolares/vendas",
        json={"data": {"some_key": "some_value"}}
    )
    assert response.status_code == 200
    assert response.json() == "Record sent to Firehose"

# Teste para uma tabela inválida
def test_process_data_invalid_table():
    response = client.post(
        "/send_data_bronze/decolares/invendas",
        json={"data": {"some_key": "some_value"}}
    )
    assert response.status_code == 400
    assert "Invalid data model name" in response.json()["detail"]
