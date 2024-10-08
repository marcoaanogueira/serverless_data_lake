import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch
from lambdas.serverless_consumption.main import app  # Altere 'your_module' para o nome do seu módulo

@pytest.fixture
def client():
    return TestClient(app)

@pytest.fixture
def mock_duckdb():
    with patch("lambdas.serverless_consumption.main.duckdb") as mock:
        yield mock

def test_read_data(client, mock_duckdb):
    # Mockando o retorno da consulta no DuckDB
    mock_duckdb.connect.return_value.query.return_value.pl.return_value.to_dicts.return_value = [
        {"column1": "value1", "column2": "value2"},
        {"column1": "value3", "column2": "value4"},
    ]
    
    # Criando um corpo de requisição de exemplo
    query = "SELECT * FROM my_table;"
    response = client.request("GET", "/read_data", content=query)
    
    # Verificando a resposta
    assert response.status_code == 200
    assert response.json() == [
        {"column1": "value1", "column2": "value2"},
        {"column1": "value3", "column2": "value4"},
    ]
