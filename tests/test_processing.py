import polars as pl
from lambdas.serverless_processing.main import (
    filter_df,
    handler,
)


def test_filter_df():
    df_source = pl.DataFrame(
        {
            "insert_date": ["2024-10-01", "2024-10-01", "2024-10-02"],
            "id": [1, 1, 2],
        }
    )
    primary_keys = ["id"]

    filtered_df = filter_df(primary_keys, df_source)
    assert filtered_df.shape[0] == 2  # Deve retornar 2 registros


def test_handler(mocker):
    mock_process_data = mocker.patch(
        "lambdas.serverless_processing.main.process_data", return_value="Data Writed"
    )

    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "tenant-layer"},
                    "object": {"key": "firehose-data/some_data.json"},
                }
            }
        ]
    }

    response = handler(event, None)
    assert response == "Data Writed"
