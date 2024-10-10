import re
import duckdb
import os
import json

DATA_SILVER_PATH = "decolares-silver"
DATA_GOLD_PATH = "decolares-gold"

STORAGE_OPTIONS = {
    "AWS_S3_ALLOW_UNSAFE_RENAME": "True",
}


def configure_duckdb():
    con = duckdb.connect(database=":memory:")
    home_directory = "/tmp/duckdb"
    if not os.path.exists(home_directory):
        os.mkdir(home_directory)
    con.execute(f"SET home_directory='{home_directory}';")
    con.execute("INSTALL httpfs;LOAD httpfs;")
    con.execute("INSTALL delta;LOAD delta;")
    con.execute("INSTALL aws;LOAD aws;")
    con.execute(
        """CREATE SECRET (
        TYPE S3,
        PROVIDER CREDENTIAL_CHAIN
    );"""
    )
    return con


def encapsulate_with_delta_scan(query):
    pattern = r"(\bFROM\b|\bJOIN\b)\s+(\w+)(\s+AS\s+\w+)?"

    def replace_with_delta_scan(match):
        keyword = match.group(1)
        table_name = match.group(2)
        alias = match.group(3) or table_name
        return (
            f"{keyword} delta_scan('s3://{DATA_SILVER_PATH}/{table_name}/') AS {alias}"
        )

    updated_query = re.sub(pattern, replace_with_delta_scan, query, flags=re.IGNORECASE)
    return updated_query


def process_data(query: str):
    updated_query = encapsulate_with_delta_scan(query)
    con = configure_duckdb()
    data_frame_result = con.query(updated_query).pl()
    return data_frame_result


def handler(event, context):
    query = event["query"]
    job_name = event["job_name"]

    polars_dataframe = process_data(query=query)
    s3_path = f"s3://{DATA_GOLD_PATH}/{job_name}"

    polars_dataframe.write_delta(
        s3_path,
        mode="overwrite",
        delta_write_options={"schema_mode": "overwrite"},
        storage_options=STORAGE_OPTIONS,
    )
    return {"statusCode": 200, "body": json.dumps("Query executed")}
