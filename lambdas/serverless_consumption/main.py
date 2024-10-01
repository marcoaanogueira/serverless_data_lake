import re
import duckdb
import os

from fastapi import FastAPI, Request
from mangum import Mangum

DATA_PATH = "decolares-silver"

app = FastAPI()


def configure_duckdb():
    env = os.environ
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
        return f"{keyword} delta_scan('s3://{DATA_PATH}/{table_name}') AS {alias}"

    updated_query = re.sub(pattern, replace_with_delta_scan, query, flags=re.IGNORECASE)
    return updated_query


@app.get("/read_data")
async def read_data(request: Request):
    raw_text = await request.body()
    text_decoded = raw_text.decode("utf-8")
    updated_query = encapsulate_with_delta_scan(text_decoded)
    con = configure_duckdb()
    data_frame_result = con.query(updated_query).pl().to_dicts()
    return data_frame_result


handler = Mangum(app, lifespan="off")
