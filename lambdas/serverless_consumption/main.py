import duckdb
import os

from fastapi import FastAPI
from mangum import Mangum
from pydantic import BaseModel

AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID")
CATALOG_NAME = os.environ.get("CATALOG_NAME", "tadpole")

app = FastAPI()


class QueryRequest(BaseModel):
    query: str


def configure_duckdb():
    """Configure DuckDB with Glue Iceberg catalog."""
    con = duckdb.connect(database=":memory:")
    home_directory = "/tmp/duckdb"
    if not os.path.exists(home_directory):
        os.mkdir(home_directory)
    con.execute(f"SET home_directory='{home_directory}';")
    con.execute("INSTALL httpfs;LOAD httpfs;")
    con.execute("INSTALL iceberg;LOAD iceberg;")
    con.execute("INSTALL aws;LOAD aws;")
    con.execute(
        """CREATE SECRET (
        TYPE S3,
        PROVIDER CREDENTIAL_CHAIN
    );"""
    )

    # Attach Glue Catalog as Iceberg
    # Query example: SELECT * FROM glue_catalog.sales_silver.orders
    con.execute(f"""
        ATTACH '{AWS_ACCOUNT_ID}' AS {CATALOG_NAME} (
            TYPE iceberg,
            ENDPOINT_TYPE 'glue'
        );
    """)

    return con


@app.post("/consumption/query")
async def execute_query(request: QueryRequest):
    """Execute a SQL query against the Iceberg tables."""
    con = configure_duckdb()
    result = con.query(request.query).pl().to_dicts()
    return {"data": result, "row_count": len(result)}


@app.get("/consumption/tables")
async def list_tables():
    """List all available tables in the catalog."""
    con = configure_duckdb()
    result = con.execute("SHOW ALL TABLES;").fetchall()
    tables = [{"database": row[0], "schema": row[1], "name": row[2]} for row in result]
    return {"tables": tables}


handler = Mangum(app, lifespan="off")
