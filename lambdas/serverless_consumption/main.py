import duckdb
import os

from fastapi import FastAPI, Query
from mangum import Mangum
from pyiceberg.catalog import load_catalog

CATALOG_NAME = os.environ.get("CATALOG_NAME", "tadpole")

app = FastAPI()

# Load Glue catalog via PyIceberg
catalog = load_catalog("glue", **{"type": "glue"})


def configure_duckdb():
    """Configure DuckDB for querying Arrow tables."""
    con = duckdb.connect(database=":memory:")
    home_directory = "/tmp/duckdb"
    if not os.path.exists(home_directory):
        os.mkdir(home_directory)
    con.execute(f"SET home_directory='{home_directory}';")
    con.execute("INSTALL httpfs;LOAD httpfs;")
    con.execute("INSTALL aws;LOAD aws;")
    con.execute(
        """CREATE SECRET (
        TYPE S3,
        PROVIDER CREDENTIAL_CHAIN
    );"""
    )
    return con


def register_table(con, namespace: str, table_name: str):
    """Load Iceberg table via PyIceberg and register in DuckDB."""
    full_name = f"{namespace}.{table_name}"
    table = catalog.load_table(full_name)
    arrow_table = table.scan().to_arrow()
    con.register(f"{namespace}_{table_name}", arrow_table)
    return f"{namespace}_{table_name}"


@app.get("/consumption/query")
async def execute_query(
    sql: str = Query(..., description="SQL query to execute"),
    namespace: str = Query(..., description="Namespace/database (e.g., sales_silver)"),
    table: str = Query(..., description="Table name (e.g., orders)"),
):
    """Execute a SQL query against an Iceberg table.

    The table is loaded from Glue Catalog and registered in DuckDB.
    Use the registered name in your query: {namespace}_{table}

    Example: ?namespace=sales_silver&table=orders&sql=SELECT * FROM sales_silver_orders LIMIT 10
    """
    con = configure_duckdb()
    registered_name = register_table(con, namespace, table)

    # Replace catalog-style reference with registered table name
    # e.g., tadpole.sales_silver.orders -> sales_silver_orders
    processed_sql = sql.replace(f"{CATALOG_NAME}.{namespace}.{table}", registered_name)
    processed_sql = processed_sql.replace(f"{namespace}.{table}", registered_name)

    result = con.query(processed_sql).pl().to_dicts()
    return {"data": result, "row_count": len(result)}


@app.get("/consumption/tables")
async def list_tables():
    """List all available tables in the Glue catalog."""
    tables = []
    for ns in catalog.list_namespaces():
        namespace_name = ns[0]
        for tbl in catalog.list_tables(namespace_name):
            tables.append({
                "namespace": tbl[0],
                "name": tbl[1],
                "full_name": f"{tbl[0]}.{tbl[1]}"
            })
    return {"tables": tables}


handler = Mangum(app, lifespan="off")
