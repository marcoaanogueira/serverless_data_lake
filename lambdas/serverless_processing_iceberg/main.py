import polars as pl
import boto3
import re
import duckdb
import os

from pyiceberg.catalog import load_catalog
from shared.schema_registry import SchemaRegistry

LAYER_SILVER = "silver"
STORAGE_OPTIONS = {
    "AWS_S3_ALLOW_UNSAFE_RENAME": "True",
}
s3_client = boto3.client("s3")

catalog = load_catalog("glue", **{"type": "glue"})

# Initialize schema registry
registry = SchemaRegistry()


def get_schema_info(domain: str, endpoint_name: str) -> dict:
    """
    Get schema information from the registry.

    Returns dict with primary_keys and other schema metadata.
    """
    schema = registry.get(domain, endpoint_name)

    if not schema:
        return {"primary_keys": None, "columns": []}

    return {
        "primary_keys": schema.schema_def.primary_keys or None,
        "columns": [col.name for col in schema.schema_def.columns],
    }


def check_s3_partition_is_empty(bucket_name, partition_prefix):
    """Check if an S3 partition has any objects."""
    response = s3_client.list_objects_v2(
        Bucket=bucket_name, Prefix=partition_prefix, MaxKeys=1
    )
    return "Contents" not in response


def configure_duckdb():
    """Configure DuckDB with S3 access."""
    con = duckdb.connect(database=":memory:")
    home_directory = "/tmp/duckdb/"
    if not os.path.exists(home_directory):
        os.mkdir(home_directory)
    con.execute(f"SET home_directory='{home_directory}';INSTALL httpfs;LOAD httpfs;")
    con.execute("INSTALL aws;LOAD aws;")
    con.execute(
        """CREATE SECRET (
        TYPE S3,
        PROVIDER CREDENTIAL_CHAIN
    );"""
    )
    return con


def filter_df(df_source: pl.DataFrame, primary_keys, order_date_col="_insert_date"):
    """Filter DataFrame to keep only the latest record per primary key."""
    ranked_df = df_source.with_columns(
        pl.col(order_date_col).rank("ordinal").over(primary_keys).alias("rank")
    )

    # Filter to get only records with rank equal to 1
    df_filtered = ranked_df.filter(pl.col("rank") == 1).drop(["rank", "_insert_date"])

    return df_filtered


def parse_s3_path(s3_object: str) -> tuple[str, str]:
    """
    Parse S3 object path to extract domain and endpoint name.

    Expected format: firehose-data/{domain}/{endpoint_name}/...
    """
    match = re.search(r"firehose-data/([^/]+)/([^/]+)/", s3_object)
    if match:
        return match.group(1), match.group(2)

    # Fallback for old format: firehose-data/{table_name}/...
    match = re.search(r"firehose-data/([^/]+)/", s3_object)
    if match:
        return "default", match.group(1)

    raise ValueError(f"Could not parse S3 path: {s3_object}")


def process_data(bucket: str, s3_object: str):
    """Process incoming data from S3 and write to Iceberg table."""
    s3_path = f"s3://{bucket}/{s3_object}"
    tenant = bucket.split("-")[0]

    # Parse domain and endpoint from S3 path
    domain, endpoint_name = parse_s3_path(s3_object)

    # Get schema info from registry
    schema_info = get_schema_info(domain, endpoint_name)
    primary_keys = schema_info["primary_keys"]

    # Read data using DuckDB
    con = configure_duckdb()
    pyarrow_data_frame = con.query(f"SELECT * FROM read_json_auto('{s3_path}');").pl()

    # Destination path and table name
    s3_destination = f"s3://{tenant}-{LAYER_SILVER}/{domain}/{endpoint_name}"
    full_table_name = f"{tenant}.{domain}_{endpoint_name}"

    # Create namespace if not exists
    if not any(tenant == item[0] for item in catalog.list_namespaces()):
        catalog.create_namespace(tenant)

    # Columns to drop from schema (metadata columns)
    metadata_cols = ["_insert_date", "_domain", "_endpoint"]
    cols_to_drop = [c for c in metadata_cols if c in pyarrow_data_frame.columns]

    # Create or update table
    if not any(full_table_name == f"{item[0]}.{item[1]}" for item in catalog.list_tables(tenant)):
        arrow_schema = pyarrow_data_frame.drop(cols_to_drop).to_arrow().schema
        table = catalog.create_table(
            identifier=full_table_name,
            location=s3_destination,
            schema=arrow_schema
        )
    else:
        arrow_schema = pyarrow_data_frame.drop(cols_to_drop).to_arrow().schema
        table = catalog.load_table(full_table_name)
        with table.update_schema() as update_schema:
            update_schema.union_by_name(arrow_schema)

    # Write data
    if primary_keys:
        filtered_data_frame = filter_df(pyarrow_data_frame, primary_keys)
        table.upsert(filtered_data_frame.to_arrow(), primary_keys)
        return f"Data upserted to {full_table_name}"

    # No primary keys - just append
    append_df = pyarrow_data_frame.drop(cols_to_drop)
    table.append(append_df.to_arrow())

    return f"Data appended to {full_table_name}"


def handler(event, context):
    """Lambda handler for S3 events."""
    s3_event = event["Records"][0]["s3"]
    bucket = s3_event["bucket"]["name"]
    s3_object = s3_event["object"]["key"]

    return process_data(bucket, s3_object)
