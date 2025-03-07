import polars as pl
import boto3
import re
import duckdb
import os
import yaml

from pyiceberg.catalog import load_catalog

LAYER_SILVER = "silver"
LAYER_ARTIFACTS = "artifacts"
YAML_FILE_KEY = "yaml/tables.yaml"
STORAGE_OPTIONS = {
    "AWS_S3_ALLOW_UNSAFE_RENAME": "True",
}
s3_client = boto3.client("s3")

catalog = load_catalog("glue", **{"type": "glue"})


def get_primary_keys(table_name, tenant):

    # Baixar o arquivo YAML do S3
    yaml_file = s3_client.get_object(
        Bucket=f"{tenant}-{LAYER_ARTIFACTS}",
        Key=f"{tenant}/{YAML_FILE_KEY}",
    )
    yaml_content = yaml_file["Body"].read().decode("utf-8")

    # Carregar o conteúdo do YAML
    data = yaml.safe_load(yaml_content)

    # Achar as primary keys da tabela especificada
    for table in data["tenants"][0]["tables"]:
        if table["table_name"] == table_name:
            return list(table.get("primary_keys"))


def check_s3_partition_is_empty(bucket_name, partition_prefix):
    # Tenta listar objetos que começam com o prefixo fornecido
    response = s3_client.list_objects_v2(
        Bucket=bucket_name, Prefix=partition_prefix, MaxKeys=1
    )

    # Se houver ao menos um objeto, o prefixo existe
    if "Contents" in response:
        return False
    else:
        return True

def configure_duckdb():
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


def filter_df(df_source: pl.DataFrame, primary_keys, order_date_col="insert_date"):
    ranked_df = df_source.with_columns(
        pl.col(order_date_col).rank("ordinal").over(primary_keys).alias("rank")
    )

    # Filtrando para obter apenas os registros com rank igual a 1
    df_filtered = ranked_df.filter(pl.col("rank") == 1).drop(["rank", "insert_date"])

    return df_filtered


def process_data(bucket: str, s3_object: str):

    s3_path = f"s3://{bucket}/{s3_object}"
    tenant = bucket.split("-")[0]
    table_name = re.search(r"firehose-data/([^/]+)/", s3_object).group(1)
    primary_keys = get_primary_keys(table_name, tenant)

    con = configure_duckdb()
    pyarrow_data_frame = con.query(f"SELECT * FROM read_json_auto('{s3_path}');").pl()
    s3_destination = f"s3://{tenant}-{LAYER_SILVER}/{table_name}"

    full_table_name = f"{tenant}.{table_name}"
    #TODO: talvez trocar aqui por algo que  cheque no catalog
    if not any(tenant == item[0] for item in catalog.list_namespaces()):
        catalog.create_namespace(tenant)
    if not any(tenant == item[0] for item in catalog.list_tables(tenant)):
        arrow_data_frame = pyarrow_data_frame.drop("insert_date").to_arrow().schema
        table = catalog.create_table(
            identifier=full_table_name,
            location=s3_destination,
            schema=arrow_data_frame)
    else:
        arrow_data_frame = pyarrow_data_frame.drop("insert_date").to_arrow().schema
        table = catalog.load_table(full_table_name)
        with table.update_schema() as update_schema:
            update_schema.union_by_name(arrow_data_frame)

    if primary_keys:
        filtered_data_frame = filter_df(pyarrow_data_frame, primary_keys)
        table.upsert(filtered_data_frame.to_arrow(), primary_keys)
        return "Data Writed"

    table.append(arrow_data_frame)

    return "Data Writed"


def handler(event, context):

    s3_event = event["Records"][0]["s3"]
    bucket = s3_event["bucket"]["name"]
    s3_object = s3_event["object"]["key"]

    return process_data(bucket, s3_object)
