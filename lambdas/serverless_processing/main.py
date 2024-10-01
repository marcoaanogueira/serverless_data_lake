import polars as pl
import boto3
import re
import duckdb
import os
import yaml

from polars.dependencies import pyarrow as pa
from deltalake import DeltaTable
from deltalake.schema import Field, PrimitiveType, _convert_pa_schema_to_delta

LAYER_SILVER = "silver"
LAYER_ARTIFACTS = "artifacts"
YAML_FILE_KEY = "yaml/tables.yaml"
STORAGE_OPTIONS = {
    "AWS_S3_ALLOW_UNSAFE_RENAME": "True",
}
s3_client = boto3.client("s3")


def get_primary_keys(table_name, tenant):

    # Baixar o arquivo YAML do S3
    yaml_file = s3_client.get_object(
        Bucket=f"{tenant}-{LAYER_ARTIFACTS}",
        Key=f"{tenant.capitalize()}/{YAML_FILE_KEY}",
    )
    yaml_content = yaml_file["Body"].read().decode("utf-8")

    # Carregar o conteúdo do YAML
    data = yaml.safe_load(yaml_content)

    # Achar as primary keys da tabela especificada
    for table in data["tenants"][0]["tables"]:
        if table["table_name"] == table_name:
            return table["primary_keys"]

    return None  # Retornar None se não encontrar


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


# TODO: converter para pyarrow e adicionar a coluna
def insert_new_column(data_path, df):
    df_target = pl.read_delta(data_path)
    new_columns = set(df.schema) - set(df_target.schema.keys())

    new_fields_to_add = []
    if new_columns:
        for column in new_columns:
            delta_primitive_type = _convert_pa_schema_to_delta(
                df.schema(column).__repr__()
            )
            new_fields_to_add.append(Field(column, PrimitiveType(delta_primitive_type)))

        DeltaTable(data_path).alter.add_columns(new_fields_to_add)


def filter_df(primary_keys, df_source: pl.DataFrame):
    last_date_col = "max_date"

    ranked_df = df_source.with_columns(
        pl.col(last_date_col).rank("ordinal").over(primary_keys).alias("rank")
    )

    # Filtrando para obter apenas os registros com rank igual a 1
    df_filtered = ranked_df.filter(pl.col("rank") == 1).drop("rank")

    return df_filtered


def upsert_delta(primary_keys, df_source: pl.DataFrame, data_path):

    pl_filtered_dataframe = filter_df(primary_keys, df_source)
    pa_table = pl_filtered_dataframe.to_arrow()
    pa_delta_schema = _convert_pa_schema_to_delta(pa_table.schema)
    pa_source_delta_casted = pa_table.cast(pa_delta_schema)

    string_exp = ""

    for pk in primary_keys:
        string_exp += f"s.{pk} = t.{pk}" if string_exp == "" else " AND s.{pk} = t.{pk}"

    dt = DeltaTable(data_path, storage_options=STORAGE_OPTIONS)

    (
        dt.merge(
            source=pa_source_delta_casted,
            predicate=string_exp,
            source_alias="s",
            target_alias="t",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )


def append_delta(df_source: pl.DataFrame, data_path: str):

    DeltaTable(data_path, storage_options=STORAGE_OPTIONS)
    (
        df_source.write_delta(
            data_path,
            mode="append",
            delta_write_options={"schema_mode": "merge"},
        )
    )


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


def process_data(bucket: str, s3_object: str):

    # polars_data_frame = pl.read_json(s3_path)
    s3_path = f"s3://{bucket}/{s3_object}"
    tenant = bucket.split("-")[0]
    table_name = re.search(r"firehose-data/([^/]+)/", s3_object).group(1)
    primary_keys = get_primary_keys(table_name, tenant)

    primary_keys = ["id"]

    # TODO: try to read data just with polars, have problem with this before
    # polars_data_frame = pl.read_ndjson(source=s3_path)
    con = configure_duckdb()
    polars_data_frame = con.query(f"SELECT * FROM read_json_auto('{s3_path}');").pl()

    s3_destination = f"s3://{tenant}-{LAYER_SILVER}/{table_name}"
    if check_s3_partition_is_empty(f"{tenant}-{LAYER_SILVER}", table_name):
        pa_delta_schema = _convert_pa_schema_to_delta(
            polars_data_frame.to_arrow().schema
        )
        DeltaTable.create(
            s3_destination, storage_options=STORAGE_OPTIONS, schema=pa_delta_schema
        )

    if primary_keys:
        upsert_delta(
            primary_keys=primary_keys,
            df_source=polars_data_frame,
            data_path=s3_destination,
        )
        return "Data Writed"

    pa_table = polars_data_frame.to_arrow()
    pa_delta_schema = _convert_pa_schema_to_delta(pa_table.schema)
    pa_source_delta_casted = pa_table.cast(pa_delta_schema)
    append_delta(df_source=pa_source_delta_casted, data_path=s3_destination)

    return "Data Writed"


def handler(event, context):

    s3_event = event["Records"][0]["s3"]
    bucket = s3_event["bucket"]["name"]
    s3_object = s3_event["object"]["key"]

    return process_data(bucket, s3_object)
