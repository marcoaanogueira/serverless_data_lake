import polars as pl
import boto3
import re
import duckdb
import os
import yaml

from polars.dependencies import pyarrow as pa
from deltalake import DeltaTable
from deltalake.schema import Field, PrimitiveType

DATA_PATH = "./silver"
BUCKET = "decolares-silver"
BUCKET_ARTIFACTS = "decolares-artifcats"
YAML_FILE_KEY = "Decolares/yaml/tables.yaml"
STORAGE_OPTIONS = {
    "AWS_S3_ALLOW_UNSAFE_RENAME": "True",
}
s3_client = boto3.client("s3")

# Ver se a função que foi feita no polars é melhor
# def polars_to_delta_rs(polars_type: str) -> str:
#     type_mapping = {
#     "Int8": "byte",
#     "Int16": "short",
#     "Int32": "integer",
#     "Int64": "long",
#     "UInt8": "short",
#     "UInt16": "integer",
#     "UInt32": "long",
#     "UInt64": "decimal(20,0)",
#     "Float32": "float",
#     "Utf8": "string",
#     "Boolean": "boolean",
#     "Binary": "binary"
#     }

#     return type_mapping.get(polars_type, "unknowm")


def get_primary_keys(table_name):

    # Baixar o arquivo YAML do S3
    yaml_file = s3_client.get_object(Bucket=BUCKET_ARTIFACTS, Key=YAML_FILE_KEY)
    yaml_content = yaml_file['Body'].read().decode('utf-8')

    # Carregar o conteúdo do YAML
    data = yaml.safe_load(yaml_content)

    # Achar as primary keys da tabela especificada
    for table in data['tenants'][0]['tables']:
        if table['table_name'] == table_name:
            return table['primary_keys']
    
    return None  # Retornar None se não encontrar


def _convert_pa_schema_to_delta(schema: pa.schema) -> pa.schema:
    """Convert a PyArrow schema to a schema compatible with Delta Lake."""
    dtype_map = {
        pa.uint8(): pa.int8(),
        pa.uint16(): pa.int16(),
        pa.uint32(): pa.int32(),
        pa.uint64(): pa.int64(),
    }

    def dtype_to_delta_dtype(dtype: pa.DataType) -> pa.DataType:
        if isinstance(dtype, pa.LargeListType):
            return list_to_delta_dtype(dtype)
        elif isinstance(dtype, pa.StructType):
            return struct_to_delta_dtype(dtype)
        elif isinstance(dtype, pa.TimestampType):
            return pa.timestamp("us")
        try:
            return dtype_map[dtype]
        except KeyError:
            return dtype

    def list_to_delta_dtype(dtype: pa.LargeListType) -> pa.LargeListType:
        nested_dtype = dtype.value_type
        nested_dtype_cast = dtype_to_delta_dtype(nested_dtype)
        return pa.large_list(nested_dtype_cast)

    def struct_to_delta_dtype(dtype: pa.StructType) -> pa.StructType:
        fields = [dtype.field(i) for i in range(dtype.num_fields)]
        fields_cast = [pa.field(f.name, dtype_to_delta_dtype(f.type)) for f in fields]
        return pa.struct(fields_cast)

    return pa.schema([pa.field(f.name, dtype_to_delta_dtype(f.type)) for f in schema])


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


def insert_new_column(data_path, df):
    df_target = pl.read_delta(data_path)
    new_columns = set(df.schema) - set(df_target.schema.keys())

    new_fields_to_add = []
    if new_columns:
        for column in new_columns:
            # delta_primitive_type = polars_to_delta_rs(df.schema(column).__repr__())
            delta_primitive_type = _convert_pa_schema_to_delta(df.schema)
            new_fields_to_add.append(Field(column, PrimitiveType(delta_primitive_type)))

        DeltaTable(data_path).alter.add_columns(new_fields_to_add)


def upsert_delta(primary_keys, df: pl.DataFrame, data_path):

    string_exp = ""

    for pk in primary_keys:
        string_exp += f"s.{pk} = t.{pk}" if string_exp == "" else " AND s.{pk} = t.{pk}"

    (
        df.write_delta(
            data_path,
            mode="merge",
            delta_merge_options={
                "predicate": string_exp,
                "source_alias": "s",
                "target_alias": "t",
            },
            storage_options=STORAGE_OPTIONS,
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )


def append_delta(df: pl.DataFrame, data_path: str):
    (
        df.write_delta(
            data_path,
            mode="append",
            delta_write_options={"schema_mode": "merge"},
            storage_options=STORAGE_OPTIONS,
        )
    )

def configure_duckdb():
    env=os.environ
    con = duckdb.connect(database=':memory:')
    home_directory="/tmp/duckdb/"
    if not os.path.exists(home_directory) :
        os.mkdir(home_directory)
    con.execute(f"SET home_directory='{home_directory}';INSTALL httpfs;LOAD httpfs;")
    con.execute(f"SET s3_region='{env['AWS_REGION']}';")
    con.execute(f"SET s3_access_key_id='{env['AWS_ACCESS_KEY_ID']}';")
    con.execute(f"SET s3_secret_access_key='{env['AWS_SECRET_ACCESS_KEY']}';")
    con.execute(f"SET s3_session_token='{env['AWS_SESSION_TOKEN']}';")
    return con



def process_data(bucket: str, s3_object: str):

    # polars_data_frame = pl.read_json(s3_path)
    s3_path = f"s3://{bucket}/{s3_object}"
    table_name = re.search(r"firehose-data/([^/]+)/", s3_object).group(1)
    primary_keys = get_primary_keys(table_name)


    print("Configurin Duckdb")
    con = configure_duckdb()
    print("Duckdb configured")
    polars_data_frame = con.query(f"SELECT * FROM read_json_auto('{s3_path}');").pl()
    print("Data Queried")

    # TODO: try to read data just with (polars or pyarrow) or remove polars on add table
    # polars_data_frame = pl.read_ndjson(source=s3_path)
    
    s3_destination = f"s3://{BUCKET}/{table_name}"
    if check_s3_partition_is_empty(BUCKET, table_name):
        polars_data_frame.write_delta(s3_destination, storage_options=STORAGE_OPTIONS)

        return "Data Writed"

    if primary_keys:
        upsert_delta(
            primary_keys=primary_keys, df=polars_data_frame, data_path=s3_destination
        )
        return "Data Writed"

    append_delta(df=polars_data_frame, data_path=s3_destination)

    return "Data Writed"


def handler(event, context):

    s3_event = event["Records"][0]["s3"]
    bucket = s3_event["bucket"]["name"]
    s3_object = s3_event["object"]["key"]

    return process_data(bucket, s3_object)
