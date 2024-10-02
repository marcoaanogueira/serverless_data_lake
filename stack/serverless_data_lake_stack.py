import yaml
import os
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_s3_notifications as s3_notifications,
    aws_iam as iam,
    aws_s3_deployment as s3_deployment,
    aws_kinesisfirehose as firehose,
    aws_ecr_assets as ecr_assets,
    aws_dynamodb as dynamodb,
    Duration,
)
from aws_cdk.aws_lambda_python_alpha import PythonLayerVersion
from constructs import Construct
from typing import List, Optional
from pydantic import BaseModel, Field, model_validator

TIMEZONE = "America/Sao_Paulo"


def to_camel_case(snake_str):
    return "".join(x.capitalize() for x in snake_str.lower().split("_"))


class LambdaFunction(BaseModel):
    layers: Optional[List[str]] = Field(None, description="Lambda layers")
    use_ecr: bool = Field(..., description="Define se a imagem será construída com ECR")

    @model_validator(mode="before")
    def check_layers(cls, values):
        use_ecr = values.get("use_ecr")
        layers = values.get("layers")
        if use_ecr and layers is not None:
            raise ValueError("Se 'use_ecr' for True, 'layers' deve ser None")
        if not use_ecr and not layers:
            raise ValueError("Se 'use_ecr' for False, 'layers' é obrigatório")
        return values


class ServerlessDataLakeStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        yaml_path = os.path.join(
            os.path.dirname(__file__), "..", "artifacts", "tables.yaml"
        )

        if not os.path.exists(yaml_path):
            raise FileNotFoundError(
                f"Arquivo tables.yaml não encontrado no caminho {yaml_path}"
            )

        tenants_data = self.load_yaml(yaml_path)

        if tenants_data:  # Verifica se o arquivo foi carregado corretamente
            for tenant_data in tenants_data:
                tenant = tenant_data.get(
                    "tenant_name", "default-tenant"
                )  # Valor padrão para evitar erros
                self.create_tenant_resources(
                    tenant.capitalize(), tenant_data.get("tables", [])
                )
        else:
            raise ValueError(
                "Arquivo tables.yml não encontrado ou com formato inválido"
            )

    def load_yaml(self, path: str):
        """Carrega o arquivo YAML e retorna o conteúdo"""
        try:
            with open(path, "r") as file:
                return yaml.safe_load(file).get("tenants", [])
        except FileNotFoundError:
            print(f"Erro: Arquivo {path} não encontrado.")
        except yaml.YAMLError as exc:
            print(f"Erro ao carregar o YAML: {exc}")
        return None

    def create_tenant_resources(self, tenant: str, tables: list):
        """Cria os buckets, lambdas e firehoses para cada tenant"""
        buckets = self.create_buckets(tenant)

        firehose_role = self.create_firehose_role()
        firehose_streams = self.create_firehoses(
            tenant, tables, buckets["Bronze"], firehose_role
        )

        layers = self.create_layers(tenant)
        lambda_functions = {
            "serverless_consumption": LambdaFunction(use_ecr=True),
            "serverless_ingestion": LambdaFunction(
                layers=["Ingestion", "Utils"], use_ecr=False
            ),
            "serverless_processing": LambdaFunction(use_ecr=True),
        }
        lambdas = {}
        for function_name, function_attributes in lambda_functions.items():
            lambda_function = self.create_lambda_function(
                function_name=function_name,
                tenant=tenant,
                layers=layers,
                function_attributes=function_attributes,
            )
            lambdas[function_name] = lambda_function
            self.grant_bucket_permissions(lambda_function, buckets)
            self.grant_firehose_permissions(lambda_function, firehose_streams)

        self.add_s3_event_notification(
            lambdas["serverless_processing"], buckets["Bronze"]
        )
        self.deploy_yaml_to_s3(buckets["Artifacts"], tenant)
        # self.create_dynamodb_table()

    def create_buckets(self, tenant: str) -> dict:
        """Cria os buckets para cada tenant e retorna o dicionário de buckets"""
        bucket_names = ["Bronze", "Silver", "Gold", "Artifacts"]
        buckets = {}

        for name in bucket_names:
            bucket = s3.Bucket(
                self, f"{tenant}{name}", bucket_name=f"{tenant.lower()}-{name.lower()}"
            )
            buckets[name] = bucket

        return buckets

    def grant_bucket_permissions(
        self, lambda_function: _lambda.IFunction, buckets: dict
    ):
        """Concede permissões de leitura/escrita a todos os buckets para a função Lambda"""
        for bucket in buckets.values():
            bucket.grant_read_write(lambda_function)

    def create_layers(self, tenant: str):
        layer_paths = {
            "Ingestion": "layers/ingestion",
            "Utils": "layers/utils",
        }

        layers = {}
        for layer_name, layer_path in layer_paths.items():
            if os.path.exists(layer_path):
                layers[layer_name] = PythonLayerVersion(
                    self,
                    f"{tenant}{layer_name}Layer",
                    entry=layer_path,
                    compatible_runtimes=[_lambda.Runtime.PYTHON_3_10],
                    description=f"Layer for {layer_name}",
                )
            else:
                print(f"Warning: Layer file {layer_path} not found. Skipping.")

        return layers

    def create_lambda_function(
        self,
        function_name: str,
        tenant: str,
        layers: dict,
        function_attributes: LambdaFunction,
    ) -> _lambda.IFunction:
        """Cria a função Lambda com as camadas apropriadas"""

        camel_function_name = f"{tenant}{to_camel_case(function_name)}"
        if function_attributes.use_ecr:

            docker_image_asset = ecr_assets.DockerImageAsset(
                self,
                f"LambdaImage{camel_function_name}",
                directory=f"lambdas/{function_name}",
                platform=ecr_assets.Platform.LINUX_AMD64,
            )

            lambda_function = _lambda.DockerImageFunction(
                self,
                camel_function_name,
                function_name=camel_function_name,
                code=_lambda.DockerImageCode.from_ecr(
                    repository=docker_image_asset.repository,
                    tag=docker_image_asset.image_tag,
                ),
                memory_size=5120,
                timeout=Duration.minutes(15),
                architecture=_lambda.Architecture.X86_64,
                environment={"TZ": TIMEZONE},
            )

            return lambda_function

        else:
            layer_names = function_attributes.layers

            lambda_layers = [layers[name] for name in layer_names if name in layers]

            lambda_function = _lambda.Function(
                self,
                camel_function_name,
                function_name=camel_function_name,
                runtime=_lambda.Runtime.PYTHON_3_10,
                architecture=_lambda.Architecture.X86_64,
                handler="main.handler",
                code=_lambda.Code.from_asset(f"lambdas/{function_name}"),
                layers=lambda_layers,
            )

            lambda_function.add_function_url(
                auth_type=_lambda.FunctionUrlAuthType.NONE  # Ou você pode definir autenticação conforme necessário
            )

            return lambda_function

    def deploy_yaml_to_s3(self, artifacts_bucket: s3.IBucket, tenant: str):
        """Faz o deploy do YAML no bucket S3"""
        s3_deployment.BucketDeployment(
            self,
            f"DeployArtifacts-{tenant}",
            sources=[s3_deployment.Source.asset("artifacts")],
            destination_bucket=artifacts_bucket,
            destination_key_prefix=f"{tenant.lower()}/yaml",  # Adiciona o prefixo do tenant no bucket
        )

    def create_firehose_role(self) -> iam.Role:
        # Cria a role IAM para o Firehose com a política adequada
        role = iam.Role(
            self,
            "FirehoseRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
            ],
        )
        return role

    def create_firehoses(
        self, tenant: str, tables: list, bucket: s3.IBucket, firehose_role: iam.IRole
    ):
        """Cria os Firehoses para cada tabela"""
        firehose_streams = []
        for table in tables:
            firehose_streams.append(
                firehose.CfnDeliveryStream(
                    self,
                    f"{tenant}{table['table_name'] }Firehose",
                    delivery_stream_name=f"{tenant}{table['table_name'] }Firehose",
                    delivery_stream_type="DirectPut",
                    s3_destination_configuration=firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                        bucket_arn=bucket.bucket_arn,
                        role_arn=firehose_role.role_arn,
                        prefix=f"firehose-data/{table['table_name']}/",
                        buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                            interval_in_seconds=300, size_in_m_bs=5
                        ),
                    ),
                )
            )
        return firehose_streams

    def grant_firehose_permissions(
        self, lambda_function: _lambda.IFunction, firehose_streams: list
    ):
        """Concede permissão ao Lambda para gravar no Firehose"""
        for stream in firehose_streams:
            lambda_function.add_to_role_policy(
                iam.PolicyStatement(
                    actions=["firehose:PutRecord", "firehose:PutRecordBatch"],
                    resources=[stream.attr_arn],
                )
            )

    def add_s3_event_notification(
        self, lambda_function: _lambda.IFunction, bucket: s3.IBucket
    ):
        """Adiciona um evento de notificação S3 para invocar a função Lambda"""
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3_notifications.LambdaDestination(lambda_function),
        )

    def create_dynamodb_table(self) -> dynamodb.Table:
        return dynamodb.Table(
            self,
            "DeltaLogTable",
            table_name="delta_log",  # Nome da tabela
            partition_key=dynamodb.Attribute(
                name="tablePath",
                type=dynamodb.AttributeType.STRING,  # Tipo da chave HASH
            ),
            sort_key=dynamodb.Attribute(
                name="fileName",
                type=dynamodb.AttributeType.STRING,  # Tipo da chave RANGE
            ),
            # Definindo a capacidade provisionada (não autoscaling)
            billing_mode=dynamodb.BillingMode.PROVISIONED,
            read_capacity=5,  # Capacidade de leitura
            write_capacity=5,  # Capacidade de escrita
        )
