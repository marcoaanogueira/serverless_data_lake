"""
Serverless Data Lake Stack

This stack creates a complete serverless data lake infrastructure with:
- Multi-tenant support
- API Gateway with deterministic endpoints
- Lambda services for ingestion, processing, consumption, and analytics
- S3 buckets (Bronze, Silver, Gold, Artifacts)
- Kinesis Firehose for data buffering
- Integration with DuckDB, Polars, Delta Lake, and Iceberg
"""

import yaml
import os
import logging
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_s3_notifications as s3_notifications,
    aws_iam as iam,
    aws_s3_deployment as s3_deployment,
    aws_kinesisfirehose as firehose,
    aws_events as events,
    aws_events_targets as targets,
    aws_dynamodb as dynamodb,
    CfnOutput,
)
from aws_cdk.aws_lambda_python_alpha import PythonLayerVersion
from constructs import Construct
from typing import List, Optional, Dict, Any

from .constructs import ApiGateway, ApiService, ApiServiceConfig

TIMEZONE = "America/Sao_Paulo"
ARTIFACTS_FOLDER = "artifacts"

logging.basicConfig(level=logging.INFO)


def to_camel_case(snake_str: str) -> str:
    return "".join(x.capitalize() for x in snake_str.lower().split("_"))


# =============================================================================
# SERVICE CONFIGURATIONS - Declarative API Service Definitions
# =============================================================================
# Define your services here. The framework will automatically:
# - Create Lambda functions (Docker or Layer-based)
# - Register routes in API Gateway
# - Configure permissions
# =============================================================================

API_SERVICES: Dict[str, ApiServiceConfig] = {
    # Endpoints API - Manage ingestion endpoint schemas (Schema Registry)
    # Also provisions Firehose streams when endpoints are created
    "endpoints": ApiServiceConfig(
        code_path="lambdas/endpoints",
        route="/endpoints",
        use_docker=False,
        layers=["Shared", "Utils"],
        memory_size=256,
        timeout_seconds=30,
        grant_s3_access=True,
        grant_firehose_access=True,  # Needs to create/delete Firehose streams
    ),
    # Ingestion API - Receives data and sends to Firehose
    "ingestion": ApiServiceConfig(
        code_path="lambdas/serverless_ingestion",
        route="/ingest",
        use_docker=False,
        layers=["Shared", "Ingestion", "Utils"],
        memory_size=256,
        timeout_seconds=30,
        grant_s3_access=True,
        grant_firehose_access=True,
    ),
    # Consumption API - Query data using DuckDB
    "query_api": ApiServiceConfig(
        code_path="lambdas/query_api",
        route="/consumption",
        use_docker=True,
        memory_size=5120,
        timeout_seconds=900,
        grant_s3_access=True,
        grant_glue_access=True,
    ),
}

# Background/Event-driven services (no API Gateway routes)
BACKGROUND_SERVICES: Dict[str, ApiServiceConfig] = {
    # Processing - Triggered by S3 events (Bronze bucket)
    "processing": ApiServiceConfig(
        code_path="lambdas/serverless_processing",
        use_docker=True,
        memory_size=5120,
        timeout_seconds=900,
        enable_api=False,
        grant_s3_access=True,
        grant_glue_access=True,
        grant_lambda_invoke=True,
    ),
    # Processing Iceberg - Triggered by S3 events
    "processing_iceberg": ApiServiceConfig(
        code_path="lambdas/serverless_processing_iceberg",
        use_docker=True,
        memory_size=5120,
        timeout_seconds=900,
        enable_api=False,
        grant_s3_access=True,
        grant_glue_access=True,
    ),
    # XTable - Delta to Iceberg converter (invoked async)
    # DISABLED: Maven 3.9.6 download URL is broken
    # "xtable": ApiServiceConfig(
    #     code_path="lambdas/serverless_xtable",
    #     use_docker=True,
    #     memory_size=5120,
    #     timeout_seconds=900,
    #     architecture="arm64",
    #     enable_api=False,
    #     grant_s3_access=True,
    #     grant_glue_access=True,
    # ),
    # Analytics - Triggered by EventBridge schedules
    "analytics": ApiServiceConfig(
        code_path="lambdas/serverless_analytics",
        use_docker=True,
        memory_size=5120,
        timeout_seconds=900,
        enable_api=False,
        grant_s3_access=True,
    ),
}


class ServerlessDataLakeStack(Stack):
    """
    Main CDK Stack for Serverless Data Lake.

    Creates a complete data lake infrastructure with multi-tenant support,
    API Gateway integration, and event-driven processing pipelines.
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        tables_data = self.load_yaml(ARTIFACTS_FOLDER, "tables.yaml")

        if not tables_data:
            raise ValueError(
                "Arquivo tables.yaml não encontrado ou com formato inválido"
            )

        # Create shared API Gateway
        self.api_gateway = ApiGateway(
            self,
            "DataLakeApiGateway",
            api_name="data-lake-api",
            cors_origins=["*"],
            enable_access_logs=True,
        )

        # Create resources for each tenant
        for table_data in tables_data:
            tenant = table_data.get("tenant_name", "default-tenant")
            self.create_tenant_resources(
                tenant=tenant.capitalize(),
                tables=table_data.get("tables", []),
                jobs=table_data.get("jobs"),
            )

        # Output API endpoint
        CfnOutput(
            self,
            "ApiEndpoint",
            value=self.api_gateway.endpoint,
            description="Data Lake API Gateway Endpoint",
        )

    def load_yaml(self, folder: str, file: str) -> Optional[List[Dict[str, Any]]]:
        """Load YAML configuration file"""
        yaml_path = os.path.join(os.path.dirname(__file__), "..", folder, file)

        if not os.path.exists(yaml_path):
            raise FileNotFoundError(
                f"Arquivo tables.yaml não encontrado no caminho {yaml_path}"
            )

        try:
            with open(yaml_path, "r") as f:
                return yaml.safe_load(f).get("tenants", [])
        except yaml.YAMLError as exc:
            logging.error(f"Erro ao carregar o YAML: {exc}")
            return None

    def create_tenant_resources(
        self,
        tenant: str,
        tables: List[Dict[str, Any]],
        jobs: List[Dict[str, Any]],
    ) -> None:
        """Create all resources for a tenant"""

        # Create S3 buckets
        buckets = self.create_buckets(tenant)

        # Create Firehose streams
        firehose_role = self.create_firehose_role(buckets["Bronze"])
        firehose_streams = self.create_firehoses(
            tenant, tables, buckets["Bronze"], firehose_role
        )

        # Create Lambda layers
        layers = self.create_layers(tenant)

        # Create API services (with API Gateway routes)
        services = {}
        for service_name, config in API_SERVICES.items():
            # Add service-specific environment variables
            env_overrides = {}
            if service_name == "endpoints":
                env_overrides["SCHEMA_BUCKET"] = buckets["Artifacts"].bucket_name
                env_overrides["BRONZE_BUCKET"] = buckets["Bronze"].bucket_name
                env_overrides["FIREHOSE_ROLE_ARN"] = firehose_role.role_arn
                env_overrides["TENANT"] = tenant
            elif service_name == "ingestion":
                env_overrides["SCHEMA_BUCKET"] = buckets["Artifacts"].bucket_name
                env_overrides["TENANT"] = tenant
            elif service_name == "query_api":
                env_overrides["AWS_ACCOUNT_ID"] = self.account

            service = ApiService(
                self,
                f"{tenant}-{service_name}",
                config=config,
                tenant=tenant,
                api_gateway=self.api_gateway,
                layers=layers,
                buckets=buckets,
                firehose_streams=firehose_streams,
                environment_overrides=env_overrides,
            )
            services[service_name] = service

        # Create background services (no API Gateway)
        for service_name, config in BACKGROUND_SERVICES.items():
            # Add service-specific environment variables
            bg_env_overrides = {}
            if service_name == "processing_iceberg":
                bg_env_overrides["SCHEMA_BUCKET"] = buckets["Artifacts"].bucket_name

            service = ApiService(
                self,
                f"{tenant}-{service_name}",
                config=config,
                tenant=tenant,
                api_gateway=None,
                layers=layers,
                buckets=buckets,
                firehose_streams=firehose_streams,
                environment_overrides=bg_env_overrides,
            )
            services[service_name] = service

        # Configure S3 event triggers for processing (Iceberg is the default)
        if "processing_iceberg" in services:
            services["processing_iceberg"].add_s3_trigger(
                bucket=buckets["Bronze"],
                events=[s3.EventType.OBJECT_CREATED],
            )

        # Configure scheduled jobs for analytics
        if jobs and "analytics" in services:
            self.create_jobs(services["analytics"].lambda_function, jobs)

        # Deploy YAML configuration to S3
        self.deploy_yaml_to_s3(buckets["Artifacts"], tenant)

    def create_buckets(self, tenant: str) -> Dict[str, s3.IBucket]:
        """Create S3 buckets for the tenant"""
        bucket_names = ["Bronze", "Silver", "Gold", "Artifacts"]
        return {
            name: s3.Bucket(
                self,
                f"{tenant}{name}",
                bucket_name=f"{tenant.lower()}-{name.lower()}",
            )
            for name in bucket_names
        }

    def create_firehose_role(self, bronze_bucket: s3.IBucket) -> iam.IRole:
        """Create IAM role for Kinesis Firehose"""
        role = iam.Role(
            self,
            "FirehoseRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
        )
        bronze_bucket.grant_write(role)
        return role

    def create_firehoses(
        self,
        tenant: str,
        tables: List[Dict[str, Any]],
        bronze_bucket: s3.IBucket,
        firehose_role: iam.IRole,
    ) -> List[firehose.CfnDeliveryStream]:
        """Create Kinesis Firehose delivery streams for each table"""
        firehose_streams = []

        for table in tables:
            stream = firehose.CfnDeliveryStream(
                self,
                f"{tenant}{table['table_name']}Firehose",
                delivery_stream_name=f"{tenant}{table['table_name']}Firehose",
                delivery_stream_type="DirectPut",
                s3_destination_configuration=firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                    bucket_arn=bronze_bucket.bucket_arn,
                    role_arn=firehose_role.role_arn,
                    prefix=f"firehose-data/{table['table_name']}/",
                    buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                        interval_in_seconds=900,
                        size_in_m_bs=128,
                    ),
                ),
            )
            firehose_streams.append(stream)

        return firehose_streams

    def create_layers(self, tenant: str) -> Dict[str, _lambda.ILayerVersion]:
        """Create Lambda layers"""
        # PythonLayerVersion for requirements.txt based layers
        python_layer_paths = {
            "Ingestion": "layers/ingestion",
            "Utils": "layers/utils",
            "Duckdb": "layers/duckdb",
        }

        layers = {}
        for layer_name, layer_path in python_layer_paths.items():
            if os.path.exists(layer_path):
                layers[layer_name] = PythonLayerVersion(
                    self,
                    f"{tenant}{layer_name}Layer",
                    entry=layer_path,
                    compatible_runtimes=[_lambda.Runtime.PYTHON_3_10],
                    description=f"Layer for {layer_name}",
                )
            else:
                logging.warning(f"Layer path {layer_path} not found. Skipping.")

        # Standard LayerVersion for custom code (shared module)
        shared_layer_path = "layers/shared"
        if os.path.exists(shared_layer_path):
            layers["Shared"] = _lambda.LayerVersion(
                self,
                f"{tenant}SharedLayer",
                code=_lambda.Code.from_asset(shared_layer_path),
                compatible_runtimes=[_lambda.Runtime.PYTHON_3_10],
                description="Shared code layer (models, schema_registry, infrastructure)",
            )

        return layers

    def create_jobs(
        self,
        lambda_function: _lambda.IFunction,
        jobs: List[Dict[str, Any]],
    ) -> None:
        """Create EventBridge scheduled jobs for analytics"""
        for job in jobs:
            job_name = job["job_name"]
            query = job["query"]
            cron = job["cron"]

            cron_expression = events.Schedule.expression(f"cron({cron})")
            rule = events.Rule(
                self,
                job_name,
                schedule=cron_expression,
            )

            rule.add_target(
                targets.LambdaFunction(
                    lambda_function,
                    event=events.RuleTargetInput.from_object({
                        "query": query,
                        "job_name": job_name,
                        "cron_expression": str(cron_expression),
                    }),
                )
            )

    def deploy_yaml_to_s3(self, artifacts_bucket: s3.IBucket, tenant: str) -> None:
        """Deploy YAML configuration to S3"""
        s3_deployment.BucketDeployment(
            self,
            f"DeployArtifacts-{tenant}",
            sources=[s3_deployment.Source.asset("artifacts")],
            destination_bucket=artifacts_bucket,
            destination_key_prefix=f"{tenant.lower()}/yaml",
        )

    def create_dynamodb_table(self) -> dynamodb.Table:
        """Create DynamoDB table for Delta Log (optional)"""
        return dynamodb.Table(
            self,
            "DeltaLogTable",
            table_name="delta_log",
            partition_key=dynamodb.Attribute(
                name="tablePath",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="fileName",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PROVISIONED,
            read_capacity=5,
            write_capacity=5,
        )
