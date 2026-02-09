"""
Serverless Data Lake Stack

This stack creates a complete serverless data lake infrastructure with:
- Multi-tenant support
- API Gateway with deterministic endpoints
- Lambda services for ingestion, processing, consumption, analytics, and transforms
- S3 buckets (Bronze, Silver, Gold, Artifacts)
- Kinesis Firehose for data buffering
- Step Functions + ECS Fargate for dbt-based transformations
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
    aws_ecs as ecs,
    aws_ec2 as ec2,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_logs as logs,
    aws_ecr_assets as ecr_assets,
    Duration,
    RemovalPolicy,
    CfnOutput,
)
from aws_cdk.aws_lambda_python_alpha import PythonLayerVersion
from constructs import Construct
from typing import List, Optional, Dict, Any

from .constructs import ApiGateway, ApiService, ApiServiceConfig, StaticWebsite
from .constructs.static_website import CustomDomainConfig

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
    # Transform Jobs API - CRUD for gold layer transform jobs + trigger executions
    "transform_jobs": ApiServiceConfig(
        code_path="lambdas/transform_jobs",
        route="/transform",
        use_docker=True,
        memory_size=512,
        timeout_seconds=30,
        grant_s3_access=True,
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

        # Deploy frontend (S3 + CloudFront)
        certificate_arn = self.node.try_get_context("certificate_arn")
        custom_domain = None
        if certificate_arn:
            custom_domain = CustomDomainConfig(
                domain_name="tadpole.com",
                certificate_arn=certificate_arn,
                hosted_zone_name="tadpole.com",
            )

        self.website = StaticWebsite(
            self,
            "Frontend",
            site_name="data-lake",
            source_path="frontend/dist",
            api_endpoint=self.api_gateway.endpoint,
            custom_domain=custom_domain,
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
                env_overrides["SCHEMA_BUCKET"] = buckets["Artifacts"].bucket_name
            elif service_name == "transform_jobs":
                env_overrides["SCHEMA_BUCKET"] = buckets["Artifacts"].bucket_name
                env_overrides["TENANT"] = tenant

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

        # Create transform pipeline (Step Functions + ECS)
        state_machine = self.create_transform_pipeline(tenant, buckets)

        # Set STATE_MACHINE_ARN on transform_jobs Lambda
        if "transform_jobs" in services and state_machine:
            services["transform_jobs"].lambda_function.add_environment(
                "STATE_MACHINE_ARN", state_machine.state_machine_arn
            )
            state_machine.grant_start_execution(services["transform_jobs"].lambda_function)
            # Grant describe/list executions
            services["transform_jobs"].lambda_function.add_to_role_policy(
                iam.PolicyStatement(
                    actions=[
                        "states:DescribeExecution",
                        "states:ListExecutions",
                    ],
                    resources=["*"],
                )
            )

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

    def create_transform_pipeline(
        self,
        tenant: str,
        buckets: Dict[str, s3.IBucket],
    ) -> sfn.StateMachine:
        """
        Create Step Functions state machine + ECS Fargate for dbt transforms.

        Flow: Validate Config -> Run ECS Task (dbt) -> Update Status
        """
        # VPC for ECS tasks (default VPC with public subnets for simplicity)
        vpc = ec2.Vpc(
            self,
            f"{tenant}TransformVpc",
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                )
            ],
        )

        # ECS Cluster
        cluster = ecs.Cluster(
            self,
            f"{tenant}TransformCluster",
            cluster_name=f"{tenant.lower()}-transform",
            vpc=vpc,
        )

        # Task execution role (for pulling images, writing logs)
        execution_role = iam.Role(
            self,
            f"{tenant}TransformExecRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonECSTaskExecutionRolePolicy"
                ),
            ],
        )

        # Task role (for accessing S3, Glue from within the container)
        task_role = iam.Role(
            self,
            f"{tenant}TransformTaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        )
        for bucket in buckets.values():
            bucket.grant_read_write(task_role)
        # Glue permissions for Iceberg catalog (DuckDB ATTACH + PyIceberg writes)
        task_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase",
                    "glue:GetTable", "glue:GetTables",
                    "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
                    "glue:GetCatalog", "glue:GetCatalogs",
                ],
                resources=["*"],
            )
        )

        # Task definition
        task_definition = ecs.FargateTaskDefinition(
            self,
            f"{tenant}DbtRunnerTask",
            family=f"{tenant.lower()}-dbt-runner",
            memory_limit_mib=2048,
            cpu=1024,
            execution_role=execution_role,
            task_role=task_role,
            runtime_platform=ecs.RuntimePlatform(
                cpu_architecture=ecs.CpuArchitecture.X86_64,
                operating_system_family=ecs.OperatingSystemFamily.LINUX,
            ),
        )

        # Container (dbt runner)
        dbt_container = task_definition.add_container(
            "dbt-runner",
            image=ecs.ContainerImage.from_asset(
                "containers/dbt_runner",
                platform=ecr_assets.Platform.LINUX_AMD64,
            ),
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="dbt-runner",
                log_group=logs.LogGroup(
                    self,
                    f"{tenant}DbtRunnerLogs",
                    log_group_name=f"/ecs/{tenant.lower()}/dbt-runner",
                    removal_policy=RemovalPolicy.DESTROY,
                ),
            ),
            environment={
                "SCHEMA_BUCKET": buckets["Artifacts"].bucket_name,
                "SILVER_BUCKET": buckets["Silver"].bucket_name,
                "GOLD_BUCKET": buckets["Gold"].bucket_name,
                "AWS_REGION": self.region,
                "AWS_ACCOUNT_ID": self.account,
                "GLUE_CATALOG_NAME": "tadpole",
                "TENANT": tenant,
            },
        )

        # Shared ECS launch config
        ecs_launch_target = sfn_tasks.EcsFargateLaunchTarget(
            platform_version=ecs.FargatePlatformVersion.LATEST,
        )

        # Single-mode ECS task (triggered via API for one job)
        run_single_task = sfn_tasks.EcsRunTask(
            self,
            f"{tenant}RunDbtSingle",
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            cluster=cluster,
            task_definition=task_definition,
            launch_target=ecs_launch_target,
            assign_public_ip=True,
            container_overrides=[
                sfn_tasks.ContainerOverride(
                    container_definition=dbt_container,
                    environment=[
                        sfn_tasks.TaskEnvironmentVariable(
                            name="RUN_MODE",
                            value="single",
                        ),
                        sfn_tasks.TaskEnvironmentVariable(
                            name="JOB_DOMAIN",
                            value=sfn.JsonPath.string_at("$.domain"),
                        ),
                        sfn_tasks.TaskEnvironmentVariable(
                            name="JOB_NAME",
                            value=sfn.JsonPath.string_at("$.job_name"),
                        ),
                        sfn_tasks.TaskEnvironmentVariable(
                            name="QUERY",
                            value=sfn.JsonPath.string_at("$.query"),
                        ),
                        sfn_tasks.TaskEnvironmentVariable(
                            name="WRITE_MODE",
                            value=sfn.JsonPath.string_at("$.write_mode"),
                        ),
                        sfn_tasks.TaskEnvironmentVariable(
                            name="UNIQUE_KEY",
                            value=sfn.JsonPath.string_at("$.unique_key"),
                        ),
                    ],
                )
            ],
            result_path="$.ecs_result",
        )

        # Scheduled-mode ECS task (triggered via EventBridge for batch runs)
        run_scheduled_task = sfn_tasks.EcsRunTask(
            self,
            f"{tenant}RunDbtScheduled",
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            cluster=cluster,
            task_definition=task_definition,
            launch_target=ecs_launch_target,
            assign_public_ip=True,
            container_overrides=[
                sfn_tasks.ContainerOverride(
                    container_definition=dbt_container,
                    environment=[
                        sfn_tasks.TaskEnvironmentVariable(
                            name="RUN_MODE",
                            value="scheduled",
                        ),
                        sfn_tasks.TaskEnvironmentVariable(
                            name="TAG_FILTER",
                            value=sfn.JsonPath.string_at("$.tag_filter"),
                        ),
                    ],
                )
            ],
            result_path="$.ecs_result",
        )

        # Success and failure states
        success_state = sfn.Succeed(self, f"{tenant}TransformSuccess")
        fail_state = sfn.Fail(
            self,
            f"{tenant}TransformFailed",
            cause="dbt run failed",
            error="DbtRunFailed",
        )

        # Add retry and catch to both ECS tasks
        for task in [run_single_task, run_scheduled_task]:
            task.add_retry(
                max_attempts=2,
                interval=Duration.seconds(10),
                backoff_rate=2.0,
            )
            task.add_catch(fail_state, result_path="$.error")

        # Choice state: route by run_mode
        check_mode = sfn.Choice(self, f"{tenant}CheckRunMode")
        definition = check_mode.when(
            sfn.Condition.string_equals("$.run_mode", "scheduled"),
            run_scheduled_task.next(success_state),
        ).otherwise(
            run_single_task.next(success_state),
        )

        # State Machine
        state_machine = sfn.StateMachine(
            self,
            f"{tenant}TransformPipelineV2",
            state_machine_name=f"{tenant.lower()}-transform-pipeline-v2",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.hours(1),
        )

        CfnOutput(
            self,
            f"{tenant}TransformStateMachineArnV2",
            value=state_machine.state_machine_arn,
            description=f"Transform Pipeline State Machine ARN for {tenant}",
        )

        # EventBridge scheduled rules for batch dbt runs
        schedules = {
            "hourly": "rate(1 hour)",
            "daily": "cron(0 2 * * ? *)",       # 2 AM UTC daily
            "monthly": "cron(0 3 1 * ? *)",      # 3 AM UTC 1st of month
        }
        for tag, schedule_expr in schedules.items():
            rule = events.Rule(
                self,
                f"{tenant}Transform{tag.capitalize()}",
                schedule=events.Schedule.expression(schedule_expr),
            )
            rule.add_target(
                targets.SfnStateMachine(
                    state_machine,
                    input=events.RuleTargetInput.from_object({
                        "run_mode": "scheduled",
                        "tag_filter": tag,
                    }),
                )
            )

        return state_machine

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
