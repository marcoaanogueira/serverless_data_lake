"""
API Service Construct - Reusable Lambda Service with API Gateway Integration

This construct provides a declarative way to create Lambda services that are
automatically integrated with an API Gateway. Just define the configuration
and the framework handles the rest.

Usage:
    # Define your service configuration
    config = ApiServiceConfig(
        code_path="lambdas/serverless_ingestion",
        route="/ingestion/{tenant}/{table}",
        use_docker=False,
        layers=["Ingestion", "Utils"],
        memory_size=256,
        timeout_seconds=30,
    )

    # Create the service
    service = ApiService(
        self, "ingestion-service",
        config=config,
        api_gateway=api_gateway,
        layers=layers_dict,
        tenant="MyTenant",
    )
"""

from typing import Optional, List, Dict, Any
from aws_cdk import (
    aws_lambda as _lambda,
    aws_ecr_assets as ecr_assets,
    aws_iam as iam,
    aws_s3 as s3,
    Duration,
    CfnOutput,
)
from aws_cdk.aws_lambda_python_alpha import PythonLayerVersion
from constructs import Construct
from pydantic import BaseModel, Field, model_validator

from .api_gateway import ApiGateway


class ApiServiceConfig(BaseModel):
    """
    Configuration for an API Service.

    This declarative configuration allows you to define all aspects of a
    Lambda service that will be integrated with API Gateway.
    """

    # Code configuration
    code_path: str = Field(..., description="Path to Lambda code directory (e.g., 'lambdas/serverless_ingestion')")
    handler: str = Field("main.handler", description="Lambda handler (module.function)")

    # API Gateway configuration
    route: Optional[str] = Field(None, description="API Gateway route path (e.g., '/ingestion/{tenant}/{table}')")
    enable_api: bool = Field(True, description="Whether to expose this service via API Gateway")

    # Lambda configuration
    use_docker: bool = Field(False, description="Use Docker image instead of zip deployment")
    layers: Optional[List[str]] = Field(None, description="Layer names to attach (if not using Docker)")
    memory_size: int = Field(256, description="Lambda memory in MB")
    timeout_seconds: int = Field(30, description="Lambda timeout in seconds")
    architecture: str = Field("x86", description="Lambda architecture: 'x86' or 'arm64'")
    environment: Dict[str, str] = Field(default_factory=dict, description="Environment variables")

    # Permissions
    grant_s3_access: bool = Field(False, description="Grant read/write access to tenant S3 buckets")
    grant_firehose_access: bool = Field(False, description="Grant access to Firehose streams")
    grant_glue_access: bool = Field(False, description="Grant access to Glue catalog")
    grant_lambda_invoke: bool = Field(False, description="Grant permission to invoke other Lambdas")
    grant_bedrock_access: bool = Field(False, description="Grant access to invoke Bedrock models")

    # Authentication
    require_auth: bool = Field(
        True,
        description=(
            "Require x-api-key header validated by the Lambda authorizer. "
            "Set to False only for public endpoints (e.g., health checks). "
            "Future: swap authorizer for JWT/OIDC without touching this field."
        ),
    )

    # Additional IAM policies
    additional_policies: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Additional IAM policy statements"
    )

    # Architecture mapping
    _architecture_map = {
        "x86": _lambda.Architecture.X86_64,
        "arm64": _lambda.Architecture.ARM_64,
    }

    @model_validator(mode="before")
    def check_layers_and_docker(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        use_docker = values.get("use_docker", False)
        layers = values.get("layers")

        if use_docker and layers:
            raise ValueError("Cannot use both 'use_docker=True' and 'layers'. Choose one.")
        if not use_docker and not layers:
            # Default to empty layers for non-Docker deployments
            values["layers"] = []

        return values

    @property
    def architecture_enum(self) -> _lambda.Architecture:
        """Return CDK Architecture enum value"""
        return self._architecture_map[self.architecture]


class ApiService(Construct):
    """
    Reusable API Service construct.

    Creates a Lambda function with optional API Gateway integration based on
    the provided configuration. Handles both Docker and Layer-based deployments.

    Attributes:
        lambda_function: The created Lambda function
        config: The service configuration
    """

    def __init__(
        self,
        scope: Construct,
        id: str,
        config: ApiServiceConfig,
        tenant: str,
        api_gateway: Optional[ApiGateway] = None,
        layers: Optional[Dict[str, PythonLayerVersion]] = None,
        buckets: Optional[Dict[str, s3.IBucket]] = None,
        firehose_streams: Optional[List] = None,
        environment_overrides: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        self.config = config
        self.tenant = tenant

        # Merge environment variables
        environment = {
            "TZ": "America/Sao_Paulo",
            "TENANT": tenant,
            **config.environment,
            **(environment_overrides or {}),
        }

        # Add API Gateway endpoint to environment if available
        # This allows Lambdas to call other APIs without hardcoding URLs
        if api_gateway:
            environment["API_GATEWAY_ENDPOINT"] = api_gateway.endpoint

        # Create Lambda function
        self.lambda_function = self._create_lambda(
            config=config,
            tenant=tenant,
            layers=layers,
            environment=environment,
        )

        # Grant permissions
        if config.grant_s3_access and buckets:
            self._grant_bucket_permissions(buckets)

        if config.grant_firehose_access and firehose_streams:
            self._grant_firehose_permissions(firehose_streams)

        if config.grant_glue_access:
            self._grant_glue_permissions()

        if config.grant_lambda_invoke:
            self._grant_lambda_invoke_permissions()

        if config.grant_bedrock_access:
            self._grant_bedrock_permissions()

        # Apply additional policies
        for policy in config.additional_policies:
            self.lambda_function.add_to_role_policy(
                iam.PolicyStatement(
                    actions=policy.get("actions", []),
                    resources=policy.get("resources", ["*"]),
                )
            )

        # Register with API Gateway if enabled
        # Note: We always use HTTP method ANY - FastAPI inside Lambda handles routing
        if config.enable_api and api_gateway and config.route:
            route_authorizer = api_gateway.authorizer if config.require_auth else None
            api_gateway.add_route(
                lambda_function=self.lambda_function,
                path=config.route,
                route_id=f"{tenant}-{id}",
                authorizer=route_authorizer,
            )

        # Grant read access to API Gateway endpoint parameter in SSM
        if api_gateway and hasattr(api_gateway, "endpoint_parameter"):
            api_gateway.endpoint_parameter.grant_read(self.lambda_function)

        # Output Lambda ARN
        CfnOutput(
            self,
            "LambdaArn",
            value=self.lambda_function.function_arn,
            description=f"Lambda ARN for {id}",
        )

    def _to_camel_case(self, snake_str: str) -> str:
        """Convert snake_case to CamelCase"""
        return "".join(x.capitalize() for x in snake_str.lower().split("_"))

    def _create_lambda(
        self,
        config: ApiServiceConfig,
        tenant: str,
        layers: Optional[Dict[str, PythonLayerVersion]],
        environment: Dict[str, str],
    ) -> _lambda.IFunction:
        """Create Lambda function based on configuration"""

        # Generate function name
        service_name = config.code_path.split("/")[-1]
        function_name = f"{tenant}{self._to_camel_case(service_name)}"

        if config.use_docker:
            return self._create_docker_lambda(
                config=config,
                function_name=function_name,
                environment=environment,
            )
        else:
            return self._create_zip_lambda(
                config=config,
                function_name=function_name,
                layers=layers,
                environment=environment,
            )

    def _create_docker_lambda(
        self,
        config: ApiServiceConfig,
        function_name: str,
        environment: Dict[str, str],
    ) -> _lambda.DockerImageFunction:
        """Create Docker-based Lambda function"""

        # Determine platform based on architecture
        platform = (
            ecr_assets.Platform.LINUX_ARM64
            if config.architecture == "arm64"
            else ecr_assets.Platform.LINUX_AMD64
        )

        docker_image = ecr_assets.DockerImageAsset(
            self,
            f"DockerImage-{function_name}",
            directory=config.code_path,
            platform=platform,
        )

        return _lambda.DockerImageFunction(
            self,
            function_name,
            function_name=function_name,
            code=_lambda.DockerImageCode.from_ecr(
                repository=docker_image.repository,
                tag=docker_image.image_tag,
            ),
            memory_size=config.memory_size,
            timeout=Duration.seconds(config.timeout_seconds),
            architecture=config.architecture_enum,
            environment=environment,
        )

    def _create_zip_lambda(
        self,
        config: ApiServiceConfig,
        function_name: str,
        layers: Optional[Dict[str, PythonLayerVersion]],
        environment: Dict[str, str],
    ) -> _lambda.Function:
        """Create zip-based Lambda function with layers"""

        # Resolve layers
        lambda_layers = []
        if layers and config.layers:
            for layer_name in config.layers:
                if layer_name in layers:
                    lambda_layers.append(layers[layer_name])

        return _lambda.Function(
            self,
            function_name,
            function_name=function_name,
            runtime=_lambda.Runtime.PYTHON_3_10,
            architecture=config.architecture_enum,
            handler=config.handler,
            code=_lambda.Code.from_asset(config.code_path),
            layers=lambda_layers,
            memory_size=config.memory_size,
            timeout=Duration.seconds(config.timeout_seconds),
            environment=environment,
        )

    def _grant_bucket_permissions(self, buckets: Dict[str, s3.IBucket]) -> None:
        """Grant read/write access to all tenant buckets"""
        for bucket in buckets.values():
            bucket.grant_read_write(self.lambda_function)

    def _grant_firehose_permissions(self, firehose_streams: List) -> None:
        """Grant access to Firehose streams (read, write, create, delete)"""
        # Grant permissions on existing streams
        for stream in firehose_streams:
            self.lambda_function.add_to_role_policy(
                iam.PolicyStatement(
                    actions=["firehose:PutRecord", "firehose:PutRecordBatch"],
                    resources=[stream.attr_arn],
                )
            )

        # Grant permissions to create/delete/describe streams dynamically
        # This is needed for the endpoints service to provision Firehose on-demand
        self.lambda_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "firehose:CreateDeliveryStream",
                    "firehose:DeleteDeliveryStream",
                    "firehose:DescribeDeliveryStream",
                    "firehose:PutRecord",
                    "firehose:PutRecordBatch",
                ],
                resources=["*"],  # Dynamic streams - names not known at deploy time
            )
        )

        # Grant PassRole for Firehose to assume its role
        self.lambda_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["iam:PassRole"],
                resources=["*"],
                conditions={
                    "StringEquals": {
                        "iam:PassedToService": "firehose.amazonaws.com"
                    }
                },
            )
        )

    def _grant_glue_permissions(self) -> None:
        """Grant access to Glue catalog operations"""
        self.lambda_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    # Standard Glue permissions
                    "glue:CreateDatabase",
                    "glue:DeleteDatabase",
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:CreateTable",
                    "glue:DeleteTable",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:UpdateTable",
                    "glue:CreateCrawler",
                    "glue:DeleteCrawler",
                    "glue:GetCrawler",
                    "glue:GetCrawlers",
                    "glue:StartCrawler",
                    "glue:StopCrawler",
                    "glue:CreateJob",
                    "glue:DeleteJob",
                    "glue:GetJob",
                    "glue:GetJobs",
                    "glue:StartJobRun",
                    "glue:StopJobRun",
                    # Glue Iceberg REST endpoint permissions
                    "glue:GetCatalog",
                    "glue:GetCatalogs",
                ],
                resources=["*"],
            )
        )
        # Lake Formation permissions for Iceberg
        self.lambda_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "lakeformation:GetDataAccess",
                    "lakeformation:GetResourceLFTags",
                    "lakeformation:ListLFTags",
                ],
                resources=["*"],
            )
        )

    def _grant_lambda_invoke_permissions(self) -> None:
        """Grant permission to invoke other Lambda functions"""
        self.lambda_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction", "lambda:InvokeAsync"],
                resources=["*"],
            )
        )

    def _grant_bedrock_permissions(self) -> None:
        """Grant access to invoke Bedrock foundation models"""
        self.lambda_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "bedrock:InvokeModel",
                    "bedrock:InvokeModelWithResponseStream",
                ],
                resources=["*"],
            )
        )

    def add_s3_trigger(self, bucket: s3.IBucket, events: List[s3.EventType] = None) -> None:
        """Add S3 event trigger to this Lambda"""
        from aws_cdk import aws_s3_notifications as s3_notifications

        events = events or [s3.EventType.OBJECT_CREATED]
        notification = s3_notifications.LambdaDestination(self.lambda_function)

        for event in events:
            bucket.add_event_notification(event, notification)

    def add_schedule(self, schedule_expression: str, event_input: Dict[str, Any] = None) -> None:
        """Add EventBridge schedule trigger to this Lambda"""
        from aws_cdk import aws_events as events, aws_events_targets as targets

        rule = events.Rule(
            self,
            f"Schedule-{self.node.id}",
            schedule=events.Schedule.expression(schedule_expression),
        )

        target_input = events.RuleTargetInput.from_object(event_input) if event_input else None
        rule.add_target(targets.LambdaFunction(self.lambda_function, event=target_input))
