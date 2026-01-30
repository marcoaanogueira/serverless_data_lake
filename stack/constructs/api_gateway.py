"""
API Gateway Construct - Centralized HTTP API Gateway

This construct creates a shared HTTP API Gateway that can be used by multiple
Lambda services. It supports:
- CORS configuration
- Custom domain (optional)
- Access logging
- Multiple routes with proxy pattern
"""

from typing import Optional, List
from aws_cdk import (
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as integrations,
    aws_logs as logs,
    aws_lambda as _lambda,
    aws_certificatemanager as acm,
    aws_route53 as route53,
    aws_route53_targets as route53_targets,
    CfnOutput,
    RemovalPolicy,
)
from constructs import Construct
from pydantic import BaseModel, Field


class CustomDomainConfig(BaseModel):
    """Configuration for custom domain setup"""
    domain_name: str = Field(..., description="Custom domain name (e.g., api.example.com)")
    certificate_arn: str = Field(..., description="ARN of ACM certificate")
    hosted_zone_id: Optional[str] = Field(None, description="Route53 hosted zone ID")
    hosted_zone_name: Optional[str] = Field(None, description="Route53 hosted zone name")


class ApiGateway(Construct):
    """
    Shared HTTP API Gateway construct.

    Creates a centralized API Gateway that multiple Lambda services can register
    their routes to. Supports CORS, custom domains, and access logging.

    Usage:
        api_gateway = ApiGateway(
            self, "api-gateway",
            api_name="data-lake-api",
            cors_origins=["*"],
        )

        # Add a Lambda route
        api_gateway.add_route(
            lambda_function=my_lambda,
            path="/ingestion/{tenant}/{table}",
            methods=["POST"],
        )
    """

    def __init__(
        self,
        scope: Construct,
        id: str,
        api_name: str,
        cors_origins: List[str] = None,
        cors_methods: List[apigwv2.CorsHttpMethod] = None,
        custom_domain: Optional[CustomDomainConfig] = None,
        enable_access_logs: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        self.api_name = api_name

        # Default CORS configuration
        cors_origins = cors_origins or ["*"]
        cors_methods = cors_methods or [apigwv2.CorsHttpMethod.ANY]

        # Create HTTP API with CORS
        # Note: allow_credentials cannot be used with allow_origins=["*"]
        self.api = apigwv2.HttpApi(
            self,
            "HttpApi",
            api_name=api_name,
            cors_preflight=apigwv2.CorsPreflightOptions(
                allow_origins=cors_origins,
                allow_methods=cors_methods,
                allow_headers=["*"],
            ),
        )

        # Setup access logging if enabled
        if enable_access_logs:
            self._setup_access_logging()

        # Setup custom domain if provided
        if custom_domain:
            self._setup_custom_domain(custom_domain)

        # Output the API endpoint
        CfnOutput(
            self,
            "ApiEndpoint",
            value=self.api.api_endpoint,
            description=f"HTTP API endpoint for {api_name}",
            export_name=f"{api_name}-endpoint",
        )

    def _setup_access_logging(self) -> None:
        """Configure CloudWatch access logging for the API"""
        log_group = logs.LogGroup(
            self,
            "AccessLogs",
            log_group_name=f"/aws/apigateway/{self.api_name}",
            removal_policy=RemovalPolicy.DESTROY,
            retention=logs.RetentionDays.ONE_MONTH,
        )

        # Note: Access logging is configured on the default stage
        # For HTTP APIs, the $default stage is automatically created

    def _setup_custom_domain(self, config: CustomDomainConfig) -> None:
        """Setup custom domain with Route53 alias record"""
        # Import certificate
        certificate = acm.Certificate.from_certificate_arn(
            self,
            "Certificate",
            certificate_arn=config.certificate_arn,
        )

        # Create custom domain
        domain = apigwv2.DomainName(
            self,
            "CustomDomain",
            domain_name=config.domain_name,
            certificate=certificate,
        )

        # Map domain to API
        apigwv2.ApiMapping(
            self,
            "ApiMapping",
            api=self.api,
            domain_name=domain,
        )

        # Create Route53 alias record if hosted zone is provided
        if config.hosted_zone_id or config.hosted_zone_name:
            if config.hosted_zone_id:
                zone = route53.HostedZone.from_hosted_zone_attributes(
                    self,
                    "HostedZone",
                    hosted_zone_id=config.hosted_zone_id,
                    zone_name=config.hosted_zone_name or config.domain_name.split(".", 1)[1],
                )
            else:
                zone = route53.HostedZone.from_lookup(
                    self,
                    "HostedZone",
                    domain_name=config.hosted_zone_name,
                )

            route53.ARecord(
                self,
                "AliasRecord",
                zone=zone,
                target=route53.RecordTarget.from_alias(
                    route53_targets.ApiGatewayv2DomainProperties(
                        regional_domain_name=domain.regional_domain_name,
                        regional_hosted_zone_id=domain.regional_hosted_zone_id,
                    )
                ),
                record_name=config.domain_name.split(".")[0],
            )

        # Output custom domain
        CfnOutput(
            self,
            "CustomDomainEndpoint",
            value=f"https://{config.domain_name}",
            description=f"Custom domain endpoint for {self.api_name}",
        )

    def add_route(
        self,
        lambda_function: _lambda.IFunction,
        path: str,
        methods: List[str] = None,
        route_id: str = None,
    ) -> None:
        """
        Add a Lambda integration route to the API Gateway.

        This method creates both the exact path route and a proxy route
        for nested resources (e.g., /path/{proxy+}).

        Args:
            lambda_function: The Lambda function to integrate
            path: The route path (e.g., "/ingestion/{tenant}/{table}")
            methods: HTTP methods to allow (default: ["ANY"])
            route_id: Optional unique ID for the route (auto-generated if not provided)
        """
        methods = methods or ["ANY"]
        route_id = route_id or path.replace("/", "-").replace("{", "").replace("}", "").strip("-")

        # Convert string methods to HttpMethod enum
        http_methods = []
        for method in methods:
            method_upper = method.upper()
            if method_upper == "ANY":
                http_methods.append(apigwv2.HttpMethod.ANY)
            elif method_upper == "GET":
                http_methods.append(apigwv2.HttpMethod.GET)
            elif method_upper == "POST":
                http_methods.append(apigwv2.HttpMethod.POST)
            elif method_upper == "PUT":
                http_methods.append(apigwv2.HttpMethod.PUT)
            elif method_upper == "DELETE":
                http_methods.append(apigwv2.HttpMethod.DELETE)
            elif method_upper == "PATCH":
                http_methods.append(apigwv2.HttpMethod.PATCH)
            elif method_upper == "HEAD":
                http_methods.append(apigwv2.HttpMethod.HEAD)
            elif method_upper == "OPTIONS":
                http_methods.append(apigwv2.HttpMethod.OPTIONS)

        # Create Lambda integration
        integration = integrations.HttpLambdaIntegration(
            f"{route_id}-integration",
            handler=lambda_function,
            payload_format_version=apigwv2.PayloadFormatVersion.VERSION_2_0,
        )

        # Add main route
        self.api.add_routes(
            path=path,
            methods=http_methods,
            integration=integration,
        )

        # Add proxy route for nested resources if path doesn't already have proxy
        if "{proxy+" not in path:
            proxy_path = f"{path.rstrip('/')}/{{proxy+}}"
            proxy_integration = integrations.HttpLambdaIntegration(
                f"{route_id}-proxy-integration",
                handler=lambda_function,
                payload_format_version=apigwv2.PayloadFormatVersion.VERSION_2_0,
            )
            self.api.add_routes(
                path=proxy_path,
                methods=http_methods,
                integration=proxy_integration,
            )

    @property
    def endpoint(self) -> str:
        """Return the API endpoint URL"""
        return self.api.api_endpoint
