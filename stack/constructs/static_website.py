"""
Static Website Construct - S3 + CloudFront Hosting

This construct creates a serverless static website hosting infrastructure using
S3 for storage and CloudFront for global CDN distribution.

Features:
- S3 bucket for static assets (not publicly accessible)
- CloudFront distribution with Origin Access Control (OAC)
- HTTPS by default
- SPA routing support (redirects 404s to index.html)
- Optional custom domain with automatic ACM certificate creation

Note: For custom domains with CloudFront, the stack MUST be deployed in
us-east-1 since CloudFront requires certificates in that region.
"""

from typing import Optional
from aws_cdk import (
    aws_s3 as s3,
    aws_s3_deployment as s3_deployment,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
    aws_certificatemanager as acm,
    aws_route53 as route53,
    aws_route53_targets as route53_targets,
    aws_ssm as ssm,
    CfnOutput,
    RemovalPolicy,
    Duration,
)
from constructs import Construct
from pydantic import BaseModel, Field


class CustomDomainConfig(BaseModel):
    """Configuration for custom domain setup"""
    domain_name: str = Field(..., description="Custom domain name (e.g., tadpole.com)")
    hosted_zone_name: str = Field(..., description="Route53 hosted zone name (e.g., tadpole.com)")
    certificate_arn: Optional[str] = Field(None, description="Existing ACM certificate ARN. If not provided, a new certificate is created with DNS validation.")
    hosted_zone_id: Optional[str] = Field(None, description="Route53 hosted zone ID (optional, used for lookup)")


class StaticWebsite(Construct):
    """
    Serverless Static Website construct using S3 + CloudFront.

    Creates infrastructure for hosting a React/Vite SPA with:
    - Private S3 bucket for assets
    - CloudFront CDN with Origin Access Control
    - SPA routing support
    - Optional custom domain with automatic certificate

    Usage:
        # Without custom domain (CloudFront URL only)
        website = StaticWebsite(
            self, "frontend",
            site_name="data-platform",
            source_path="frontend/dist",
        )

        # With custom domain (auto-creates ACM certificate)
        website = StaticWebsite(
            self, "frontend",
            site_name="data-platform",
            source_path="frontend/dist",
            custom_domain=CustomDomainConfig(
                domain_name="tadpole.com",
                hosted_zone_name="tadpole.com",
            ),
        )
    """

    def __init__(
        self,
        scope: Construct,
        id: str,
        site_name: str,
        source_path: Optional[str] = None,
        custom_domain: Optional[CustomDomainConfig] = None,
        api_endpoint: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        self.site_name = site_name

        # Create S3 bucket for website content
        self.bucket = s3.Bucket(
            self,
            "WebsiteBucket",
            bucket_name=f"{site_name}-website-{id.lower()}",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # Create Origin Access Control for CloudFront
        oac = cloudfront.S3OriginAccessControl(
            self,
            "OAC",
            description=f"OAC for {site_name} website",
        )

        # Configure custom domain if provided
        domain_names = None
        certificate = None
        self._hosted_zone = None

        if custom_domain:
            domain_names = [custom_domain.domain_name]
            self._hosted_zone = self._lookup_hosted_zone(custom_domain)
            certificate = self._resolve_certificate(custom_domain)

        # Create CloudFront distribution
        self.distribution = cloudfront.Distribution(
            self,
            "Distribution",
            default_behavior=cloudfront.BehaviorOptions(
                origin=origins.S3BucketOrigin.with_origin_access_control(
                    self.bucket,
                    origin_access_control=oac,
                ),
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                allowed_methods=cloudfront.AllowedMethods.ALLOW_GET_HEAD_OPTIONS,
                cached_methods=cloudfront.CachedMethods.CACHE_GET_HEAD_OPTIONS,
                cache_policy=cloudfront.CachePolicy.CACHING_OPTIMIZED,
                compress=True,
            ),
            default_root_object="index.html",
            domain_names=domain_names,
            certificate=certificate,
            error_responses=[
                # SPA routing: redirect 404s to index.html
                cloudfront.ErrorResponse(
                    http_status=404,
                    response_http_status=200,
                    response_page_path="/index.html",
                    ttl=Duration.seconds(0),
                ),
                cloudfront.ErrorResponse(
                    http_status=403,
                    response_http_status=200,
                    response_page_path="/index.html",
                    ttl=Duration.seconds(0),
                ),
            ],
            price_class=cloudfront.PriceClass.PRICE_CLASS_100,
            http_version=cloudfront.HttpVersion.HTTP2_AND_3,
            minimum_protocol_version=cloudfront.SecurityPolicyProtocol.TLS_V1_2_2021,
        )

        # Deploy website content if source path provided
        if source_path:
            self._deploy_website(source_path, api_endpoint)

        # Create Route53 record if custom domain and hosted zone available
        if custom_domain and self._hosted_zone:
            self._create_dns_record(custom_domain)

        # Store website URL in SSM Parameter Store
        self.url_parameter = ssm.StringParameter(
            self,
            "WebsiteUrl",
            parameter_name=f"/data-lake/{site_name}/website-url",
            string_value=self.distribution_url,
            description=f"Website URL for {site_name}",
        )

        # Outputs
        CfnOutput(
            self,
            "WebsiteURL",
            value=self.distribution_url,
            description="CloudFront distribution URL",
            export_name=f"{site_name}-website-url",
        )

        CfnOutput(
            self,
            "BucketName",
            value=self.bucket.bucket_name,
            description="S3 bucket name for website content",
        )

        CfnOutput(
            self,
            "DistributionId",
            value=self.distribution.distribution_id,
            description="CloudFront distribution ID",
        )

    def _lookup_hosted_zone(self, config: CustomDomainConfig) -> route53.IHostedZone:
        """Look up the Route53 hosted zone"""
        if config.hosted_zone_id:
            return route53.HostedZone.from_hosted_zone_attributes(
                self,
                "HostedZone",
                hosted_zone_id=config.hosted_zone_id,
                zone_name=config.hosted_zone_name,
            )
        return route53.HostedZone.from_lookup(
            self,
            "HostedZone",
            domain_name=config.hosted_zone_name,
        )

    def _resolve_certificate(self, config: CustomDomainConfig) -> acm.ICertificate:
        """Resolve or create the ACM certificate for CloudFront"""
        if config.certificate_arn:
            return acm.Certificate.from_certificate_arn(
                self,
                "Certificate",
                certificate_arn=config.certificate_arn,
            )

        # Create a new certificate with DNS validation (auto-validated via Route53)
        return acm.Certificate(
            self,
            "Certificate",
            domain_name=config.domain_name,
            validation=acm.CertificateValidation.from_dns(self._hosted_zone),
        )

    def _deploy_website(self, source_path: str, api_endpoint: Optional[str] = None) -> None:
        """Deploy website content to S3 bucket"""

        # Create environment configuration file for the frontend
        env_config = {}
        if api_endpoint:
            env_config["VITE_API_URL"] = api_endpoint

        s3_deployment.BucketDeployment(
            self,
            "DeployWebsite",
            sources=[s3_deployment.Source.asset(source_path)],
            destination_bucket=self.bucket,
            distribution=self.distribution,
            distribution_paths=["/*"],
        )

    def _create_dns_record(self, config: CustomDomainConfig) -> None:
        """Create Route53 alias record pointing to the CloudFront distribution"""
        # For apex domain (e.g., tadpole.com): record_name is the domain itself
        # For subdomain (e.g., app.tadpole.com): record_name is the subdomain part
        is_apex = config.domain_name == config.hosted_zone_name
        record_name = config.domain_name if is_apex else config.domain_name.split(".")[0]

        route53.ARecord(
            self,
            "AliasRecord",
            zone=self._hosted_zone,
            target=route53.RecordTarget.from_alias(
                route53_targets.CloudFrontTarget(self.distribution)
            ),
            record_name=record_name,
        )

    @property
    def distribution_url(self) -> str:
        """Return the CloudFront distribution URL"""
        return f"https://{self.distribution.distribution_domain_name}"
