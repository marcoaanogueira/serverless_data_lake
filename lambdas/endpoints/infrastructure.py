"""
Dynamic Infrastructure Management

Creates and manages AWS infrastructure (Firehose, etc.) dynamically via boto3
when endpoints are created/deleted through the API.
"""

import os
import boto3
from typing import Optional
from botocore.exceptions import ClientError


class InfrastructureManager:
    """
    Manages dynamic infrastructure for data lake endpoints.

    When an endpoint is created, this manager provisions the necessary
    AWS resources (Firehose delivery stream) to support data ingestion.
    """

    def __init__(
        self,
        tenant: Optional[str] = None,
        bronze_bucket: Optional[str] = None,
        firehose_role_arn: Optional[str] = None,
    ):
        self.firehose = boto3.client("firehose")
        self.iam = boto3.client("iam")

        self.tenant = tenant or os.environ.get("TENANT", "default")
        self.bronze_bucket = bronze_bucket or os.environ.get("BRONZE_BUCKET", f"{self.tenant}-bronze")
        self.firehose_role_arn = firehose_role_arn or os.environ.get("FIREHOSE_ROLE_ARN")

    def _get_firehose_name(self, domain: str, endpoint_name: str) -> str:
        """Generate Firehose delivery stream name from domain and endpoint"""
        # Format: {Tenant}{Domain}{EndpointName}Firehose
        # e.g., DefaultSalesOrdersFirehose
        tenant_part = self.tenant.capitalize()
        domain_part = domain.title().replace("_", "")
        name_part = endpoint_name.title().replace("_", "")
        return f"{tenant_part}{domain_part}{name_part}Firehose"

    def firehose_exists(self, domain: str, endpoint_name: str) -> bool:
        """Check if Firehose delivery stream already exists"""
        stream_name = self._get_firehose_name(domain, endpoint_name)
        try:
            self.firehose.describe_delivery_stream(DeliveryStreamName=stream_name)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return False
            raise

    def create_firehose(
        self,
        domain: str,
        endpoint_name: str,
        buffer_seconds: int = 60,
        buffer_size_mb: int = 5,
    ) -> dict:
        """
        Create a Kinesis Firehose delivery stream for an endpoint.

        Args:
            domain: Business domain (e.g., sales, finance)
            endpoint_name: Endpoint/table name (e.g., orders)
            buffer_seconds: Buffer interval in seconds (default 60)
            buffer_size_mb: Buffer size in MB (default 5)

        Returns:
            Dict with stream ARN and name
        """
        stream_name = self._get_firehose_name(domain, endpoint_name)

        # Check if already exists
        if self.firehose_exists(domain, endpoint_name):
            # Return existing stream info
            response = self.firehose.describe_delivery_stream(DeliveryStreamName=stream_name)
            return {
                "stream_name": stream_name,
                "stream_arn": response["DeliveryStreamDescription"]["DeliveryStreamARN"],
                "status": "already_exists",
            }

        # S3 prefix for this endpoint's data
        s3_prefix = f"firehose-data/{domain}/{endpoint_name}/"
        s3_error_prefix = f"firehose-errors/{domain}/{endpoint_name}/"

        try:
            # Create the delivery stream
            response = self.firehose.create_delivery_stream(
                DeliveryStreamName=stream_name,
                DeliveryStreamType="DirectPut",
                ExtendedS3DestinationConfiguration={
                    "BucketARN": f"arn:aws:s3:::{self.bronze_bucket}",
                    "RoleARN": self.firehose_role_arn,
                    "Prefix": s3_prefix,
                    "ErrorOutputPrefix": s3_error_prefix,
                    "BufferingHints": {
                        "SizeInMBs": buffer_size_mb,
                        "IntervalInSeconds": buffer_seconds,
                    },
                    "CompressionFormat": "UNCOMPRESSED",
                    "CloudWatchLoggingOptions": {
                        "Enabled": False,
                    },
                },
            )

            return {
                "stream_name": stream_name,
                "stream_arn": response["DeliveryStreamARN"],
                "status": "created",
                "s3_prefix": s3_prefix,
            }

        except ClientError as e:
            raise RuntimeError(f"Failed to create Firehose stream: {e}")

    def delete_firehose(self, domain: str, endpoint_name: str) -> bool:
        """
        Delete a Firehose delivery stream.

        Args:
            domain: Business domain
            endpoint_name: Endpoint/table name

        Returns:
            True if deleted, False if didn't exist
        """
        stream_name = self._get_firehose_name(domain, endpoint_name)

        try:
            self.firehose.delete_delivery_stream(
                DeliveryStreamName=stream_name,
                AllowForceDelete=True,
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return False
            raise

    def get_firehose_status(self, domain: str, endpoint_name: str) -> Optional[dict]:
        """
        Get status of a Firehose delivery stream.

        Returns:
            Dict with stream info or None if doesn't exist
        """
        stream_name = self._get_firehose_name(domain, endpoint_name)

        try:
            response = self.firehose.describe_delivery_stream(DeliveryStreamName=stream_name)
            desc = response["DeliveryStreamDescription"]
            return {
                "stream_name": stream_name,
                "stream_arn": desc["DeliveryStreamARN"],
                "status": desc["DeliveryStreamStatus"],
                "create_timestamp": desc.get("CreateTimestamp"),
            }
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return None
            raise
