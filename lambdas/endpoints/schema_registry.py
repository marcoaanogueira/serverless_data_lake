"""
Schema Registry - S3 Storage with Versioning

Manages schema definitions in S3 with automatic versioning.

Structure:
    s3://{bucket}/schemas/{domain}/{table_name}/
        ├── v1.yaml
        ├── v2.yaml
        ├── ...
        └── latest.yaml  (copy of the latest version)
"""

import os
import boto3
import yaml
from datetime import datetime
from typing import Optional
from botocore.exceptions import ClientError

from models import EndpointSchema, SchemaDefinition, ColumnDefinition, DataType, SchemaMode


class SchemaRegistry:
    """
    Schema Registry for managing endpoint schemas in S3.

    Provides versioned storage of schema definitions with automatic
    latest.yaml maintenance.
    """

    def __init__(self, bucket_name: Optional[str] = None):
        self.s3 = boto3.client("s3")
        self.bucket = bucket_name or os.environ.get("SCHEMA_BUCKET", "data-lake-schemas")
        self.prefix = "schemas"

    def _get_schema_path(self, domain: str, name: str, version: Optional[int] = None) -> str:
        """Get S3 key for a schema file"""
        base = f"{self.prefix}/{domain}/{name}"
        if version is None:
            return f"{base}/latest.yaml"
        return f"{base}/v{version}.yaml"

    def _get_next_version(self, domain: str, name: str) -> int:
        """Get the next version number for a schema"""
        prefix = f"{self.prefix}/{domain}/{name}/v"
        try:
            response = self.s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix,
            )
            if "Contents" not in response:
                return 1

            versions = []
            for obj in response["Contents"]:
                key = obj["Key"]
                # Extract version number from v{n}.yaml
                if key.endswith(".yaml") and "/v" in key:
                    try:
                        version_str = key.split("/v")[-1].replace(".yaml", "")
                        versions.append(int(version_str))
                    except ValueError:
                        continue

            return max(versions) + 1 if versions else 1

        except ClientError:
            return 1

    def create(
        self,
        name: str,
        domain: str,
        columns: list[dict],
        mode: SchemaMode = SchemaMode.MANUAL,
        description: Optional[str] = None,
        created_by: Optional[str] = None,
    ) -> EndpointSchema:
        """
        Create a new endpoint schema.

        Args:
            name: Table/dataset name
            domain: Business domain
            columns: List of column definitions
            mode: Schema mode (manual, auto_inference, single_column)
            description: Optional description
            created_by: Optional user identifier

        Returns:
            Created EndpointSchema
        """
        # Check if schema already exists
        existing = self.get(domain, name)
        if existing:
            raise ValueError(f"Schema {domain}/{name} already exists. Use update() to modify.")

        # Build column definitions
        col_defs = [
            ColumnDefinition(
                name=col.get("name") or col.get("column_name"),
                type=DataType(col.get("type") or col.get("data_type", "string")),
                required=col.get("required", False),
                primary_key=col.get("primary_key", col.get("is_primary_key", False)),
                description=col.get("description"),
            )
            for col in columns
        ]

        schema = EndpointSchema(
            name=name,
            domain=domain,
            version=1,
            mode=mode,
            schema=SchemaDefinition(columns=col_defs),
            description=description,
            created_by=created_by,
        )

        # Save to S3
        self._save_schema(schema)

        return schema

    def update(
        self,
        domain: str,
        name: str,
        columns: Optional[list[dict]] = None,
        description: Optional[str] = None,
    ) -> EndpointSchema:
        """
        Update an existing schema (creates new version).

        Args:
            domain: Business domain
            name: Table/dataset name
            columns: New column definitions (optional)
            description: New description (optional)

        Returns:
            Updated EndpointSchema with incremented version
        """
        existing = self.get(domain, name)
        if not existing:
            raise ValueError(f"Schema {domain}/{name} not found")

        # Build new version
        new_version = self._get_next_version(domain, name)

        if columns:
            col_defs = [
                ColumnDefinition(
                    name=col.get("name") or col.get("column_name"),
                    type=DataType(col.get("type") or col.get("data_type", "string")),
                    required=col.get("required", False),
                    primary_key=col.get("primary_key", col.get("is_primary_key", False)),
                    description=col.get("description"),
                )
                for col in columns
            ]
            new_schema_def = SchemaDefinition(columns=col_defs)
        else:
            new_schema_def = existing.schema_def

        schema = EndpointSchema(
            name=name,
            domain=domain,
            version=new_version,
            mode=existing.mode,
            schema=new_schema_def,
            description=description or existing.description,
            created_at=existing.created_at,
            updated_at=datetime.utcnow(),
            created_by=existing.created_by,
        )

        # Save to S3
        self._save_schema(schema)

        return schema

    def get(self, domain: str, name: str, version: Optional[int] = None) -> Optional[EndpointSchema]:
        """
        Get a schema by domain/name and optional version.

        Args:
            domain: Business domain
            name: Table/dataset name
            version: Specific version (None for latest)

        Returns:
            EndpointSchema or None if not found
        """
        key = self._get_schema_path(domain, name, version)

        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            content = response["Body"].read().decode("utf-8")
            data = yaml.safe_load(content)
            return EndpointSchema.from_yaml_dict(data)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    def list_all(self, domain: Optional[str] = None) -> list[EndpointSchema]:
        """
        List all schemas, optionally filtered by domain.

        Args:
            domain: Filter by domain (optional)

        Returns:
            List of EndpointSchema (latest versions only)
        """
        prefix = f"{self.prefix}/"
        if domain:
            prefix = f"{self.prefix}/{domain}/"

        schemas = []

        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith("/latest.yaml"):
                        # Extract domain and name from path
                        parts = obj["Key"].split("/")
                        if len(parts) >= 4:
                            schema_domain = parts[1]
                            schema_name = parts[2]
                            schema = self.get(schema_domain, schema_name)
                            if schema:
                                schemas.append(schema)
        except ClientError:
            pass

        return schemas

    def list_versions(self, domain: str, name: str) -> list[int]:
        """
        List all versions of a schema.

        Args:
            domain: Business domain
            name: Table/dataset name

        Returns:
            List of version numbers
        """
        prefix = f"{self.prefix}/{domain}/{name}/v"
        versions = []

        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            for obj in response.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".yaml"):
                    try:
                        version_str = key.split("/v")[-1].replace(".yaml", "")
                        versions.append(int(version_str))
                    except ValueError:
                        continue
        except ClientError:
            pass

        return sorted(versions)

    def delete(self, domain: str, name: str) -> bool:
        """
        Delete a schema and all its versions.

        Args:
            domain: Business domain
            name: Table/dataset name

        Returns:
            True if deleted, False if not found
        """
        prefix = f"{self.prefix}/{domain}/{name}/"

        try:
            # List all objects for this schema
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            if "Contents" not in response:
                return False

            # Delete all objects
            objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
            self.s3.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": objects}
            )
            return True

        except ClientError:
            return False

    def _save_schema(self, schema: EndpointSchema) -> None:
        """Save schema to S3 (version file + latest)"""
        yaml_content = yaml.dump(schema.to_yaml_dict(), default_flow_style=False, allow_unicode=True)

        # Save versioned file
        version_key = self._get_schema_path(schema.domain, schema.name, schema.version)
        self.s3.put_object(
            Bucket=self.bucket,
            Key=version_key,
            Body=yaml_content.encode("utf-8"),
            ContentType="application/x-yaml",
        )

        # Save/update latest
        latest_key = self._get_schema_path(schema.domain, schema.name)
        self.s3.put_object(
            Bucket=self.bucket,
            Key=latest_key,
            Body=yaml_content.encode("utf-8"),
            ContentType="application/x-yaml",
        )

    def get_schema_url(self, domain: str, name: str, version: Optional[int] = None) -> str:
        """Get the S3 URL for a schema file"""
        key = self._get_schema_path(domain, name, version)
        return f"s3://{self.bucket}/{key}"

    def generate_presigned_url(
        self,
        domain: str,
        name: str,
        version: Optional[int] = None,
        expiration: int = 3600
    ) -> str:
        """Generate a presigned URL to download the schema"""
        key = self._get_schema_path(domain, name, version)
        return self.s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=expiration,
        )
