"""
Schema Validator for Ingestion

Validates incoming payloads against schemas stored in S3.
Dynamically creates Pydantic validators based on schema definitions.
"""

import os
from datetime import datetime, date
from typing import Any, Optional
from pydantic import BaseModel, create_model, ValidationError
import boto3
import yaml
from botocore.exceptions import ClientError


class SchemaNotFoundError(Exception):
    """Raised when schema doesn't exist in the registry"""
    pass


class SchemaValidationError(Exception):
    """Raised when payload doesn't match schema"""
    def __init__(self, message: str, errors: list[dict]):
        super().__init__(message)
        self.errors = errors


# Mapping from schema types to Python types
TYPE_MAPPING = {
    "string": str,
    "integer": int,
    "float": float,
    "boolean": bool,
    "timestamp": str,  # Accept as string, validate format
    "date": str,       # Accept as string, validate format
    "json": dict,
    "array": list,
    "decimal": float,
}


class SchemaValidator:
    """
    Validates payloads against schemas stored in S3.

    Uses the same S3 structure as the endpoints service:
    s3://{bucket}/schemas/{domain}/{name}/latest.yaml
    """

    def __init__(self, bucket_name: Optional[str] = None):
        self.s3 = boto3.client("s3")
        self.bucket = bucket_name or os.environ.get("SCHEMA_BUCKET", "data-lake-schemas")
        self.prefix = "schemas"
        self._cache: dict[str, dict] = {}  # Simple in-memory cache

    def _get_schema_path(self, domain: str, name: str) -> str:
        """Get S3 key for latest schema"""
        return f"{self.prefix}/{domain}/{name}/latest.yaml"

    def endpoint_exists(self, domain: str, name: str) -> bool:
        """
        Check if an endpoint exists by looking for its schema in S3.

        More efficient than loading the full schema - just checks if the key exists.
        """
        key = self._get_schema_path(domain, name)
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def list_endpoints(self, domain: Optional[str] = None) -> list[dict]:
        """
        List all available endpoints.

        Returns list of {domain, name} dicts.
        """
        prefix = f"{self.prefix}/"
        if domain:
            prefix = f"{self.prefix}/{domain}/"

        endpoints = []
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith("/latest.yaml"):
                        parts = obj["Key"].split("/")
                        if len(parts) >= 4:
                            endpoints.append({
                                "domain": parts[1],
                                "name": parts[2],
                            })
        except ClientError:
            pass

        return endpoints

    def get_schema(self, domain: str, name: str, use_cache: bool = True) -> dict:
        """
        Get schema definition from S3.

        Args:
            domain: Business domain
            name: Endpoint name
            use_cache: Whether to use cached schema

        Returns:
            Schema dictionary

        Raises:
            SchemaNotFoundError: If schema doesn't exist
        """
        cache_key = f"{domain}/{name}"

        if use_cache and cache_key in self._cache:
            return self._cache[cache_key]

        key = self._get_schema_path(domain, name)

        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            content = response["Body"].read().decode("utf-8")
            schema = yaml.safe_load(content)

            if use_cache:
                self._cache[cache_key] = schema

            return schema
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise SchemaNotFoundError(
                    f"Endpoint '{domain}/{name}' not found. "
                    f"Create it first using the /endpoints API."
                )
            raise

    def _build_pydantic_model(self, schema: dict) -> type[BaseModel]:
        """
        Dynamically build a Pydantic model from schema definition.

        Args:
            schema: Schema dictionary from YAML

        Returns:
            Dynamically created Pydantic model class
        """
        columns = schema.get("schema", {}).get("columns", [])

        if not columns:
            # No columns defined - accept any data
            return create_model("DynamicPayload", data=(dict, ...))

        field_definitions = {}

        for col in columns:
            col_name = col["name"]
            col_type = col.get("type", "string")
            is_required = col.get("required", False)
            default_value = col.get("default")

            python_type = TYPE_MAPPING.get(col_type, str)

            if is_required:
                # Required field with no default
                field_definitions[col_name] = (python_type, ...)
            elif default_value is not None:
                # Optional field with default
                field_definitions[col_name] = (Optional[python_type], default_value)
            else:
                # Optional field, default to None
                field_definitions[col_name] = (Optional[python_type], None)

        model_name = f"Payload_{schema.get('domain', 'unknown')}_{schema.get('name', 'unknown')}"
        return create_model(model_name, **field_definitions)

    def validate(self, domain: str, name: str, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Validate a payload against the schema.

        Args:
            domain: Business domain
            name: Endpoint name
            payload: Data payload to validate

        Returns:
            Validated and possibly coerced payload

        Raises:
            SchemaNotFoundError: If schema doesn't exist
            SchemaValidationError: If payload doesn't match schema
        """
        schema = self.get_schema(domain, name)

        # Check schema mode
        mode = schema.get("mode", "manual")

        if mode == "single_column":
            # Single column mode - accept any data without validation
            return payload

        # Build dynamic model
        PayloadModel = self._build_pydantic_model(schema)

        try:
            validated = PayloadModel(**payload)
            # Return as dict, excluding None values for optional fields
            return validated.model_dump(exclude_none=True)
        except ValidationError as e:
            errors = []
            for error in e.errors():
                errors.append({
                    "field": ".".join(str(loc) for loc in error["loc"]),
                    "message": error["msg"],
                    "type": error["type"],
                })

            raise SchemaValidationError(
                f"Payload validation failed for {domain}/{name}",
                errors=errors,
            )

    def validate_types(self, domain: str, name: str, payload: dict[str, Any]) -> list[dict]:
        """
        Validate payload types and return list of warnings/errors.

        This is a softer validation that returns issues instead of raising.
        Useful for auto_inference mode where we want to be more lenient.

        Args:
            domain: Business domain
            name: Endpoint name
            payload: Data payload to validate

        Returns:
            List of validation issues (empty if all valid)
        """
        issues = []

        try:
            schema = self.get_schema(domain, name)
        except SchemaNotFoundError:
            return [{"field": "_schema", "message": "Schema not found", "severity": "error"}]

        columns = schema.get("schema", {}).get("columns", [])
        column_map = {col["name"]: col for col in columns}

        # Check for missing required fields
        for col in columns:
            if col.get("required") and col["name"] not in payload:
                issues.append({
                    "field": col["name"],
                    "message": f"Required field '{col['name']}' is missing",
                    "severity": "error",
                })

        # Check types for provided fields
        for field_name, value in payload.items():
            if field_name not in column_map:
                issues.append({
                    "field": field_name,
                    "message": f"Field '{field_name}' not defined in schema",
                    "severity": "warning",
                })
                continue

            expected_type = column_map[field_name].get("type", "string")
            actual_type = self._infer_type(value)

            if not self._types_compatible(expected_type, actual_type):
                issues.append({
                    "field": field_name,
                    "message": f"Expected {expected_type}, got {actual_type}",
                    "severity": "error",
                })

        return issues

    def _infer_type(self, value: Any) -> str:
        """Infer the type of a value"""
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int):
            return "integer"
        if isinstance(value, float):
            return "float"
        if isinstance(value, list):
            return "array"
        if isinstance(value, dict):
            return "json"
        return "string"

    def _types_compatible(self, expected: str, actual: str) -> bool:
        """Check if actual type is compatible with expected type"""
        if expected == actual:
            return True

        # Some type coercions are acceptable
        compatible_pairs = {
            ("float", "integer"),  # int can be coerced to float
            ("decimal", "integer"),
            ("decimal", "float"),
            ("string", "integer"),  # numbers can be stringified
            ("string", "float"),
            ("string", "boolean"),
            ("timestamp", "string"),  # timestamps are strings
            ("date", "string"),
        }

        return (expected, actual) in compatible_pairs

    def clear_cache(self, domain: Optional[str] = None, name: Optional[str] = None):
        """Clear schema cache"""
        if domain and name:
            cache_key = f"{domain}/{name}"
            self._cache.pop(cache_key, None)
        elif domain:
            # Clear all schemas for a domain
            keys_to_remove = [k for k in self._cache if k.startswith(f"{domain}/")]
            for k in keys_to_remove:
                del self._cache[k]
        else:
            self._cache.clear()
