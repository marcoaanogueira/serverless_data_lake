"""
Schema Registry Models

These Pydantic models define the structure of endpoint schemas.
They can be serialized to YAML for storage in S3 and used for
runtime data validation.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field, field_validator, create_model, ValidationError
import re


class DataType(str, Enum):
    """Supported data types for schema columns"""
    STRING = "string"
    VARCHAR = "varchar"
    INTEGER = "integer"
    BIGINT = "bigint"
    FLOAT = "float"
    DOUBLE = "double"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    DATE = "date"
    JSON = "json"
    ARRAY = "array"
    DECIMAL = "decimal"


class SchemaMode(str, Enum):
    """Schema definition modes"""
    MANUAL = "manual"  # User defines columns manually
    AUTO_INFERENCE = "auto_inference"  # Schema inferred from first data payload
    SINGLE_COLUMN = "single_column"  # Single 'data' column for raw payloads


class ColumnDefinition(BaseModel):
    """Definition of a single column in the schema"""
    name: str = Field(..., description="Column name (snake_case)")
    type: DataType = Field(default=DataType.STRING, description="Data type")
    required: bool = Field(default=False, description="Whether the column is required")
    primary_key: bool = Field(default=False, description="Whether this is a primary key")
    description: Optional[str] = Field(default=None, description="Column description")
    default: Optional[str] = Field(default=None, description="Default value as string")

    @field_validator("name")
    @classmethod
    def validate_column_name(cls, v: str) -> str:
        if not re.match(r"^[a-z][a-z0-9_]*$", v):
            raise ValueError(
                "Column name must be snake_case (lowercase, start with letter, "
                "only letters, numbers, and underscores)"
            )
        return v


class SchemaDefinition(BaseModel):
    """Schema definition containing all columns"""
    columns: list[ColumnDefinition] = Field(default_factory=list)

    @property
    def primary_keys(self) -> list[str]:
        """Get list of primary key column names"""
        return [col.name for col in self.columns if col.primary_key]

    @property
    def required_columns(self) -> list[str]:
        """Get list of required column names"""
        return [col.name for col in self.columns if col.required]


class EndpointSchema(BaseModel):
    """
    Complete endpoint schema definition.

    This is the main model that gets serialized to YAML and stored in S3.
    It contains all metadata about an ingestion endpoint.
    """
    name: str = Field(..., description="Table/dataset name")
    domain: str = Field(..., description="Business domain (e.g., sales, ads, finance)")
    version: int = Field(default=1, description="Schema version")
    mode: SchemaMode = Field(default=SchemaMode.MANUAL, description="Schema definition mode")
    schema_def: SchemaDefinition = Field(default_factory=SchemaDefinition, alias="schema")
    description: Optional[str] = Field(default=None, description="Endpoint description")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: Optional[str] = Field(default=None, description="User who created the endpoint")

    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {
            "example": {
                "name": "orders",
                "domain": "sales",
                "version": 1,
                "mode": "manual",
                "schema": {
                    "columns": [
                        {"name": "order_id", "type": "integer", "required": True, "primary_key": True},
                        {"name": "customer_id", "type": "integer", "required": True},
                        {"name": "total_amount", "type": "decimal", "required": True},
                        {"name": "status", "type": "string", "required": False},
                        {"name": "created_at", "type": "timestamp", "required": False},
                    ]
                },
                "description": "Customer orders data",
            }
        }
    }

    @field_validator("name")
    @classmethod
    def validate_table_name(cls, v: str) -> str:
        if not re.match(r"^[a-z][a-z0-9_]*$", v):
            raise ValueError(
                "Table name must be snake_case (lowercase, start with letter, "
                "only letters, numbers, and underscores)"
            )
        return v

    @field_validator("domain")
    @classmethod
    def validate_domain(cls, v: str) -> str:
        if not re.match(r"^[a-z][a-z0-9_]*$", v):
            raise ValueError(
                "Domain must be snake_case (lowercase, start with letter, "
                "only letters, numbers, and underscores)"
            )
        return v

    def to_yaml_dict(self) -> dict:
        """Convert to dictionary suitable for YAML serialization"""
        return {
            "name": self.name,
            "domain": self.domain,
            "version": self.version,
            "mode": self.mode.value,
            "description": self.description,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "created_by": self.created_by,
            "schema": {
                "columns": [
                    {
                        "name": col.name,
                        "type": col.type.value,
                        "required": col.required,
                        "primary_key": col.primary_key,
                        **({"description": col.description} if col.description else {}),
                        **({"default": col.default} if col.default else {}),
                    }
                    for col in self.schema_def.columns
                ]
            }
        }

    @classmethod
    def from_yaml_dict(cls, data: dict) -> "EndpointSchema":
        """Create instance from YAML dictionary"""
        schema_data = data.get("schema", {})
        columns = [
            ColumnDefinition(
                name=col["name"],
                type=DataType(col.get("type", "string")),
                required=col.get("required", False),
                primary_key=col.get("primary_key", False),
                description=col.get("description"),
                default=col.get("default"),
            )
            for col in schema_data.get("columns", [])
        ]

        return cls(
            name=data["name"],
            domain=data["domain"],
            version=data.get("version", 1),
            mode=SchemaMode(data.get("mode", "manual")),
            schema=SchemaDefinition(columns=columns),
            description=data.get("description"),
            created_at=datetime.fromisoformat(data["created_at"]) if "created_at" in data else datetime.utcnow(),
            updated_at=datetime.fromisoformat(data["updated_at"]) if "updated_at" in data else datetime.utcnow(),
            created_by=data.get("created_by"),
        )

    def validate_payload(self, payload: dict[str, Any]) -> tuple[dict[str, Any], list[dict]]:
        """
        Validate a payload against this schema.

        Args:
            payload: Data payload to validate

        Returns:
            Tuple of (validated_payload, errors)
            - validated_payload: The payload with type coercion applied
            - errors: List of validation error dicts (empty if valid)
        """
        # Single column mode - accept anything
        if self.mode == SchemaMode.SINGLE_COLUMN:
            return payload, []

        columns = self.schema_def.columns
        if not columns:
            return payload, []

        # Build type mapping
        type_map = {
            DataType.STRING: str,
            DataType.VARCHAR: str,
            DataType.INTEGER: int,
            DataType.BIGINT: int,
            DataType.FLOAT: float,
            DataType.DOUBLE: float,
            DataType.BOOLEAN: bool,
            DataType.TIMESTAMP: str,
            DataType.DATE: str,
            DataType.JSON: dict,
            DataType.ARRAY: list,
            DataType.DECIMAL: float,
        }

        # Build field definitions for dynamic model
        field_definitions = {}
        for col in columns:
            python_type = type_map.get(col.type, str)
            if col.required:
                field_definitions[col.name] = (python_type, ...)
            else:
                field_definitions[col.name] = (Optional[python_type], None)

        # Create dynamic model
        DynamicModel = create_model(
            f"Payload_{self.domain}_{self.name}",
            **field_definitions
        )

        try:
            validated = DynamicModel(**payload)
            return validated.model_dump(exclude_none=True), []
        except ValidationError as e:
            errors = [
                {
                    "field": ".".join(str(loc) for loc in err["loc"]),
                    "message": err["msg"],
                    "type": err["type"],
                }
                for err in e.errors()
            ]
            return payload, errors


class CreateEndpointRequest(BaseModel):
    """Request model for creating a new endpoint"""
    name: str = Field(..., description="Table/dataset name")
    domain: str = Field(..., description="Business domain")
    mode: SchemaMode = Field(default=SchemaMode.MANUAL)
    columns: list[ColumnDefinition] = Field(default_factory=list)
    description: Optional[str] = None

    @field_validator("name", "domain")
    @classmethod
    def validate_snake_case(cls, v: str) -> str:
        if not re.match(r"^[a-z][a-z0-9_]*$", v):
            raise ValueError("Must be snake_case")
        return v


class EndpointResponse(BaseModel):
    """Response model for endpoint operations"""
    id: str = Field(..., description="Unique endpoint ID (domain/name)")
    name: str
    domain: str
    version: int
    mode: SchemaMode
    endpoint_url: str = Field(..., description="URL to POST data")
    schema_url: str = Field(..., description="URL to download schema YAML")
    status: str = Field(default="active")
    created_at: datetime
    updated_at: datetime
