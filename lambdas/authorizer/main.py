"""
API Key Lambda Authorizer for HTTP API Gateway v2

Validates the x-api-key header against the value stored in AWS Secrets Manager.
Returns {"isAuthorized": True/False} using the SIMPLE response format (HTTP API v2).

Future migration path:
- Replace this authorizer with an HttpJwtAuthorizer pointing to a Supabase/Cognito
  JWKS endpoint — no changes required in the business Lambdas (auth stays in the
  API Gateway layer, backends remain agnostic).
"""

import os
import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_secrets_client = boto3.client("secretsmanager")

# In-memory cache: avoids a Secrets Manager call on every warm invocation.
# The Lambda authorizer itself caches results per identity_source for results_cache_ttl,
# so this cache only matters when the authorizer Lambda is invoked without cached result.
_cached_api_key: str | None = None


def _get_api_key() -> str:
    """Fetch API key from Secrets Manager with in-memory cache."""
    global _cached_api_key
    if _cached_api_key:
        return _cached_api_key
    secret_arn = os.environ["API_KEY_SECRET_ARN"]
    response = _secrets_client.get_secret_value(SecretId=secret_arn)
    _cached_api_key = response["SecretString"]
    return _cached_api_key


def handler(event: dict, context) -> dict:
    headers = event.get("headers") or {}
    # HTTP API v2 lowercases all header names
    api_key = headers.get("x-api-key", "").strip()

    if not api_key:
        logger.warning("Request rejected: missing x-api-key header")
        return {"isAuthorized": False}

    try:
        expected_key = _get_api_key()
    except ClientError as e:
        logger.error(f"Failed to retrieve API key from Secrets Manager: {e}")
        return {"isAuthorized": False}

    is_authorized = api_key == expected_key
    if not is_authorized:
        logger.warning("Request rejected: invalid API key")
    return {"isAuthorized": is_authorized}
