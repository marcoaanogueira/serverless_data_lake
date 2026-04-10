"""
API Key Lambda Authorizer for HTTP API Gateway v2

Validates:
  1. x-api-key header — against the value stored in Secrets Manager.
  2. x-origin-verify header — must match the CloudFront origin-verify secret,
     proving the request arrived through CloudFront and not directly to API Gateway.

Returns {"isAuthorized": True/False} using the SIMPLE response format (HTTP API v2).

Future migration path:
- Replace this authorizer with an HttpJwtAuthorizer pointing to a Supabase/Cognito
  JWKS endpoint — no changes required in the business Lambdas (auth stays in the
  API Gateway layer, backends remain agnostic).
"""

import hmac
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
_cached_origin_verify_token: str | None = None


def _get_secret(secret_arn: str) -> str:
    response = _secrets_client.get_secret_value(SecretId=secret_arn)
    return response["SecretString"]


def _get_api_key() -> str:
    """Fetch API key from Secrets Manager with in-memory cache."""
    global _cached_api_key
    if _cached_api_key:
        return _cached_api_key
    _cached_api_key = _get_secret(os.environ["API_KEY_SECRET_ARN"])
    return _cached_api_key


def _get_origin_verify_token() -> str:
    """Fetch CloudFront origin-verify token from Secrets Manager with in-memory cache."""
    global _cached_origin_verify_token
    if _cached_origin_verify_token:
        return _cached_origin_verify_token
    _cached_origin_verify_token = _get_secret(os.environ["ORIGIN_VERIFY_SECRET_ARN"])
    return _cached_origin_verify_token


def handler(event: dict, context) -> dict:
    headers = event.get("headers") or {}
    # HTTP API v2 lowercases all header names
    api_key = headers.get("x-api-key", "").strip()
    origin_verify = headers.get("x-origin-verify", "").strip()

    if not api_key:
        logger.warning("Request rejected: missing x-api-key header")
        return {"isAuthorized": False}

    if not origin_verify:
        logger.warning("Request rejected: missing x-origin-verify header (not routed through CloudFront)")
        return {"isAuthorized": False}

    try:
        expected_key = _get_api_key()
        expected_origin_token = _get_origin_verify_token()
    except ClientError as e:
        logger.error(f"Failed to retrieve secrets from Secrets Manager: {e}")
        return {"isAuthorized": False}

    key_valid = hmac.compare_digest(api_key, expected_key)
    origin_valid = hmac.compare_digest(origin_verify, expected_origin_token)

    if not key_valid:
        logger.warning("Request rejected: invalid x-api-key")
    if not origin_valid:
        logger.warning("Request rejected: invalid x-origin-verify (direct API Gateway call or wrong token)")

    return {"isAuthorized": key_valid and origin_valid}
