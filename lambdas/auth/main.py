"""
Auth Lambda - Login endpoint for the Data Lake frontend.

No external dependencies — only boto3 (built-in Lambda runtime) and stdlib.
Validates email/password against a PBKDF2-hashed secret in Secrets Manager,
and returns the API key on success.

Setup: run  python scripts/hash_password.py  to generate credentials and store them
in the /data-lake/auth-credentials secret.

Migration path to Supabase JWT (Phase 3):
  Delete this Lambda and swap the api_gateway.setup_lambda_authorizer() call
  for api_gateway.setup_jwt_authorizer(jwks_uri, issuer). The frontend's
  x-api-key header becomes  Authorization: Bearer <jwt>. No business Lambda
  changes required.
"""

import hashlib
import hmac
import json
import logging
import os

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_secrets_client = boto3.client("secretsmanager")

# In-memory cache — avoids Secrets Manager round-trip on warm invocations.
_cached_credentials: dict | None = None
_cached_api_key: str | None = None


def _cors_headers() -> dict:
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "content-type,x-api-key",
        "Access-Control-Allow-Methods": "POST,OPTIONS",
        "Content-Type": "application/json",
    }


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": _cors_headers(),
        "body": json.dumps(body),
    }


def _get_credentials() -> dict | None:
    global _cached_credentials
    if _cached_credentials:
        return _cached_credentials
    try:
        secret = _secrets_client.get_secret_value(
            SecretId=os.environ["AUTH_CREDENTIALS_SECRET_ARN"]
        )
        _cached_credentials = json.loads(secret["SecretString"])
        return _cached_credentials
    except ClientError as e:
        logger.error(f"Failed to read auth credentials: {e}")
        return None


def _get_api_key() -> str:
    global _cached_api_key
    if _cached_api_key:
        return _cached_api_key
    secret = _secrets_client.get_secret_value(
        SecretId=os.environ["API_KEY_SECRET_ARN"]
    )
    _cached_api_key = secret["SecretString"]
    return _cached_api_key


def _verify_password(password: str, stored_hash: str, salt: str) -> bool:
    """PBKDF2-HMAC-SHA256 verification (260k iterations, constant-time compare)."""
    key = hashlib.pbkdf2_hmac(
        "sha256", password.encode(), bytes.fromhex(salt), 260_000
    )
    return hmac.compare_digest(key.hex(), stored_hash)


def handler(event: dict, context) -> dict:
    method = (
        event.get("requestContext", {}).get("http", {}).get("method", "").upper()
    )

    # CORS preflight
    if method == "OPTIONS":
        return _response(200, {})

    if method != "POST":
        return _response(405, {"detail": "Method not allowed"})

    # Parse body
    try:
        body = json.loads(event.get("body") or "{}")
    except (json.JSONDecodeError, TypeError):
        return _response(400, {"detail": "Invalid JSON"})

    email: str = body.get("email", "").strip()
    password: str = body.get("password", "")

    if not email or not password:
        return _response(400, {"detail": "Email and password are required"})

    # Load credentials
    credentials = _get_credentials()
    if not credentials or credentials.get("password_hash") == "placeholder":
        logger.warning("Auth credentials not yet configured — run: python scripts/hash_password.py")
        return _response(503, {"detail": "Auth not configured. Run scripts/hash_password.py"})

    # Constant-time email comparison (avoids timing attacks)
    stored_email = credentials.get("email", "")
    if not hmac.compare_digest(stored_email.lower(), email.lower()):
        return _response(401, {"detail": "Invalid email or password"})

    if not _verify_password(password, credentials["password_hash"], credentials["salt"]):
        return _response(401, {"detail": "Invalid email or password"})

    try:
        api_key = _get_api_key()
    except ClientError as e:
        logger.error(f"Failed to read API key: {e}")
        return _response(500, {"detail": "Internal server error"})

    logger.info(f"Successful login: {email}")
    return _response(200, {"token": api_key})
