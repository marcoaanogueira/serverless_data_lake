"""
Chat session persistence using S3 (Artifacts bucket).

Stores chat sessions and messages as JSON files under the
``chat_sessions/`` prefix so that the data lake uses a single
storage backend (S3) for all artifacts.

Layout::

    s3://{bucket}/chat_sessions/{session_id}/metadata.json
    s3://{bucket}/chat_sessions/{session_id}/messages/{timestamp}_{message_id}.json
    s3://{bucket}/chat_sessions/{session_id}/agent_context.json
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

import boto3

logger = logging.getLogger(__name__)

SCHEMA_BUCKET = os.environ.get("SCHEMA_BUCKET", "")
PREFIX = "chat_sessions"

_s3 = None


def _get_s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))
    return _s3


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _put_json(key: str, data: dict) -> None:
    _get_s3().put_object(
        Bucket=SCHEMA_BUCKET,
        Key=key,
        Body=json.dumps(data, default=str).encode("utf-8"),
        ContentType="application/json",
    )


def _get_json(key: str) -> Optional[dict]:
    try:
        resp = _get_s3().get_object(Bucket=SCHEMA_BUCKET, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except _get_s3().exceptions.NoSuchKey:
        return None
    except Exception:
        logger.exception("Failed to read %s", key)
        return None


def _metadata_key(session_id: str) -> str:
    return f"{PREFIX}/{session_id}/metadata.json"


def _messages_prefix(session_id: str) -> str:
    return f"{PREFIX}/{session_id}/messages/"


def _agent_context_key(session_id: str) -> str:
    return f"{PREFIX}/{session_id}/agent_context.json"


# ---------------------------------------------------------------------------
# Public API (same interface as before)
# ---------------------------------------------------------------------------


def create_session(title: str = "New Chat") -> dict:
    """Create a new chat session."""
    session_id = str(uuid.uuid4())
    now = _now_iso()

    metadata = {
        "session_id": session_id,
        "title": title,
        "created_at": now,
        "updated_at": now,
        "message_count": 0,
    }
    _put_json(_metadata_key(session_id), metadata)
    return metadata


def get_session(session_id: str) -> Optional[dict]:
    """Get session metadata."""
    data = _get_json(_metadata_key(session_id))
    if not data:
        return None
    return {
        "session_id": data["session_id"],
        "title": data.get("title", ""),
        "created_at": data.get("created_at", ""),
        "updated_at": data.get("updated_at", ""),
        "message_count": data.get("message_count", 0),
    }


def list_sessions(limit: int = 50) -> list[dict]:
    """List all chat sessions, ordered by most recent first."""
    s3 = _get_s3()
    paginator = s3.get_paginator("list_objects_v2")
    sessions = []

    for page in paginator.paginate(Bucket=SCHEMA_BUCKET, Prefix=f"{PREFIX}/", Delimiter="/"):
        for common_prefix in page.get("CommonPrefixes", []):
            # Each common prefix is  chat_sessions/{session_id}/
            session_dir = common_prefix["Prefix"]
            meta_key = f"{session_dir}metadata.json"
            meta = _get_json(meta_key)
            if meta:
                sessions.append({
                    "session_id": meta["session_id"],
                    "title": meta.get("title", ""),
                    "created_at": meta.get("created_at", ""),
                    "updated_at": meta.get("updated_at", ""),
                    "message_count": meta.get("message_count", 0),
                })

    sessions.sort(key=lambda s: s.get("updated_at", ""), reverse=True)
    return sessions[:limit]


def add_message(session_id: str, role: str, content: list[dict]) -> dict:
    """Add a message to a chat session."""
    message_id = str(uuid.uuid4())
    now = _now_iso()

    message = {
        "message_id": message_id,
        "role": role,
        "content": content,
        "created_at": now,
    }

    # Timestamp-prefixed key ensures lexicographic ordering = chronological
    msg_key = f"{_messages_prefix(session_id)}{now}_{message_id}.json"
    _put_json(msg_key, message)

    # Update session metadata
    metadata = _get_json(_metadata_key(session_id))
    if metadata:
        metadata["updated_at"] = now
        metadata["message_count"] = metadata.get("message_count", 0) + 1

        # Auto-update title from first user message
        if metadata["message_count"] <= 1 and role == "user":
            for block in content:
                if block.get("type") == "text":
                    metadata["title"] = block["text"][:80]
                    break

        _put_json(_metadata_key(session_id), metadata)

    return message


def get_messages(session_id: str) -> list[dict]:
    """Get all messages for a session, ordered chronologically."""
    s3 = _get_s3()
    paginator = s3.get_paginator("list_objects_v2")
    messages = []

    for page in paginator.paginate(Bucket=SCHEMA_BUCKET, Prefix=_messages_prefix(session_id)):
        for obj in page.get("Contents", []):
            data = _get_json(obj["Key"])
            if data:
                messages.append({
                    "message_id": data.get("message_id", ""),
                    "role": data.get("role", ""),
                    "content": data.get("content", []),
                    "created_at": data.get("created_at", ""),
                })

    # Keys are timestamp-prefixed so sort is chronological
    messages.sort(key=lambda m: m.get("created_at", ""))
    return messages


def save_agent_context(session_id: str, agent_messages: list[dict]) -> None:
    """Save the raw Strands agent messages for a session."""
    _put_json(_agent_context_key(session_id), {"messages": agent_messages})


def load_agent_context(session_id: str) -> list[dict] | None:
    """Load the raw Strands agent messages for a session."""
    data = _get_json(_agent_context_key(session_id))
    if not data or "messages" not in data:
        return None
    return data["messages"]


def delete_session(session_id: str) -> bool:
    """Delete a session and all its objects."""
    s3 = _get_s3()
    session_prefix = f"{PREFIX}/{session_id}/"

    paginator = s3.get_paginator("list_objects_v2")
    objects_to_delete = []

    for page in paginator.paginate(Bucket=SCHEMA_BUCKET, Prefix=session_prefix):
        for obj in page.get("Contents", []):
            objects_to_delete.append({"Key": obj["Key"]})

    # S3 delete_objects accepts up to 1000 keys per call
    while objects_to_delete:
        batch = objects_to_delete[:1000]
        objects_to_delete = objects_to_delete[1000:]
        s3.delete_objects(Bucket=SCHEMA_BUCKET, Delete={"Objects": batch})

    return True
