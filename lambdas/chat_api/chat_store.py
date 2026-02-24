"""
Chat session persistence using DynamoDB.

Stores chat sessions and messages for the analytics agent.
Each session has metadata and a list of messages.
"""

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger(__name__)

CHAT_TABLE_NAME = os.environ.get("CHAT_TABLE_NAME", "")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")


def _get_table():
    """Get the DynamoDB table resource."""
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    return dynamodb.Table(CHAT_TABLE_NAME)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_session(title: str = "New Chat") -> dict:
    """Create a new chat session."""
    session_id = str(uuid.uuid4())
    now = _now_iso()

    item = {
        "pk": f"SESSION#{session_id}",
        "sk": "METADATA",
        "session_id": session_id,
        "title": title,
        "created_at": now,
        "updated_at": now,
        "message_count": 0,
    }

    table = _get_table()
    table.put_item(Item=item)
    return {
        "session_id": session_id,
        "title": title,
        "created_at": now,
        "updated_at": now,
        "message_count": 0,
    }


def get_session(session_id: str) -> Optional[dict]:
    """Get session metadata."""
    table = _get_table()
    resp = table.get_item(Key={"pk": f"SESSION#{session_id}", "sk": "METADATA"})
    item = resp.get("Item")
    if not item:
        return None
    return {
        "session_id": item["session_id"],
        "title": item.get("title", ""),
        "created_at": item.get("created_at", ""),
        "updated_at": item.get("updated_at", ""),
        "message_count": item.get("message_count", 0),
    }


def list_sessions(limit: int = 50) -> list[dict]:
    """List all chat sessions, ordered by most recent first."""
    table = _get_table()

    # Scan for all session metadata items
    resp = table.scan(
        FilterExpression="sk = :sk",
        ExpressionAttributeValues={":sk": "METADATA"},
        Limit=limit * 2,  # Over-scan since filter is post-scan
    )

    sessions = []
    for item in resp.get("Items", []):
        sessions.append({
            "session_id": item["session_id"],
            "title": item.get("title", ""),
            "created_at": item.get("created_at", ""),
            "updated_at": item.get("updated_at", ""),
            "message_count": item.get("message_count", 0),
        })

    # Sort by updated_at descending
    sessions.sort(key=lambda s: s.get("updated_at", ""), reverse=True)
    return sessions[:limit]


def add_message(session_id: str, role: str, content: list[dict]) -> dict:
    """
    Add a message to a chat session.

    Args:
        session_id: The session to add the message to.
        role: 'user' or 'assistant'.
        content: List of content blocks (text, chart, etc.)
    """
    message_id = str(uuid.uuid4())
    now = _now_iso()

    table = _get_table()

    # Store the message
    message_item = {
        "pk": f"SESSION#{session_id}",
        "sk": f"MSG#{now}#{message_id}",
        "message_id": message_id,
        "role": role,
        "content": content,
        "created_at": now,
    }
    table.put_item(Item=message_item)

    # Update session metadata
    table.update_item(
        Key={"pk": f"SESSION#{session_id}", "sk": "METADATA"},
        UpdateExpression="SET updated_at = :now, message_count = message_count + :inc",
        ExpressionAttributeValues={":now": now, ":inc": 1},
    )

    # Auto-update title from first user message
    session = get_session(session_id)
    if session and session.get("message_count", 0) <= 1 and role == "user":
        first_text = ""
        for block in content:
            if block.get("type") == "text":
                first_text = block["text"][:80]
                break
        if first_text:
            table.update_item(
                Key={"pk": f"SESSION#{session_id}", "sk": "METADATA"},
                UpdateExpression="SET title = :title",
                ExpressionAttributeValues={":title": first_text},
            )

    return {
        "message_id": message_id,
        "role": role,
        "content": content,
        "created_at": now,
    }


def get_messages(session_id: str) -> list[dict]:
    """Get all messages for a session, ordered chronologically."""
    table = _get_table()
    resp = table.query(
        KeyConditionExpression=Key("pk").eq(f"SESSION#{session_id}")
        & Key("sk").begins_with("MSG#"),
        ScanIndexForward=True,  # Ascending order
    )

    messages = []
    for item in resp.get("Items", []):
        messages.append({
            "message_id": item.get("message_id", ""),
            "role": item.get("role", ""),
            "content": item.get("content", []),
            "created_at": item.get("created_at", ""),
        })

    return messages


def save_agent_context(session_id: str, agent_messages: list[dict]) -> None:
    """Save the raw Strands agent messages for a session.

    These include the full conversation with tool use/results so the agent
    can maintain context across turns.
    """
    import json

    table = _get_table()
    serialized = json.dumps(agent_messages, default=str)

    # DynamoDB has a 400KB item limit. Truncate old messages if too large.
    max_bytes = 350_000  # Leave headroom for DynamoDB overhead
    while len(serialized.encode("utf-8")) > max_bytes and agent_messages:
        # Remove the oldest pair of messages (user + assistant)
        agent_messages = agent_messages[2:]
        serialized = json.dumps(agent_messages, default=str)

    table.put_item(Item={
        "pk": f"SESSION#{session_id}",
        "sk": "AGENT_CONTEXT",
        "messages": serialized,
        "updated_at": _now_iso(),
    })


def load_agent_context(session_id: str) -> list[dict] | None:
    """Load the raw Strands agent messages for a session."""
    import json

    table = _get_table()
    resp = table.get_item(Key={"pk": f"SESSION#{session_id}", "sk": "AGENT_CONTEXT"})
    item = resp.get("Item")
    if not item or "messages" not in item:
        return None
    try:
        return json.loads(item["messages"])
    except (json.JSONDecodeError, TypeError):
        logger.warning(f"Failed to parse agent context for session {session_id}")
        return None


def delete_session(session_id: str) -> bool:
    """Delete a session and all its messages."""
    table = _get_table()

    # Query all items for this session
    resp = table.query(
        KeyConditionExpression=Key("pk").eq(f"SESSION#{session_id}"),
    )

    # Delete all items in batch
    with table.batch_writer() as batch:
        for item in resp.get("Items", []):
            batch.delete_item(Key={"pk": item["pk"], "sk": item["sk"]})

    return True
