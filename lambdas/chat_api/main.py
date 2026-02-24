"""
Chat API — FastAPI + Mangum Lambda for analytics chat agent.

Provides endpoints for managing chat sessions and sending messages
to a Strands-powered analytics agent with SQL and chart tools.
"""

import json
import logging
import os
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum
from pydantic import BaseModel

import chat_store
from agent import create_agent, run_agent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_GATEWAY_ENDPOINT = os.environ.get("API_GATEWAY_ENDPOINT", "")
API_KEY_SECRET_ARN = os.environ.get("API_KEY_SECRET_ARN", "")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---- Request/Response models ----


class SendMessageRequest(BaseModel):
    session_id: Optional[str] = None
    message: str


class SendMessageResponse(BaseModel):
    session_id: str
    message_id: str
    content: list[dict]


# ---- Helpers ----


def _fetch_tables_metadata() -> list[dict]:
    """Fetch table metadata from the query_api via API Gateway."""
    import urllib.request
    import urllib.error

    url = f"{API_GATEWAY_ENDPOINT}/consumption/tables"
    headers = {"Content-Type": "application/json"}

    # Retrieve API key
    api_key_arn = API_KEY_SECRET_ARN
    if api_key_arn:
        try:
            import boto3
            sm = boto3.client("secretsmanager")
            resp = sm.get_secret_value(SecretId=api_key_arn)
            headers["x-api-key"] = resp["SecretString"]
        except Exception as e:
            logger.warning(f"Could not retrieve API key: {e}")

    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
            return data.get("tables", [])
    except Exception as e:
        logger.warning(f"Could not fetch table metadata: {e}")
        return []


# ---- Endpoints ----


@app.post("/chat/message")
async def send_message(request: SendMessageRequest) -> SendMessageResponse:
    """Send a message to the analytics agent and get a response."""

    # Create or reuse session
    session_id = request.session_id
    if not session_id:
        session = chat_store.create_session(title=request.message[:80])
        session_id = session["session_id"]
    else:
        existing = chat_store.get_session(session_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Session not found")

    # Save user message
    user_content = [{"type": "text", "text": request.message}]
    chat_store.add_message(session_id, "user", user_content)

    # Load conversation history
    history = chat_store.get_messages(session_id)
    # Convert to simple format for agent context (exclude the message just added)
    conversation_history = []
    for msg in history[:-1]:  # All except the one we just saved
        text_parts = []
        for block in msg.get("content", []):
            if block.get("type") == "text":
                text_parts.append(block["text"])
        if text_parts:
            conversation_history.append({
                "role": msg["role"],
                "text": " ".join(text_parts),
            })

    # Fetch table metadata and create agent
    tables_metadata = _fetch_tables_metadata()
    agent = create_agent(tables_metadata)

    # Run the agent
    result = run_agent(agent, request.message, conversation_history)

    # Save assistant response
    assistant_content = result.get("content", [])
    assistant_msg = chat_store.add_message(session_id, "assistant", assistant_content)

    return SendMessageResponse(
        session_id=session_id,
        message_id=assistant_msg["message_id"],
        content=assistant_content,
    )


@app.get("/chat/sessions")
async def list_sessions():
    """List all chat sessions."""
    sessions = chat_store.list_sessions()
    return {"sessions": sessions, "count": len(sessions)}


@app.get("/chat/sessions/{session_id}")
async def get_session(session_id: str):
    """Get a chat session with its messages."""
    session = chat_store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    messages = chat_store.get_messages(session_id)
    return {**session, "messages": messages}


@app.delete("/chat/sessions/{session_id}")
async def delete_session(session_id: str):
    """Delete a chat session and all its messages."""
    session = chat_store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    chat_store.delete_session(session_id)
    return {"message": "Session deleted", "session_id": session_id}


handler = Mangum(app, lifespan="off")
