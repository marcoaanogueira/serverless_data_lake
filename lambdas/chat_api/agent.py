"""
Strands Agent setup for analytics chat.

Creates a Bedrock-powered agent with SQL execution and chart tools,
using table metadata from the Schema Registry as context.
"""

import logging
import os

from strands import Agent
from strands.models import BedrockModel

from prompt import build_system_prompt
from tools import execute_sql, display_chart

logger = logging.getLogger(__name__)

BEDROCK_MODEL_ID = os.environ.get(
    "BEDROCK_MODEL_ID", "anthropic.claude-3-5-sonnet-20241022-v2:0"
)


def _create_model() -> BedrockModel:
    """Create the Bedrock model for the agent."""
    return BedrockModel(
        model_id=BEDROCK_MODEL_ID,
        streaming=True,
    )


def create_agent(tables_metadata: list[dict]) -> Agent:
    """
    Create a Strands agent configured for analytics chat.

    Args:
        tables_metadata: List of table dicts from the catalog API.

    Returns:
        Configured Strands Agent ready to process messages.
    """
    system_prompt = build_system_prompt(tables_metadata)

    return Agent(
        model=_create_model(),
        system_prompt=system_prompt,
        tools=[execute_sql, display_chart],
    )


def run_agent(
    agent: Agent,
    message: str,
    conversation_history: list[dict] | None = None,
) -> dict:
    """
    Run the agent with a user message and optional conversation history.

    Args:
        agent: The Strands agent instance.
        message: The user's message.
        conversation_history: Previous messages for context.

    Returns:
        Dict with 'content' (list of text/chart blocks) and 'raw_response'.
    """
    # Build messages list from history
    messages = []
    if conversation_history:
        for msg in conversation_history:
            role = msg.get("role", "user")
            text = msg.get("text", "")
            if role in ("user", "assistant") and text:
                messages.append({"role": role, "content": [{"text": text}]})

    # Run the agent
    try:
        result = agent(message, messages=messages if messages else None)
    except Exception as e:
        logger.error(f"Agent error: {e}")
        return {
            "content": [{"type": "text", "text": f"Sorry, I encountered an error: {str(e)}"}],
        }

    # Parse the response into content blocks
    content_blocks = _parse_agent_response(result)

    return {"content": content_blocks}


def _parse_agent_response(result) -> list[dict]:
    """
    Parse Strands agent result into structured content blocks.

    Extracts text and chart tool results from the agent's response.
    """
    content_blocks = []

    # Get the text response
    response_text = str(result)
    if response_text:
        content_blocks.append({"type": "text", "text": response_text})

    # Extract chart tool results from the agent's tool use history
    if hasattr(result, "messages"):
        for msg in result.messages:
            if not isinstance(msg, dict):
                continue
            msg_content = msg.get("content", [])
            if not isinstance(msg_content, list):
                continue
            for block in msg_content:
                if not isinstance(block, dict):
                    continue
                # Look for tool results that are charts
                if block.get("type") == "toolResult":
                    tool_content = block.get("content", [])
                    if isinstance(tool_content, list):
                        for tc in tool_content:
                            if isinstance(tc, dict) and tc.get("type") == "text":
                                try:
                                    import json
                                    parsed = json.loads(tc["text"])
                                    if isinstance(parsed, dict) and parsed.get("type") == "chart":
                                        content_blocks.append(parsed)
                                except (json.JSONDecodeError, KeyError):
                                    pass

    # Ensure at least one content block
    if not content_blocks:
        content_blocks.append({
            "type": "text",
            "text": "I processed your request but didn't generate a response. Please try rephrasing.",
        })

    return content_blocks
