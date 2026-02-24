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
    "BEDROCK_MODEL_ID", "us.anthropic.claude-3-5-sonnet-20241022-v2:0"
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
    agent_messages: list[dict] | None = None,
) -> dict:
    """
    Run the agent with a user message and optional conversation history.

    Args:
        agent: The Strands agent instance.
        message: The user's message.
        agent_messages: Raw Strands messages from previous turns (includes tool use/results).

    Returns:
        Dict with 'content' (list of text/chart blocks) and 'agent_messages' (raw history).
    """
    # Run the agent with previous conversation context
    try:
        result = agent(message, messages=agent_messages if agent_messages else None)
    except Exception as e:
        logger.error(f"Agent error: {e}")
        return {
            "content": [{"type": "text", "text": f"Sorry, I encountered an error: {str(e)}"}],
            "agent_messages": agent_messages or [],
        }

    # Parse the response into content blocks using agent.messages (full history)
    content_blocks = _parse_agent_response(result, agent.messages)

    return {
        "content": content_blocks,
        "agent_messages": agent.messages,
    }


def _parse_agent_response(result, agent_messages: list[dict]) -> list[dict]:
    """
    Parse Strands agent result into structured content blocks.

    Extracts text from the final result and chart specs from tool results
    in the agent's full message history (Bedrock Converse API format).
    """
    import json

    content_blocks = []

    # Get the text response from the final result
    response_text = str(result)
    if response_text:
        content_blocks.append({"type": "text", "text": response_text})

    # Extract chart tool results from the agent's message history.
    # Bedrock Converse API format uses:
    #   {"toolResult": {"toolUseId": "...", "content": [{"json": {...}}], "status": "success"}}
    for msg in agent_messages:
        if not isinstance(msg, dict):
            continue
        msg_content = msg.get("content", [])
        if not isinstance(msg_content, list):
            continue
        for block in msg_content:
            if not isinstance(block, dict):
                continue
            # Bedrock Converse format: toolResult is a key, not a "type" value
            tool_result = block.get("toolResult")
            if not tool_result or not isinstance(tool_result, dict):
                continue
            tool_content = tool_result.get("content", [])
            if not isinstance(tool_content, list):
                continue
            for tc in tool_content:
                if not isinstance(tc, dict):
                    continue
                # Primary: JSON format (Bedrock native for dict returns)
                json_data = tc.get("json")
                if isinstance(json_data, dict) and json_data.get("type") == "chart":
                    content_blocks.append(json_data)
                    continue
                # Fallback: text format (JSON-serialized string)
                text_data = tc.get("text")
                if text_data:
                    try:
                        parsed = json.loads(text_data)
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
