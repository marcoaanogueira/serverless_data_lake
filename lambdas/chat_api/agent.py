"""
Strands Agent setup for analytics chat.

Creates a Bedrock-powered agent with SQL execution and auto-chart generation,
using table metadata from the Schema Registry as context.
Uses S3SessionManager for automatic conversation memory persistence.
"""

import logging
import os
from typing import List, Optional

from pydantic import BaseModel, Field
from strands import Agent
from strands.models import BedrockModel
from strands.session.s3_session_manager import S3SessionManager

from prompt import build_system_prompt
from tools import execute_sql

logger = logging.getLogger(__name__)

BEDROCK_MODEL_ID = os.environ.get(
    "BEDROCK_MODEL_ID", "us.anthropic.claude-sonnet-4-6"
)
SCHEMA_BUCKET = os.environ.get("SCHEMA_BUCKET", "")


class ChartSpec(BaseModel):
    chart_type: str = Field(description="bar, line, area, pie, or scatter")
    title: str
    x_key: str
    y_keys: List[str]
    data: List[dict]
    config: Optional[dict] = None


class AnalysisResponse(BaseModel):
    # TODO: Investigate extracting chart data directly from tool results instead of
    # having the LLM reproduce it in structured output. Current approach works but
    # the LLM re-serializes up to 50 rows of chart data as output tokens, which is
    # expensive. A hybrid approach (structured text + chart from tool result) could
    # reduce token cost significantly.
    analysis_text: str = Field(description="The explanation or insight about the data")
    chart: Optional[ChartSpec] = Field(description="The chart object if the data is visualizable")
    suggested_questions: List[str] = Field(description="2-3 follow-up questions")


def _create_model() -> BedrockModel:
    """Create the Bedrock model for the agent."""
    return BedrockModel(
        model_id=BEDROCK_MODEL_ID,
        streaming=True,
    )


def create_agent(tables_metadata: list[dict], session_id: str | None = None) -> Agent:
    """
    Create a Strands agent configured for analytics chat.

    When a session_id is provided, the agent uses S3SessionManager to
    automatically load previous conversation messages and persist new ones,
    giving the chat full multi-turn memory.

    Args:
        tables_metadata: List of table dicts from the catalog API.
        session_id: Chat session ID for conversation memory persistence.

    Returns:
        Configured Strands Agent ready to process messages.
    """
    system_prompt = build_system_prompt(tables_metadata)

    kwargs = dict(
        model=_create_model(),
        system_prompt=system_prompt,
        tools=[execute_sql],
        structured_output_model=AnalysisResponse,
    )

    if session_id and SCHEMA_BUCKET:
        kwargs["session_manager"] = S3SessionManager(
            session_id=session_id,
            bucket=SCHEMA_BUCKET,
            prefix="chat_sessions/",
        )

    return Agent(**kwargs)


def run_agent(agent: Agent, message: str) -> dict:
    """
    Run the agent with a user message.

    Conversation history is managed automatically by the S3SessionManager
    configured on the agent — previous messages are loaded on agent creation
    and new messages are persisted after each call.

    Args:
        agent: The Strands agent instance (with session_manager for memory).
        message: The user's message.

    Returns:
        Dict with 'content' (list of text/chart/suggestion blocks).
    """
    try:
        result = agent(message)
    except Exception as e:
        logger.error(f"Agent error: {e}")
        return {
            "content": [{"type": "text", "text": f"Sorry, I encountered an error: {str(e)}"}],
        }

    content_blocks = _parse_agent_response(result.structured_output)

    return {
        "content": content_blocks,
    }


def _parse_agent_response(result) -> list[dict]:
    """Parse the structured_output (Pydantic model) into frontend content blocks."""
    content_blocks = []

    if not result:
        return [{"type": "text", "text": "Sorry, I couldn't process that."}]

    res_dict = result.model_dump()

    if res_dict.get("analysis_text"):
        content_blocks.append({
            "type": "text",
            "text": res_dict["analysis_text"]
        })

    if res_dict.get("chart"):
        chart_data = res_dict["chart"]
        chart_data["type"] = "chart"
        content_blocks.append(chart_data)

    if res_dict.get("suggested_questions"):
        content_blocks.append({
            "type": "suggestions",
            "questions": res_dict["suggested_questions"]
        })

    return content_blocks
