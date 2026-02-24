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
from tools import execute_sql

logger = logging.getLogger(__name__)

BEDROCK_MODEL_ID = os.environ.get(
    "BEDROCK_MODEL_ID", "us.anthropic.claude-3-5-sonnet-20241022-v2:0"
)


from pydantic import BaseModel, Field
from typing import List, Optional

class ChartSpec(BaseModel):
    chart_type: str = Field(description="bar, line, area, pie, or scatter")
    title: str
    x_key: str
    y_keys: List[str]
    data: List[dict]
    config: Optional[dict] = None

class AnalysisResponse(BaseModel):
    analysis_text: str = Field(description="A explicação ou insight sobre os dados")
    chart: Optional[ChartSpec] = Field(description="O objeto de gráfico se os dados forem visualizáveis")
    suggested_questions: List[str] = Field(description="2-3 perguntas de follow-up")


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
        tools=[execute_sql], # A SQL continua como ferramenta
        structured_output_model=AnalysisResponse, # A MÁGICA ACONTECE AQUI
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
    content_blocks = _parse_agent_response(result.structured_output)

    return {
        "content": content_blocks,
        "agent_messages": agent.messages,
    }


def _parse_agent_response(result) -> list[dict]:
    """
    Parse o structured_output (Pydantic model) para blocos do frontend.
    """
    content_blocks = []
    
    if not result:
        return [{"type": "text", "text": "Sorry, I couldn't process that."}]

    # Converte o Pydantic para dicionário
    res_dict = result.model_dump()

    # 1. Texto de análise
    if res_dict.get("analysis_text"):
        content_blocks.append({
            "type": "text",
            "text": res_dict["analysis_text"]
        })

    # 2. Gráfico
    if res_dict.get("chart"):
        chart_data = res_dict["chart"]
        chart_data["type"] = "chart" # Para o seu React identificar
        content_blocks.append(chart_data)

    # 3. Sugestões
    if res_dict.get("suggested_questions"):
        content_blocks.append({
            "type": "suggestions",
            "questions": res_dict["suggested_questions"]
        })

    return content_blocks