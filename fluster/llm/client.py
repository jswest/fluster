"""generate_json — unified LLM interface with structured output and audit trail."""

import json
import sqlite3
from typing import TypeVar

from pydantic import BaseModel, ValidationError

from fluster.config.plan import LLMConfig, LLMProvider

T = TypeVar("T", bound=BaseModel)

_MAX_RETRIES = 3


def _call_openai(prompt: str, config: LLMConfig) -> str:
    """Call the OpenAI-compatible API and return raw text."""
    import openai

    client = openai.OpenAI()
    response = client.chat.completions.create(
        model=config.model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    return response.choices[0].message.content


def _call_ollama(prompt: str, config: LLMConfig) -> str:
    """Call the Ollama API and return raw text."""
    import ollama

    response = ollama.chat(
        model=config.model,
        messages=[{"role": "user", "content": prompt}],
        options={"temperature": 0},
    )
    return response.message.content


def _extract_json(text: str) -> str:
    """Extract JSON from LLM response text.

    Handles responses wrapped in markdown code blocks.
    """
    stripped = text.strip()
    if stripped.startswith("```"):
        lines = stripped.split("\n")
        # Remove first line (```json or ```) and last line (```)
        inner = "\n".join(lines[1:])
        if inner.rstrip().endswith("```"):
            inner = inner.rstrip()[:-3]
        return inner.strip()
    return stripped


def _log_call(
    conn: sqlite3.Connection | None,
    job_id: int | None,
    task_name: str,
    config: LLMConfig,
    input_json: str,
    raw_text: str | None,
    parsed_json: str | None,
) -> None:
    """Record an LLM call in the audit trail."""
    if conn is None:
        return
    conn.execute(
        "INSERT INTO llm_calls "
        "(job_id, task_name, provider, model, input_json, output_raw_text, output_parsed_json) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            job_id,
            task_name,
            config.provider.value,
            config.model,
            input_json,
            raw_text,
            parsed_json,
        ),
    )
    conn.commit()


def generate_json(
    task_name: str,
    schema_model: type[T],
    prompt: str,
    inputs: dict,
    config: LLMConfig,
    conn: sqlite3.Connection | None = None,
    job_id: int | None = None,
) -> T:
    """Call an LLM and parse the response into a Pydantic model.

    Args:
        task_name: Identifier for this type of LLM call (e.g. 'label_cluster').
        schema_model: Pydantic model class to validate the response against.
        prompt: The fully-formatted prompt string sent to the LLM.
        inputs: Dict of input context recorded in the audit trail (not interpolated into prompt).
        config: LLM configuration (provider, model).
        conn: Optional DB connection for audit logging.
        job_id: Optional job ID for audit logging.

    Returns:
        Validated Pydantic model instance.

    Raises:
        ValueError: If the LLM response cannot be parsed after retries.
    """
    if config.provider == LLMProvider.openai:
        call_fn = _call_openai
    elif config.provider == LLMProvider.ollama:
        call_fn = _call_ollama
    else:
        raise ValueError(f"Unknown provider: {config.provider}")

    input_json = json.dumps(inputs)

    last_error = None
    for attempt in range(_MAX_RETRIES):
        raw_text = call_fn(prompt, config)
        extracted = _extract_json(raw_text)

        try:
            parsed = json.loads(extracted)
            result = schema_model.model_validate(parsed)

            _log_call(
                conn, job_id, task_name, config,
                input_json, raw_text, json.dumps(parsed),
            )
            return result

        except (json.JSONDecodeError, ValidationError) as exc:
            last_error = exc
            _log_call(
                conn, job_id, task_name, config,
                input_json, raw_text, None,
            )

    raise ValueError(
        f"Failed to parse LLM response after {_MAX_RETRIES} attempts "
        f"for task '{task_name}': {last_error}"
    )
