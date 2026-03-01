"""Tests for generate_json LLM interface (Phase 12)."""

import json
from unittest.mock import patch

import pytest
from pydantic import BaseModel

from fluster.config.plan import LLMConfig, LLMProvider
from fluster.db.connection import connect
from fluster.llm.client import generate_json, _extract_json, _MAX_RETRIES


# --- Test schema models ---

class LabelOutput(BaseModel):
    label: str
    confidence: float


class SimpleOutput(BaseModel):
    answer: str


# --- Fixtures ---

@pytest.fixture
def project(tmp_path):
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    conn = connect(tmp_path)
    yield tmp_path, conn
    conn.close()


def _openai_config():
    return LLMConfig(provider=LLMProvider.openai, model="gpt-5-mini")


def _ollama_config():
    return LLMConfig(provider=LLMProvider.ollama, model="llama3")


# --- Basic generation ---

@patch("fluster.llm.client._call_openai")
def test_generate_json_returns_parsed_model(mock_call):
    mock_call.return_value = '{"label": "Science", "confidence": 0.95}'

    result = generate_json(
        task_name="label_cluster",
        schema_model=LabelOutput,
        prompt="Label this cluster.",
        inputs={"cluster_id": 1},
        config=_openai_config(),
    )

    assert isinstance(result, LabelOutput)
    assert result.label == "Science"
    assert result.confidence == 0.95


@patch("fluster.llm.client._call_ollama")
def test_generate_json_ollama_provider(mock_call):
    mock_call.return_value = '{"answer": "42"}'

    result = generate_json(
        task_name="test_task",
        schema_model=SimpleOutput,
        prompt="What is the answer?",
        inputs={},
        config=_ollama_config(),
    )

    assert result.answer == "42"


# --- JSON extraction ---

def test_extract_json_plain():
    assert _extract_json('{"a": 1}') == '{"a": 1}'


def test_extract_json_code_block():
    text = '```json\n{"a": 1}\n```'
    assert _extract_json(text) == '{"a": 1}'


def test_extract_json_code_block_no_lang():
    text = '```\n{"a": 1}\n```'
    assert _extract_json(text) == '{"a": 1}'


def test_extract_json_with_whitespace():
    text = '  \n  {"a": 1}  \n  '
    assert _extract_json(text) == '{"a": 1}'


# --- Audit trail ---

@patch("fluster.llm.client._call_openai")
def test_generate_json_logs_to_db(mock_call, project):
    pdir, conn = project
    mock_call.return_value = '{"label": "Art", "confidence": 0.8}'

    generate_json(
        task_name="label_cluster",
        schema_model=LabelOutput,
        prompt="Label this.",
        inputs={"cluster_id": 5},
        config=_openai_config(),
        conn=conn,
    )

    row = conn.execute("SELECT * FROM llm_calls").fetchone()
    assert row is not None
    assert row["task_name"] == "label_cluster"
    assert row["provider"] == "openai"
    assert row["model"] == "gpt-5-mini"
    assert json.loads(row["input_json"]) == {"cluster_id": 5}
    assert row["output_raw_text"] == '{"label": "Art", "confidence": 0.8}'
    assert json.loads(row["output_parsed_json"]) == {"label": "Art", "confidence": 0.8}


@patch("fluster.llm.client._call_openai")
def test_generate_json_logs_job_id(mock_call, project):
    pdir, conn = project
    mock_call.return_value = '{"answer": "yes"}'

    from fluster.jobs.manager import create_job
    job_id = create_job(conn, "test_job")

    generate_json(
        task_name="test",
        schema_model=SimpleOutput,
        prompt="Test",
        inputs={},
        config=_openai_config(),
        conn=conn,
        job_id=job_id,
    )

    row = conn.execute("SELECT * FROM llm_calls").fetchone()
    assert row["job_id"] == job_id


# --- Retries ---

@patch("fluster.llm.client._call_openai")
def test_generate_json_retries_on_invalid_json(mock_call, project):
    pdir, conn = project
    mock_call.side_effect = [
        "not json at all",
        '{"label": "Science", "confidence": 0.9}',
    ]

    result = generate_json(
        task_name="label",
        schema_model=LabelOutput,
        prompt="Label.",
        inputs={},
        config=_openai_config(),
        conn=conn,
    )

    assert result.label == "Science"
    assert mock_call.call_count == 2

    # Both calls should be logged.
    logs = conn.execute("SELECT * FROM llm_calls ORDER BY llm_call_id").fetchall()
    assert len(logs) == 2
    assert logs[0]["output_parsed_json"] is None  # failed parse
    assert logs[1]["output_parsed_json"] is not None  # succeeded


@patch("fluster.llm.client._call_openai")
def test_generate_json_retries_on_schema_mismatch(mock_call):
    mock_call.side_effect = [
        '{"wrong_field": "value"}',
        '{"label": "Art", "confidence": 0.5}',
    ]

    result = generate_json(
        task_name="label",
        schema_model=LabelOutput,
        prompt="Label.",
        inputs={},
        config=_openai_config(),
    )

    assert result.label == "Art"
    assert mock_call.call_count == 2


@patch("fluster.llm.client._call_openai")
def test_generate_json_raises_after_max_retries(mock_call):
    mock_call.return_value = "not json"

    with pytest.raises(ValueError, match=f"after {_MAX_RETRIES} attempts"):
        generate_json(
            task_name="label",
            schema_model=LabelOutput,
            prompt="Label.",
            inputs={},
            config=_openai_config(),
        )

    assert mock_call.call_count == _MAX_RETRIES


# --- No DB logging when conn is None ---

@patch("fluster.llm.client._call_openai")
def test_generate_json_works_without_conn(mock_call):
    mock_call.return_value = '{"answer": "hello"}'

    result = generate_json(
        task_name="test",
        schema_model=SimpleOutput,
        prompt="Say hello.",
        inputs={},
        config=_openai_config(),
    )

    assert result.answer == "hello"
