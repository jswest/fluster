"""Tests for label_clusters (Phase 13)."""

import json
from unittest.mock import patch

import pytest

from fluster.config.plan import LLMConfig, LLMProvider, Plan
from fluster.db.connection import connect
from fluster.pipeline.cluster import cluster_items
from fluster.pipeline.embed import embed_items
from fluster.pipeline.exemplars import select_exemplars
from fluster.pipeline.ingest import ingest_rows
from fluster.pipeline.label import label_clusters, ClusterLabel
from fluster.pipeline.materialize import materialize_items
from fluster.pipeline.reduce import reduce_items



def _write_csv(path, header, *rows):
    csv_file = path / "data.csv"
    csv_file.write_text("\n".join([header] + list(rows)) + "\n")
    return csv_file


def _setup_with_exemplars(pdir, conn, count=30):
    """Run full pipeline through exemplar selection, return cluster_run_id."""
    for i in range(count):
        (pdir / f"item{i}.txt").write_text(
            f"Item {i} about {'science and physics' if i % 2 == 0 else 'art and painting'} topic {i}."
        )

    rows = [f"item{i},{pdir}/item{i}.txt" for i in range(count)]
    csv_file = _write_csv(pdir, "name,file_path", *rows)
    ingest_rows(conn, csv_file, pdir)
    materialize_items(conn, pdir)
    plan = Plan()
    embed_items(conn, plan)
    reduce_items(conn, plan)
    cluster_items(conn, plan)

    run = conn.execute("SELECT cluster_run_id FROM cluster_runs LIMIT 1").fetchone()
    run_id = run["cluster_run_id"]
    select_exemplars(conn, run_id)
    return run_id


def _mock_label_response():
    return json.dumps({
        "label": "Science Topics",
        "short_label": "Science",
        "rationale": "Items discuss physics and scientific concepts.",
        "keywords": ["science", "physics", "research"],
    })


def _llm_config():
    return LLMConfig(provider=LLMProvider.openai, model="gpt-5-mini")


# --- Basic labeling ---

@patch("fluster.llm.client._call_openai")
def test_label_creates_summaries(mock_call, project):
    pdir, conn = project
    run_id = _setup_with_exemplars(pdir, conn)
    mock_call.return_value = _mock_label_response()

    summary = label_clusters(conn, run_id, _llm_config())

    assert summary["labeled"] > 0
    assert summary["skipped"] == 0

    summaries = conn.execute(
        "SELECT * FROM cluster_summaries WHERE cluster_run_id = ?",
        (run_id,),
    ).fetchall()
    assert len(summaries) > 0


@patch("fluster.llm.client._call_openai")
def test_label_stores_correct_fields(mock_call, project):
    pdir, conn = project
    run_id = _setup_with_exemplars(pdir, conn)
    mock_call.return_value = _mock_label_response()

    label_clusters(conn, run_id, _llm_config())

    row = conn.execute("SELECT * FROM cluster_summaries LIMIT 1").fetchone()
    assert row["label"] == "Science Topics"
    assert row["cluster_run_id"] == run_id

    label_data = json.loads(row["label_json"])
    assert label_data["label"] == "Science Topics"
    assert label_data["short_label"] == "Science"
    assert label_data["rationale"] == "Items discuss physics and scientific concepts."
    assert label_data["keywords"] == ["science", "physics", "research"]


@patch("fluster.llm.client._call_openai")
def test_label_calls_llm_per_cluster(mock_call, project):
    pdir, conn = project
    run_id = _setup_with_exemplars(pdir, conn)
    mock_call.return_value = _mock_label_response()

    label_clusters(conn, run_id, _llm_config())

    # Should have called LLM once per non-noise cluster.
    n_clusters = conn.execute(
        "SELECT COUNT(DISTINCT cluster_id) FROM cluster_assignments "
        "WHERE cluster_run_id = ? AND cluster_id >= 0",
        (run_id,),
    ).fetchone()[0]
    assert mock_call.call_count == n_clusters


# --- Idempotency ---

@patch("fluster.llm.client._call_openai")
def test_label_is_idempotent(mock_call, project):
    pdir, conn = project
    run_id = _setup_with_exemplars(pdir, conn)
    mock_call.return_value = _mock_label_response()

    label_clusters(conn, run_id, _llm_config())
    first_count = mock_call.call_count

    # Second call should skip all clusters.
    summary = label_clusters(conn, run_id, _llm_config())
    assert summary["labeled"] == 0
    assert summary["skipped"] > 0

    # No additional LLM calls.
    assert mock_call.call_count == first_count


# --- Audit trail ---

@patch("fluster.llm.client._call_openai")
def test_label_logs_llm_calls(mock_call, project):
    pdir, conn = project
    run_id = _setup_with_exemplars(pdir, conn)
    mock_call.return_value = _mock_label_response()

    label_clusters(conn, run_id, _llm_config())

    logs = conn.execute(
        "SELECT * FROM llm_calls WHERE task_name = 'label_cluster'"
    ).fetchall()
    assert len(logs) > 0

    for log in logs:
        assert log["provider"] == "openai"
        assert log["model"] == "gpt-5-mini"
        inputs = json.loads(log["input_json"])
        assert "cluster_run_id" in inputs
        assert "cluster_id" in inputs


# --- Job ID tracking ---

@patch("fluster.llm.client._call_openai")
def test_label_passes_job_id(mock_call, project):
    pdir, conn = project
    run_id = _setup_with_exemplars(pdir, conn)
    mock_call.return_value = _mock_label_response()

    from fluster.jobs.manager import create_job
    job_id = create_job(conn, "label")

    label_clusters(conn, run_id, _llm_config(), job_id=job_id)

    log = conn.execute("SELECT * FROM llm_calls LIMIT 1").fetchone()
    assert log["job_id"] == job_id


# --- Schema validation ---

def test_cluster_label_schema():
    data = {
        "label": "Test",
        "short_label": "T",
        "rationale": "Because.",
        "keywords": ["a", "b"],
    }
    result = ClusterLabel.model_validate(data)
    assert result.label == "Test"
    assert result.keywords == ["a", "b"]


# --- Prompt content ---

@patch("fluster.llm.client._call_openai")
def test_label_prompt_includes_exemplars(mock_call, project):
    pdir, conn = project
    run_id = _setup_with_exemplars(pdir, conn)
    mock_call.return_value = _mock_label_response()

    label_clusters(conn, run_id, _llm_config())

    # Check that the prompt sent to the LLM contains exemplar text.
    prompt_sent = mock_call.call_args_list[0][0][0]
    assert "Exemplar" in prompt_sent
    assert "Cluster size:" in prompt_sent
