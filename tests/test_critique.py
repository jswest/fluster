"""Tests for critique_clusters (Phase 14)."""

import json
from unittest.mock import patch

import pytest

from fluster.config.plan import LLMConfig, LLMProvider, Plan
from fluster.db.connection import connect
from fluster.pipeline.cluster import cluster_items
from fluster.pipeline.critique import critique_clusters, _compute_metrics
from fluster.pipeline.embed import embed_items
from fluster.pipeline.exemplars import select_exemplars
from fluster.pipeline.ingest import ingest_rows
from fluster.pipeline.label import label_clusters
from fluster.pipeline.materialize import materialize_items
from fluster.pipeline.reduce import reduce_items


@pytest.fixture
def project(tmp_path):
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    conn = connect(tmp_path)
    yield tmp_path, conn
    conn.close()


def _write_csv(path, header, *rows):
    csv_file = path / "data.csv"
    csv_file.write_text("\n".join([header] + list(rows)) + "\n")
    return csv_file


def _mock_label_response():
    return json.dumps({
        "label": "Science Topics",
        "short_label": "Science",
        "rationale": "Items discuss scientific concepts.",
        "keywords": ["science", "physics"],
    })


def _mock_critique_response():
    return json.dumps({
        "verdict": "Good clustering with clear separation between topics.",
        "quality_score": 0.85,
        "recommendations": [
            "Consider lowering min_cluster_size to capture smaller groups.",
            "Try different UMAP parameters for better separation.",
        ],
    })


def _llm_config():
    return LLMConfig(provider=LLMProvider.openai, model="gpt-5-mini")


def _setup_with_exemplars(pdir, conn, count=30):
    """Run full pipeline through labeling, return cluster_run_id."""
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


# --- Basic critique ---

@patch("fluster.llm.client._call_openai")
def test_critique_creates_entry(mock_call, project):
    pdir, conn = project
    run_id = _setup_with_exemplars(pdir, conn)

    # Label first, then critique.
    mock_call.return_value = _mock_label_response()
    label_clusters(conn, run_id, _llm_config())

    mock_call.return_value = _mock_critique_response()
    summary = critique_clusters(conn, run_id, _llm_config())

    assert summary["critiqued"] is True
    assert summary["skipped"] is False

    row = conn.execute(
        "SELECT * FROM cluster_run_critiques WHERE cluster_run_id = ?",
        (run_id,),
    ).fetchone()
    assert row is not None


@patch("fluster.llm.client._call_openai")
def test_critique_stores_correct_fields(mock_call, project):
    pdir, conn = project
    run_id = _setup_with_exemplars(pdir, conn)

    mock_call.return_value = _mock_label_response()
    label_clusters(conn, run_id, _llm_config())

    mock_call.return_value = _mock_critique_response()
    critique_clusters(conn, run_id, _llm_config())

    row = conn.execute(
        "SELECT critique_json FROM cluster_run_critiques WHERE cluster_run_id = ?",
        (run_id,),
    ).fetchone()
    data = json.loads(row["critique_json"])

    assert data["verdict"] == "Good clustering with clear separation between topics."
    assert data["quality_score"] == 0.85
    assert len(data["recommendations"]) == 2
    assert "metrics" in data
    assert data["metrics"]["total_items"] == 30


# --- Metrics ---

def test_metrics_are_computed(project):
    pdir, conn = project
    run_id = _setup_with_exemplars(pdir, conn)

    metrics = _compute_metrics(conn, run_id)

    assert metrics["total_items"] == 30
    assert metrics["n_clusters"] >= 1
    assert 0.0 <= metrics["noise_fraction"] <= 1.0
    assert metrics["size_min"] >= 1
    assert metrics["size_max"] >= metrics["size_min"]


def test_metrics_include_silhouette(project):
    pdir, conn = project
    run_id = _setup_with_exemplars(pdir, conn)

    metrics = _compute_metrics(conn, run_id)

    # With 30 items and HDBSCAN, we should get at least 2 clusters.
    n_clusters = metrics["n_clusters"]
    if n_clusters >= 2:
        assert metrics["silhouette"] is not None
        assert -1.0 <= metrics["silhouette"] <= 1.0


# --- Idempotency ---

@patch("fluster.llm.client._call_openai")
def test_critique_is_idempotent(mock_call, project):
    pdir, conn = project
    run_id = _setup_with_exemplars(pdir, conn)

    mock_call.return_value = _mock_label_response()
    label_clusters(conn, run_id, _llm_config())

    mock_call.return_value = _mock_critique_response()
    critique_clusters(conn, run_id, _llm_config())
    call_count_after_first = mock_call.call_count

    # Second call should skip.
    summary = critique_clusters(conn, run_id, _llm_config())
    assert summary["critiqued"] is False
    assert summary["skipped"] is True
    assert mock_call.call_count == call_count_after_first


# --- Audit trail ---

@patch("fluster.llm.client._call_openai")
def test_critique_logs_llm_call(mock_call, project):
    pdir, conn = project
    run_id = _setup_with_exemplars(pdir, conn)

    mock_call.return_value = _mock_label_response()
    label_clusters(conn, run_id, _llm_config())

    mock_call.return_value = _mock_critique_response()
    critique_clusters(conn, run_id, _llm_config())

    logs = conn.execute(
        "SELECT * FROM llm_calls WHERE task_name = 'critique_clusters'"
    ).fetchall()
    assert len(logs) == 1
    assert logs[0]["provider"] == "openai"


# --- Without labels ---

@patch("fluster.llm.client._call_openai")
def test_critique_works_without_labels(mock_call, project):
    """Critique should work even if clusters haven't been labeled yet."""
    pdir, conn = project
    run_id = _setup_with_exemplars(pdir, conn)

    mock_call.return_value = _mock_critique_response()
    summary = critique_clusters(conn, run_id, _llm_config())

    assert summary["critiqued"] is True

    row = conn.execute(
        "SELECT critique_json FROM cluster_run_critiques WHERE cluster_run_id = ?",
        (run_id,),
    ).fetchone()
    data = json.loads(row["critique_json"])
    assert "verdict" in data
