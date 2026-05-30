"""Tests for reconcile_labels (Issue #21)."""

import json
from unittest.mock import patch

import pytest

from fluster.config.plan import LLMConfig, LLMProvider, Plan
from fluster.pipeline.cluster import cluster_items
from fluster.pipeline.embed import embed_items
from fluster.pipeline.exemplars import select_exemplars
from fluster.pipeline.ingest import ingest_rows
from fluster.pipeline.label import label_clusters
from fluster.pipeline.materialize import materialize_items
from fluster.pipeline.reconcile import ReconcileOutput, reconcile_labels
from fluster.pipeline.reduce import reduce_items


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


def _mock_reconcile_response(cluster_ids):
    return json.dumps({
        "clusters": [
            {
                "cluster_id": cid,
                "label": f"Topic {cid}",
                "short_label": f"T{cid}",
                "keywords": ["alpha", "beta", "gamma"],
                "reconcile_rationale": "Renamed to disambiguate from siblings.",
            }
            for cid in cluster_ids
        ]
    })


def _llm_config():
    return LLMConfig(provider=LLMProvider.openai, model="gpt-5-mini")


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


def _cluster_ids(conn, run_id):
    rows = conn.execute(
        "SELECT DISTINCT cluster_id FROM cluster_assignments "
        "WHERE cluster_run_id = ? AND cluster_id >= 0 ORDER BY cluster_id",
        (run_id,),
    ).fetchall()
    return [r["cluster_id"] for r in rows]


def _label_run(mock_call, pdir, conn):
    """Set up, label every cluster, and return (run_id, cluster_ids).

    Skips the test when clustering produced fewer than two clusters — there is
    nothing for reconciliation to disambiguate.
    """
    run_id = _setup_with_exemplars(pdir, conn)
    mock_call.return_value = _mock_label_response()
    label_clusters(conn, run_id, _llm_config())
    ids = _cluster_ids(conn, run_id)
    if len(ids) < 2:
        pytest.skip("clustering produced fewer than two clusters")
    return run_id, ids


# --- Basic reconciliation ---

@patch("fluster.llm.client._call_openai")
def test_reconcile_updates_labels(mock_call, project):
    pdir, conn = project
    run_id, ids = _label_run(mock_call, pdir, conn)

    mock_call.return_value = _mock_reconcile_response(ids)
    summary = reconcile_labels(conn, run_id, _llm_config())

    assert summary["skipped"] is False
    assert summary["reconciled"] == len(ids)

    for cid in ids:
        row = conn.execute(
            "SELECT label, label_json FROM cluster_summaries "
            "WHERE cluster_run_id = ? AND cluster_id = ?",
            (run_id, cid),
        ).fetchone()
        assert row["label"] == f"Topic {cid}"

        data = json.loads(row["label_json"])
        assert data["label"] == f"Topic {cid}"
        assert data["short_label"] == f"T{cid}"
        assert data["reconciled"] is True
        assert data["reconcile_rationale"] == "Renamed to disambiguate from siblings."
        # The original per-cluster label is preserved for provenance.
        assert data["original"]["label"] == "Science Topics"


# --- Idempotency ---

@patch("fluster.llm.client._call_openai")
def test_reconcile_is_idempotent(mock_call, project):
    pdir, conn = project
    run_id, ids = _label_run(mock_call, pdir, conn)

    mock_call.return_value = _mock_reconcile_response(ids)
    reconcile_labels(conn, run_id, _llm_config())
    call_count_after_first = mock_call.call_count

    summary = reconcile_labels(conn, run_id, _llm_config())
    assert summary["skipped"] is True
    assert summary["reconciled"] == 0
    assert mock_call.call_count == call_count_after_first


# --- Prompt content ---

@patch("fluster.llm.client._call_openai")
def test_reconcile_prompt_includes_all_clusters(mock_call, project):
    pdir, conn = project
    run_id, ids = _label_run(mock_call, pdir, conn)

    mock_call.return_value = _mock_reconcile_response(ids)
    reconcile_labels(conn, run_id, _llm_config())

    prompt_sent = mock_call.call_args_list[-1][0][0]
    for cid in ids:
        assert f"Cluster {cid}" in prompt_sent
    # Every cluster's current label info is shown.
    assert "Science Topics" in prompt_sent


# --- Audit trail ---

@patch("fluster.llm.client._call_openai")
def test_reconcile_logs_llm_call(mock_call, project):
    pdir, conn = project
    run_id, ids = _label_run(mock_call, pdir, conn)

    mock_call.return_value = _mock_reconcile_response(ids)
    reconcile_labels(conn, run_id, _llm_config())

    logs = conn.execute(
        "SELECT * FROM llm_calls WHERE task_name = 'reconcile_labels'"
    ).fetchall()
    assert len(logs) == 1
    assert logs[0]["provider"] == "openai"
    inputs = json.loads(logs[0]["input_json"])
    assert inputs["cluster_run_id"] == run_id
    assert inputs["n_clusters"] == len(ids)


# --- Downstream consumers see the reconciled label ---

@patch("fluster.llm.client._call_openai")
def test_reconcile_visible_to_latest_row_query(mock_call, project):
    pdir, conn = project
    run_id, ids = _label_run(mock_call, pdir, conn)

    mock_call.return_value = _mock_reconcile_response(ids)
    reconcile_labels(conn, run_id, _llm_config())

    # The MAX(cluster_summary_id) resolution used by export.py / server.py.
    rows = conn.execute(
        "SELECT cluster_id, label FROM cluster_summaries "
        "WHERE cluster_run_id = ? AND cluster_summary_id IN ("
        "  SELECT MAX(cluster_summary_id) FROM cluster_summaries "
        "  WHERE cluster_run_id = ? GROUP BY cluster_id"
        ")",
        (run_id, run_id),
    ).fetchall()
    labels = {r["cluster_id"]: r["label"] for r in rows}
    for cid in ids:
        assert labels[cid] == f"Topic {cid}"


# --- Skips when there is nothing to disambiguate ---

@patch("fluster.llm.client._call_openai")
def test_reconcile_skips_single_cluster(mock_call, project):
    pdir, conn = project
    run_id, ids = _label_run(mock_call, pdir, conn)

    # Reduce the run to a single labeled cluster.
    keep = ids[0]
    conn.execute(
        "DELETE FROM cluster_summaries WHERE cluster_run_id = ? AND cluster_id != ?",
        (run_id, keep),
    )
    conn.commit()
    call_count_before = mock_call.call_count

    summary = reconcile_labels(conn, run_id, _llm_config())
    assert summary["skipped"] is True
    assert summary["reconciled"] == 0
    assert mock_call.call_count == call_count_before


# --- Schema validation ---

def test_reconcile_output_schema():
    data = {
        "clusters": [
            {
                "cluster_id": 0,
                "label": "Domestic Felines",
                "short_label": "Felines",
                "keywords": ["cat", "feline", "pet"],
                "reconcile_rationale": "Aligned with 'Domestic Canines'.",
            }
        ]
    }
    result = ReconcileOutput.model_validate(data)
    assert result.clusters[0].cluster_id == 0
    assert result.clusters[0].label == "Domestic Felines"
