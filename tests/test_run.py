"""Tests for run_pipeline orchestrator (Phase 16)."""

import json
from unittest.mock import MagicMock, call, patch

import pytest

from fluster.config import settings
from fluster.config.plan import load_plan
from fluster.config.project import project_dir
from fluster.db.connection import connect
from fluster.jobs.manager import create_job, get_job, start_job
from fluster.pipeline.run import PipelineCancelled, run_pipeline


@pytest.fixture
def conn(named_project):
    c = connect(project_dir(named_project))
    yield c
    c.close()


@pytest.fixture
def plan(named_project):
    return load_plan(project_dir(named_project) / settings.PLAN_YAML)


def _seed_cluster_run(conn, count=1):
    """Create fake reduction + cluster_run rows."""
    for _ in range(count):
        cur = conn.execute(
            "INSERT INTO reductions (embedding_reference, method, target_dimensions) "
            "VALUES ('test', 'umap', 8)"
        )
        conn.execute(
            "INSERT INTO cluster_runs (reduction_id, method, params_json) "
            "VALUES (?, 'hdbscan', '{}')",
            (cur.lastrowid,),
        )
    conn.commit()


_PIPELINE = "fluster.pipeline.run"


# --- Happy path ---


@patch(f"{_PIPELINE}.critique_clusters", return_value={"critiqued": True, "skipped": False})
@patch(f"{_PIPELINE}.label_clusters", return_value={"labeled": 2, "skipped": 0})
@patch(f"{_PIPELINE}.select_exemplars", return_value={"exemplars_created": 6, "skipped": 0})
@patch(f"{_PIPELINE}.cluster_items", return_value={"runs_created": 1, "skipped": 0})
@patch(f"{_PIPELINE}.reduce_items", return_value={"reductions_created": 2, "skipped": 0})
@patch(f"{_PIPELINE}.embed_items", return_value={"embedded": 10, "total": 10})
@patch(f"{_PIPELINE}.materialize_items", return_value={"materialized": 10, "skipped": 0})
def test_happy_path(
    mock_mat, mock_emb, mock_red, mock_clu, mock_exe, mock_lab, mock_cri,
    conn, plan,
):
    _seed_cluster_run(conn, count=1)
    job_id = create_job(conn, "full_run")
    start_job(conn, job_id)

    summary = run_pipeline(conn, project_dir("test-proj"), plan, job_id)

    assert summary["completed_steps"] == summary["total_steps"]
    assert summary["total_steps"] == 7  # 4 + 3*1
    mock_mat.assert_called_once()
    mock_emb.assert_called_once()
    mock_red.assert_called_once()
    mock_clu.assert_called_once()
    mock_exe.assert_called_once()
    mock_lab.assert_called_once()
    mock_cri.assert_called_once()


# --- Cancellation ---


@patch(f"{_PIPELINE}.critique_clusters")
@patch(f"{_PIPELINE}.label_clusters")
@patch(f"{_PIPELINE}.select_exemplars")
@patch(f"{_PIPELINE}.cluster_items", return_value={"runs_created": 1, "skipped": 0})
@patch(f"{_PIPELINE}.reduce_items", return_value={"reductions_created": 2, "skipped": 0})
@patch(f"{_PIPELINE}.embed_items", return_value={"embedded": 10, "total": 10})
@patch(f"{_PIPELINE}.materialize_items", return_value={"materialized": 10, "skipped": 0})
@patch(f"{_PIPELINE}.is_cancel_requested")
def test_cancellation_stops_pipeline(
    mock_cancel, mock_mat, mock_emb, mock_red, mock_clu, mock_exe, mock_lab, mock_cri,
    conn, plan,
):
    # Cancel after embed (second cancel check).
    mock_cancel.side_effect = [False, True]

    _seed_cluster_run(conn, count=1)
    job_id = create_job(conn, "full_run")
    start_job(conn, job_id)

    with pytest.raises(PipelineCancelled):
        run_pipeline(conn, project_dir("test-proj"), plan, job_id)

    # reduce should not have been called.
    mock_red.assert_not_called()

    # Job should be marked as canceled.
    row = get_job(conn, job_id)
    assert row["status"] == "canceled"


# --- Multiple cluster_runs ---


@patch(f"{_PIPELINE}.critique_clusters", return_value={"critiqued": True, "skipped": False})
@patch(f"{_PIPELINE}.label_clusters", return_value={"labeled": 2, "skipped": 0})
@patch(f"{_PIPELINE}.select_exemplars", return_value={"exemplars_created": 6, "skipped": 0})
@patch(f"{_PIPELINE}.cluster_items", return_value={"runs_created": 2, "skipped": 0})
@patch(f"{_PIPELINE}.reduce_items", return_value={"reductions_created": 2, "skipped": 0})
@patch(f"{_PIPELINE}.embed_items", return_value={"embedded": 10, "total": 10})
@patch(f"{_PIPELINE}.materialize_items", return_value={"materialized": 10, "skipped": 0})
def test_multiple_cluster_runs(
    mock_mat, mock_emb, mock_red, mock_clu, mock_exe, mock_lab, mock_cri,
    conn, plan,
):
    _seed_cluster_run(conn, count=2)
    job_id = create_job(conn, "full_run")
    start_job(conn, job_id)

    summary = run_pipeline(conn, project_dir("test-proj"), plan, job_id)

    assert summary["total_steps"] == 10  # 4 + 3*2
    assert mock_exe.call_count == 2
    assert mock_lab.call_count == 2
    assert mock_cri.call_count == 2


# --- Progress tracking ---


@patch(f"{_PIPELINE}.critique_clusters", return_value={"critiqued": True, "skipped": False})
@patch(f"{_PIPELINE}.label_clusters", return_value={"labeled": 2, "skipped": 0})
@patch(f"{_PIPELINE}.select_exemplars", return_value={"exemplars_created": 6, "skipped": 0})
@patch(f"{_PIPELINE}.cluster_items", return_value={"runs_created": 1, "skipped": 0})
@patch(f"{_PIPELINE}.reduce_items", return_value={"reductions_created": 2, "skipped": 0})
@patch(f"{_PIPELINE}.embed_items", return_value={"embedded": 10, "total": 10})
@patch(f"{_PIPELINE}.materialize_items", return_value={"materialized": 10, "skipped": 0})
def test_progress_updated(
    mock_mat, mock_emb, mock_red, mock_clu, mock_exe, mock_lab, mock_cri,
    conn, plan,
):
    _seed_cluster_run(conn, count=1)
    job_id = create_job(conn, "full_run")
    start_job(conn, job_id)

    run_pipeline(conn, project_dir("test-proj"), plan, job_id)

    row = get_job(conn, job_id)
    progress = json.loads(row["progress_json"])
    assert progress["completed_steps"] == progress["total_steps"]
    assert progress["step"] == "critique"


# --- embed_items receives job_id ---


@patch(f"{_PIPELINE}.critique_clusters", return_value={"critiqued": True, "skipped": False})
@patch(f"{_PIPELINE}.label_clusters", return_value={"labeled": 2, "skipped": 0})
@patch(f"{_PIPELINE}.select_exemplars", return_value={"exemplars_created": 6, "skipped": 0})
@patch(f"{_PIPELINE}.cluster_items", return_value={"runs_created": 1, "skipped": 0})
@patch(f"{_PIPELINE}.reduce_items", return_value={"reductions_created": 2, "skipped": 0})
@patch(f"{_PIPELINE}.embed_items", return_value={"embedded": 10, "total": 10})
@patch(f"{_PIPELINE}.materialize_items", return_value={"materialized": 10, "skipped": 0})
def test_embed_receives_job_id(
    mock_mat, mock_emb, mock_red, mock_clu, mock_exe, mock_lab, mock_cri,
    conn, plan,
):
    _seed_cluster_run(conn, count=1)
    job_id = create_job(conn, "full_run")
    start_job(conn, job_id)

    run_pipeline(conn, project_dir("test-proj"), plan, job_id)

    _, kwargs = mock_emb.call_args
    assert kwargs["job_id"] == job_id


# --- Exception propagates ---


@patch(f"{_PIPELINE}.cluster_items", side_effect=ValueError("bad reduction"))
@patch(f"{_PIPELINE}.reduce_items", return_value={"reductions_created": 2, "skipped": 0})
@patch(f"{_PIPELINE}.embed_items", return_value={"embedded": 10, "total": 10})
@patch(f"{_PIPELINE}.materialize_items", return_value={"materialized": 10, "skipped": 0})
def test_exception_propagates(
    mock_mat, mock_emb, mock_red, mock_clu,
    conn, plan,
):
    job_id = create_job(conn, "full_run")
    start_job(conn, job_id)

    with pytest.raises(ValueError, match="bad reduction"):
        run_pipeline(conn, project_dir("test-proj"), plan, job_id)

    # embed was called, but cluster raised before exemplars.
    mock_emb.assert_called_once()
