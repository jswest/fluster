"""Tests for jobs + job_logs (Phase 4)."""

import json

import pytest

from fluster.db.connection import connect
from fluster.jobs.manager import (
    create_job,
    fail_job,
    get_job,
    get_job_logs,
    is_cancel_requested,
    log_job,
    mark_canceled,
    request_cancel,
    start_job,
    succeed_job,
    update_progress,
)


@pytest.fixture
def conn(tmp_path):
    c = connect(tmp_path)
    yield c
    c.close()


# --- Table existence ---

def test_jobs_table_exists(conn):
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert "jobs" in tables
    assert "job_logs" in tables


# --- Job lifecycle ---

def test_create_job(conn):
    job_id = create_job(conn, "ingest_rows", {"csv_path": "/tmp/data.csv"})
    job = get_job(conn, job_id)
    assert job["job_type"] == "ingest_rows"
    assert job["status"] == "queued"
    assert json.loads(job["input_params_json"]) == {"csv_path": "/tmp/data.csv"}
    assert job["created_at"] is not None


def test_start_job(conn):
    job_id = create_job(conn, "embed_items")
    start_job(conn, job_id)
    job = get_job(conn, job_id)
    assert job["status"] == "running"
    assert job["started_at"] is not None


def test_succeed_job(conn):
    job_id = create_job(conn, "embed_items")
    start_job(conn, job_id)
    succeed_job(conn, job_id)
    job = get_job(conn, job_id)
    assert job["status"] == "succeeded"
    assert job["finished_at"] is not None


def test_fail_job(conn):
    job_id = create_job(conn, "embed_items")
    start_job(conn, job_id)
    fail_job(conn, job_id, "Something went wrong")
    job = get_job(conn, job_id)
    assert job["status"] == "failed"
    assert job["error_message"] == "Something went wrong"
    assert job["finished_at"] is not None


def test_start_only_queued(conn):
    """Starting a non-queued job should be a no-op."""
    job_id = create_job(conn, "embed_items")
    start_job(conn, job_id)
    succeed_job(conn, job_id)

    # Try to start again — should not change status back to running.
    start_job(conn, job_id)
    job = get_job(conn, job_id)
    assert job["status"] == "succeeded"


# --- Cancellation ---

def test_request_cancel(conn):
    job_id = create_job(conn, "embed_items")
    start_job(conn, job_id)
    assert not is_cancel_requested(conn, job_id)

    request_cancel(conn, job_id)
    assert is_cancel_requested(conn, job_id)


def test_mark_canceled(conn):
    job_id = create_job(conn, "embed_items")
    start_job(conn, job_id)
    request_cancel(conn, job_id)
    mark_canceled(conn, job_id)
    job = get_job(conn, job_id)
    assert job["status"] == "canceled"
    assert job["finished_at"] is not None


def test_cancel_only_active_jobs(conn):
    """Canceling a finished job should be a no-op."""
    job_id = create_job(conn, "embed_items")
    start_job(conn, job_id)
    succeed_job(conn, job_id)

    request_cancel(conn, job_id)
    # cancel_requested_at should not be set on a succeeded job.
    assert not is_cancel_requested(conn, job_id)


def test_mark_canceled_only_active_jobs(conn):
    """Marking a finished job as canceled should be a no-op."""
    job_id = create_job(conn, "embed_items")
    start_job(conn, job_id)
    succeed_job(conn, job_id)

    mark_canceled(conn, job_id)
    job = get_job(conn, job_id)
    assert job["status"] == "succeeded"


# --- Progress ---

def test_update_progress(conn):
    job_id = create_job(conn, "embed_items")
    start_job(conn, job_id)
    update_progress(conn, job_id, {"done": 50, "total": 100})
    job = get_job(conn, job_id)
    assert json.loads(job["progress_json"]) == {"done": 50, "total": 100}


# --- Job logs ---

def test_log_job(conn):
    job_id = create_job(conn, "embed_items")
    log_job(conn, job_id, "Starting embedding", level="info")
    log_job(conn, job_id, "Batch 1 done", payload={"batch": 1})
    log_job(conn, job_id, "Something odd", level="warning")

    logs = get_job_logs(conn, job_id)
    assert len(logs) == 3
    assert logs[0]["message"] == "Starting embedding"
    assert logs[0]["level"] == "info"
    assert logs[1]["payload_json"] is not None
    assert json.loads(logs[1]["payload_json"]) == {"batch": 1}
    assert logs[2]["level"] == "warning"


def test_log_level_check(conn):
    job_id = create_job(conn, "embed_items")
    with pytest.raises(Exception):
        log_job(conn, job_id, "bad level", level="critical")


def test_job_status_check(conn):
    """Status CHECK constraint should reject invalid values."""
    with pytest.raises(Exception):
        conn.execute(
            "INSERT INTO jobs (job_type, status) VALUES (?, ?)",
            ("test", "invalid_status"),
        )


def test_get_nonexistent_job(conn):
    assert get_job(conn, 9999) is None
