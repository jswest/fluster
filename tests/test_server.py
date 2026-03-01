"""Tests for the FastAPI server (Phase 15)."""

import json

import pytest
from fastapi.testclient import TestClient

from fluster.config.project import project_dir
from fluster.db.connection import connect
from fluster.jobs.manager import create_job, start_job, succeed_job
from fluster.server import create_app


@pytest.fixture
def client(named_project):
    app = create_app(named_project)
    return TestClient(app)


# --- Health ---


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["project_name"] == "test-proj"
    assert "version" in data


# --- POST /jobs ---


def test_create_job(client):
    resp = client.post("/jobs", json={"job_type": "full_run"})
    assert resp.status_code == 201
    data = resp.json()
    assert "job_id" in data
    assert data["job_id"] >= 1


def test_create_job_conflict(client):
    client.post("/jobs", json={"job_type": "full_run"})
    resp = client.post("/jobs", json={"job_type": "full_run"})
    assert resp.status_code == 409


# --- GET /jobs/{job_id} ---


def test_get_job(client):
    create_resp = client.post("/jobs", json={
        "job_type": "full_run",
        "input_params": {"foo": "bar"},
    })
    job_id = create_resp.json()["job_id"]

    resp = client.get(f"/jobs/{job_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["job_id"] == job_id
    assert data["job_type"] == "full_run"
    assert data["status"] == "queued"
    assert data["input_params"] == {"foo": "bar"}
    assert data["progress"] == {}
    assert data["created_at"] is not None


def test_get_job_not_found(client):
    resp = client.get("/jobs/9999")
    assert resp.status_code == 404


# --- POST /jobs/{job_id}/cancel ---


def test_cancel_job(client):
    create_resp = client.post("/jobs", json={"job_type": "full_run"})
    job_id = create_resp.json()["job_id"]

    resp = client.post(f"/jobs/{job_id}/cancel")
    assert resp.status_code == 200
    data = resp.json()
    assert data["job_id"] == job_id
    assert data["cancel_requested"] is True


def test_cancel_finished_job(client, named_project):
    # Create and finish a job directly via the manager.
    conn = connect(project_dir(named_project))
    job_id = create_job(conn, "full_run")
    start_job(conn, job_id)
    succeed_job(conn, job_id)
    conn.close()

    resp = client.post(f"/jobs/{job_id}/cancel")
    assert resp.status_code == 200
    assert resp.json()["cancel_requested"] is False


def test_cancel_job_not_found(client):
    resp = client.post("/jobs/9999/cancel")
    assert resp.status_code == 404


# --- GET /cluster-runs ---


def test_list_cluster_runs_empty(client):
    resp = client.get("/cluster-runs")
    assert resp.status_code == 200
    assert resp.json() == []


# --- GET /cluster-runs/{id} ---


def test_get_cluster_run_not_found(client):
    resp = client.get("/cluster-runs/9999")
    assert resp.status_code == 404


def test_get_cluster_run_detail(client, named_project):
    """Seed a minimal cluster run and verify the detail endpoint."""
    conn = connect(project_dir(named_project))

    # Create a reduction for the cluster run to reference.
    conn.execute(
        "INSERT INTO reductions (embedding_reference, method, target_dimensions) "
        "VALUES ('test', 'umap', 8)"
    )
    reduction_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Create a cluster run.
    conn.execute(
        "INSERT INTO cluster_runs (reduction_id, method, params_json) "
        "VALUES (?, 'hdbscan', '{}')",
        (reduction_id,),
    )
    run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Add a row + item for assignments.
    conn.execute("INSERT INTO rows (row_name) VALUES ('r1')")
    row_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute("INSERT INTO items (row_id) VALUES (?)", (row_id,))
    item_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    conn.execute(
        "INSERT INTO cluster_assignments (cluster_run_id, item_id, cluster_id, membership_probability) "
        "VALUES (?, ?, 0, 0.95)",
        (run_id, item_id),
    )

    # Add a label.
    label_data = json.dumps({
        "label": "Test Cluster",
        "short_label": "Test",
        "rationale": "For testing.",
        "keywords": ["test"],
    })
    conn.execute(
        "INSERT INTO cluster_summaries (cluster_run_id, cluster_id, label, label_json) "
        "VALUES (?, 0, 'Test Cluster', ?)",
        (run_id, label_data),
    )

    # Add a critique.
    critique_data = json.dumps({
        "verdict": "Good.",
        "quality_score": 0.9,
        "recommendations": ["None."],
    })
    conn.execute(
        "INSERT INTO cluster_run_critiques (cluster_run_id, critique_json) "
        "VALUES (?, ?)",
        (run_id, critique_data),
    )

    conn.commit()
    conn.close()

    resp = client.get(f"/cluster-runs/{run_id}")
    assert resp.status_code == 200
    data = resp.json()

    assert data["cluster_run_id"] == run_id
    assert data["method"] == "hdbscan"
    assert len(data["assignments"]) == 1
    assert data["assignments"][0]["cluster_id"] == 0
    assert len(data["labels"]) == 1
    assert data["labels"][0]["label"] == "Test Cluster"
    assert data["critique"]["verdict"] == "Good."
