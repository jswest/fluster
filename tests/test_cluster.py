"""Tests for cluster_items (Phase 10)."""

import json

import pytest

from fluster.config.plan import ClusteringConfig, Plan, PCAReduction, UMAPReduction
from fluster.db.connection import connect
from fluster.pipeline.cluster import cluster_items
from fluster.pipeline.embed import embed_items
from fluster.pipeline.ingest import ingest_rows
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


def _setup_clusterable(pdir, conn, count=30):
    """Create items through the full pipeline up to reduction."""
    for i in range(count):
        (pdir / f"item{i}.txt").write_text(
            f"Item {i} about {'science' if i % 2 == 0 else 'art'} topic {i}."
        )

    rows = [f"item{i},{pdir}/item{i}.txt" for i in range(count)]
    csv_file = _write_csv(pdir, "name,file_path", *rows)
    ingest_rows(conn, csv_file, pdir)
    materialize_items(conn, pdir)
    plan = Plan()
    embed_items(conn, plan)
    reduce_items(conn, plan)


# --- Basic clustering ---

def test_cluster_creates_run(project):
    pdir, conn = project
    _setup_clusterable(pdir, conn)

    summary = cluster_items(conn, Plan())

    assert summary["runs_created"] == 1
    assert summary["skipped"] == 0

    run = conn.execute("SELECT * FROM cluster_runs").fetchone()
    assert run is not None
    assert run["method"] == "hdbscan"


def test_cluster_assigns_all_items(project):
    pdir, conn = project
    _setup_clusterable(pdir, conn)

    cluster_items(conn, Plan())

    assignments = conn.execute("SELECT * FROM cluster_assignments").fetchall()
    assert len(assignments) == 30

    # Every item should have a cluster_id (including -1 for noise).
    for a in assignments:
        assert isinstance(a["cluster_id"], int)
        assert 0.0 <= a["membership_probability"] <= 1.0


def test_cluster_stores_params(project):
    pdir, conn = project
    _setup_clusterable(pdir, conn)

    cluster_items(conn, Plan())

    run = conn.execute("SELECT * FROM cluster_runs").fetchone()
    params = json.loads(run["params_json"])
    assert params["min_cluster_size"] == 5


def test_cluster_links_to_reduction(project):
    pdir, conn = project
    _setup_clusterable(pdir, conn)

    cluster_items(conn, Plan())

    run = conn.execute("SELECT * FROM cluster_runs").fetchone()
    reduction = conn.execute(
        "SELECT * FROM reductions WHERE reduction_id = ?",
        (run["reduction_id"],),
    ).fetchone()
    assert reduction is not None
    assert reduction["method"] == "umap"
    assert reduction["target_dimensions"] == 8


# --- Idempotency ---

def test_cluster_is_idempotent(project):
    pdir, conn = project
    _setup_clusterable(pdir, conn)

    cluster_items(conn, Plan())

    # Second call should skip.
    summary = cluster_items(conn, Plan())
    assert summary["runs_created"] == 0
    assert summary["skipped"] == 1

    count = conn.execute("SELECT COUNT(*) FROM cluster_runs").fetchone()[0]
    assert count == 1


# --- Multiple configs ---

def test_cluster_multiple_configs(project):
    pdir, conn = project
    _setup_clusterable(pdir, conn)

    plan = Plan(clustering=[
        ClusteringConfig(params={"min_cluster_size": 5}),
        ClusteringConfig(params={"min_cluster_size": 3}),
    ])

    summary = cluster_items(conn, plan)
    assert summary["runs_created"] == 2

    runs = conn.execute("SELECT * FROM cluster_runs ORDER BY cluster_run_id").fetchall()
    assert len(runs) == 2

    params_0 = json.loads(runs[0]["params_json"])
    params_1 = json.loads(runs[1]["params_json"])
    assert params_0["min_cluster_size"] == 5
    assert params_1["min_cluster_size"] == 3


# --- Missing reduction ---

def test_cluster_missing_reduction_errors(project):
    pdir, conn = project
    # Don't run reduce_items — no reductions exist.
    for i in range(5):
        (pdir / f"item{i}.txt").write_text(f"item {i}")
    rows = [f"item{i},{pdir}/item{i}.txt" for i in range(5)]
    csv_file = _write_csv(pdir, "name,file_path", *rows)
    ingest_rows(conn, csv_file, pdir)
    materialize_items(conn, pdir)
    embed_items(conn, Plan())

    with pytest.raises(ValueError, match="not found"):
        cluster_items(conn, Plan())


# --- Noise points ---

def test_cluster_handles_noise_points(project):
    """HDBSCAN labels noise as -1. These should be stored with cluster_id=-1."""
    pdir, conn = project
    _setup_clusterable(pdir, conn)

    cluster_items(conn, Plan())

    # Just verify noise points (if any) have cluster_id = -1 and prob ~0.
    noise = conn.execute(
        "SELECT * FROM cluster_assignments WHERE cluster_id = -1"
    ).fetchall()
    for n in noise:
        assert n["membership_probability"] == 0.0
