"""Tests for select_exemplars (Phase 11)."""

import pytest

from fluster.config.plan import Plan
from fluster.db.connection import connect
from fluster.pipeline.cluster import cluster_items
from fluster.pipeline.embed import embed_items
from fluster.pipeline.exemplars import select_exemplars
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


def _setup_clustered(pdir, conn, count=30):
    """Run full pipeline through clustering, return the cluster_run_id."""
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
    return run["cluster_run_id"]


# --- Basic exemplar selection ---

def test_select_exemplars_creates_entries(project):
    pdir, conn = project
    run_id = _setup_clustered(pdir, conn)

    summary = select_exemplars(conn, run_id)

    assert summary["exemplars_created"] > 0
    assert summary["skipped"] is False

    exemplars = conn.execute(
        "SELECT * FROM cluster_exemplars WHERE cluster_run_id = ?",
        (run_id,),
    ).fetchall()
    assert len(exemplars) > 0


def test_exemplars_have_valid_ranks(project):
    pdir, conn = project
    run_id = _setup_clustered(pdir, conn)

    select_exemplars(conn, run_id)

    exemplars = conn.execute(
        "SELECT * FROM cluster_exemplars WHERE cluster_run_id = ? "
        "ORDER BY cluster_id, rank",
        (run_id,),
    ).fetchall()

    # Ranks should start at 1 and be sequential per cluster.
    by_cluster: dict[int, list] = {}
    for e in exemplars:
        by_cluster.setdefault(e["cluster_id"], []).append(e)

    for cluster_id, entries in by_cluster.items():
        ranks = [e["rank"] for e in entries]
        assert ranks == list(range(1, len(ranks) + 1))


def test_exemplars_have_valid_scores(project):
    pdir, conn = project
    run_id = _setup_clustered(pdir, conn)

    select_exemplars(conn, run_id)

    exemplars = conn.execute(
        "SELECT * FROM cluster_exemplars WHERE cluster_run_id = ?",
        (run_id,),
    ).fetchall()

    for e in exemplars:
        assert -1.0 <= e["score"] <= 1.0


def test_exemplars_are_cluster_members(project):
    """Each exemplar should be a member of its cluster."""
    pdir, conn = project
    run_id = _setup_clustered(pdir, conn)

    select_exemplars(conn, run_id)

    exemplars = conn.execute(
        "SELECT * FROM cluster_exemplars WHERE cluster_run_id = ?",
        (run_id,),
    ).fetchall()

    for e in exemplars:
        assignment = conn.execute(
            "SELECT cluster_id FROM cluster_assignments "
            "WHERE cluster_run_id = ? AND item_id = ?",
            (run_id, e["item_id"]),
        ).fetchone()
        assert assignment is not None
        assert assignment["cluster_id"] == e["cluster_id"]


def test_exemplars_respect_top_k(project):
    pdir, conn = project
    run_id = _setup_clustered(pdir, conn)

    select_exemplars(conn, run_id, top_k=2)

    by_cluster = {}
    for e in conn.execute(
        "SELECT * FROM cluster_exemplars WHERE cluster_run_id = ?",
        (run_id,),
    ).fetchall():
        by_cluster.setdefault(e["cluster_id"], []).append(e)

    for cluster_id, entries in by_cluster.items():
        assert len(entries) <= 2


# --- Idempotency ---

def test_exemplars_idempotent(project):
    pdir, conn = project
    run_id = _setup_clustered(pdir, conn)

    select_exemplars(conn, run_id)

    # Second call should skip.
    summary = select_exemplars(conn, run_id)
    assert summary["exemplars_created"] == 0
    assert summary["skipped"] is True


# --- Noise exclusion ---

def test_exemplars_exclude_noise(project):
    """Noise points (cluster_id = -1) should not be selected as exemplars."""
    pdir, conn = project
    run_id = _setup_clustered(pdir, conn)

    select_exemplars(conn, run_id)

    noise = conn.execute(
        "SELECT item_id FROM cluster_assignments "
        "WHERE cluster_run_id = ? AND cluster_id = -1",
        (run_id,),
    ).fetchall()
    noise_ids = {n["item_id"] for n in noise}

    exemplars = conn.execute(
        "SELECT item_id FROM cluster_exemplars WHERE cluster_run_id = ?",
        (run_id,),
    ).fetchall()
    exemplar_ids = {e["item_id"] for e in exemplars}

    assert exemplar_ids.isdisjoint(noise_ids)


# --- Scores are ordered ---

def test_exemplars_ranked_by_score(project):
    """Within each cluster, rank 1 should have the highest score."""
    pdir, conn = project
    run_id = _setup_clustered(pdir, conn)

    select_exemplars(conn, run_id)

    by_cluster = {}
    for e in conn.execute(
        "SELECT * FROM cluster_exemplars WHERE cluster_run_id = ? "
        "ORDER BY cluster_id, rank",
        (run_id,),
    ).fetchall():
        by_cluster.setdefault(e["cluster_id"], []).append(e)

    for cluster_id, entries in by_cluster.items():
        scores = [e["score"] for e in entries]
        assert scores == sorted(scores, reverse=True)
