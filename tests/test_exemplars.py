"""Tests for select_exemplars (Phase 11)."""

import pytest

from fluster.config.plan import Plan
from fluster.db.connection import connect
from fluster.pipeline.cluster import cluster_items
from fluster.pipeline.embed import embed_items
from fluster.pipeline.exemplars import _select_for_cluster, select_exemplars
from fluster.pipeline.ingest import ingest_rows
from fluster.pipeline.materialize import materialize_items
from fluster.pipeline.reduce import reduce_items



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
        "ORDER BY cluster_id, kind, rank",
        (run_id,),
    ).fetchall()

    # Ranks should start at 1 and be sequential per (cluster, kind) group.
    by_group: dict[tuple[int, str], list] = {}
    for e in exemplars:
        by_group.setdefault((e["cluster_id"], e["kind"]), []).append(e)

    for (cluster_id, kind), entries in by_group.items():
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

    select_exemplars(conn, run_id, top_k=2, outskirts_k=1)

    by_cluster: dict[int, dict[str, int]] = {}
    for e in conn.execute(
        "SELECT * FROM cluster_exemplars WHERE cluster_run_id = ?",
        (run_id,),
    ).fetchall():
        counts = by_cluster.setdefault(e["cluster_id"], {})
        counts[e["kind"]] = counts.get(e["kind"], 0) + 1

    for cluster_id, counts in by_cluster.items():
        assert counts.get("center", 0) <= 2
        assert counts.get("outskirt", 0) <= 1


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
    """Center exemplars rank by descending score (most typical first); outskirt
    exemplars rank by ascending centroid similarity (most peripheral first)."""
    pdir, conn = project
    run_id = _setup_clustered(pdir, conn)

    select_exemplars(conn, run_id)

    by_group: dict[tuple[int, str], list] = {}
    for e in conn.execute(
        "SELECT * FROM cluster_exemplars WHERE cluster_run_id = ? "
        "ORDER BY cluster_id, kind, rank",
        (run_id,),
    ).fetchall():
        by_group.setdefault((e["cluster_id"], e["kind"]), []).append(e)

    for (cluster_id, kind), entries in by_group.items():
        scores = [e["score"] for e in entries]
        if kind == "center":
            assert scores == sorted(scores, reverse=True)
        else:
            assert scores == sorted(scores)


# --- Center / outskirt split ---

def test_exemplars_have_both_kinds(project):
    """A run over varied data should yield both center and outskirt exemplars,
    and the two sets should be disjoint."""
    pdir, conn = project
    run_id = _setup_clustered(pdir, conn)

    select_exemplars(conn, run_id)

    rows = conn.execute(
        "SELECT item_id, kind FROM cluster_exemplars WHERE cluster_run_id = ?",
        (run_id,),
    ).fetchall()
    kinds = {r["kind"] for r in rows}
    assert kinds == {"center", "outskirt"}

    centers = {r["item_id"] for r in rows if r["kind"] == "center"}
    outskirts = {r["item_id"] for r in rows if r["kind"] == "outskirt"}
    assert centers.isdisjoint(outskirts)


def test_select_for_cluster_separates_core_from_edges():
    """With a tight core and a few far-flung members, center exemplars come from
    the core and outskirt exemplars from the edges."""
    import numpy as np

    # Five near-identical core members around [1, 0], two distant edge members.
    vectors = {
        1: np.array([1.0, 0.0], dtype=np.float32),
        2: np.array([0.99, 0.01], dtype=np.float32),
        3: np.array([0.98, 0.02], dtype=np.float32),
        4: np.array([1.0, 0.03], dtype=np.float32),
        5: np.array([0.97, 0.0], dtype=np.float32),
        6: np.array([-1.0, 1.0], dtype=np.float32),
        7: np.array([-0.8, 1.2], dtype=np.float32),
    }
    member_ids = list(vectors.keys())

    selected = _select_for_cluster(member_ids, vectors, n_candidates=20, top_k=3, outskirts_k=2)

    center_ids = {item_id for item_id, kind, _, _ in selected if kind == "center"}
    outskirt_ids = {item_id for item_id, kind, _, _ in selected if kind == "outskirt"}

    assert outskirt_ids == {6, 7}
    assert center_ids <= {1, 2, 3, 4, 5}
    assert center_ids.isdisjoint(outskirt_ids)
