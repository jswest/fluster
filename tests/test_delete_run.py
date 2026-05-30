"""Tests for delete_cluster_run (issue #22)."""

import json

import pytest

from fluster.db.delete_run import delete_cluster_run


def _seed_run(conn, method="hdbscan"):
    """Create a reduction + cluster run with a full set of dependent rows.

    Returns the new cluster_run_id.
    """
    cur = conn.execute(
        "INSERT INTO reductions (embedding_reference, method, target_dimensions) "
        "VALUES ('test', 'umap', 8)"
    )
    reduction_id = cur.lastrowid
    cur = conn.execute(
        "INSERT INTO cluster_runs (reduction_id, method, params_json) VALUES (?, ?, '{}')",
        (reduction_id, method),
    )
    run_id = cur.lastrowid

    # A row + item to hang assignments/exemplars off of.
    cur = conn.execute("INSERT INTO rows (row_name) VALUES ('r1')")
    cur = conn.execute("INSERT INTO items (row_id) VALUES (?)", (cur.lastrowid,))
    item_id = cur.lastrowid

    conn.execute(
        "INSERT INTO cluster_assignments "
        "(cluster_run_id, item_id, cluster_id, membership_probability) "
        "VALUES (?, ?, 0, 0.9)",
        (run_id, item_id),
    )
    conn.execute(
        "INSERT INTO cluster_exemplars "
        "(cluster_run_id, cluster_id, item_id, kind, rank, score) "
        "VALUES (?, 0, ?, 'center', 1, 0.95)",
        (run_id, item_id),
    )
    conn.execute(
        "INSERT INTO cluster_summaries "
        "(cluster_run_id, cluster_id, provider, model, label, label_json) "
        "VALUES (?, 0, 'openai', 'gpt-5-nano', 'L', ?)",
        (run_id, json.dumps({"label": "L"})),
    )
    conn.execute(
        "INSERT INTO cluster_run_critiques "
        "(cluster_run_id, provider, model, critique_json) "
        "VALUES (?, 'openai', 'gpt-5-nano', ?)",
        (run_id, json.dumps({"verdict": "ok"})),
    )
    conn.commit()
    return run_id


def _counts(conn, run_id):
    """Row counts across all of a run's tables."""
    tables = [
        "cluster_assignments",
        "cluster_exemplars",
        "cluster_summaries",
        "cluster_run_critiques",
        "cluster_runs",
    ]
    return {
        t: conn.execute(
            f"SELECT COUNT(*) FROM {t} WHERE cluster_run_id = ?", (run_id,)
        ).fetchone()[0]
        for t in tables
    }


def test_delete_removes_only_target_run(project):
    _pdir, conn = project
    target = _seed_run(conn)
    keep = _seed_run(conn)

    summary = delete_cluster_run(conn, target)

    assert summary["cluster_run_id"] == target
    # Every dependent table reported one row removed, plus the run itself.
    assert summary["deleted"]["cluster_runs"] == 1
    assert all(v == 0 for v in _counts(conn, target).values())
    # The other run is untouched.
    assert all(v == 1 for v in _counts(conn, keep).values())


def test_delete_preserves_embeddings_and_reductions(project):
    _pdir, conn = project
    run_id = _seed_run(conn)
    reductions_before = conn.execute("SELECT COUNT(*) FROM reductions").fetchone()[0]

    delete_cluster_run(conn, run_id)

    # Reductions survive — deletion must not force a re-embed/re-reduce.
    assert conn.execute("SELECT COUNT(*) FROM reductions").fetchone()[0] == reductions_before


def test_delete_missing_run_raises(project):
    _pdir, conn = project
    with pytest.raises(ValueError, match="not found"):
        delete_cluster_run(conn, 9999)
