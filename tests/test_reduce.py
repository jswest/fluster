"""Tests for reduce_items (Phase 9)."""

import json

import pytest

from fluster.config.plan import (
    PCAReduction,
    Plan,
    UMAPReduction,
)
from fluster.db.connection import connect
from fluster.pipeline.embed import embed_items
from fluster.pipeline.ingest import ingest_rows
from fluster.pipeline.materialize import materialize_items
from fluster.pipeline.reduce import reduce_items



def _write_csv(path, header, *rows):
    csv_file = path / "data.csv"
    csv_file.write_text("\n".join([header] + list(rows)) + "\n")
    return csv_file


def _setup_items(pdir, conn, count=20):
    """Create count items with embeddings ready for reduction."""
    for i in range(count):
        (pdir / f"item{i}.txt").write_text(f"Item {i} with unique content about topic {i}.")

    rows = [f"item{i},{pdir}/item{i}.txt" for i in range(count)]
    csv_file = _write_csv(pdir, "name,file_path", *rows)
    ingest_rows(conn, csv_file, pdir)
    materialize_items(conn, pdir)
    embed_items(conn, Plan())


# --- Basic reduction ---

def test_reduce_creates_reductions(project):
    pdir, conn = project
    _setup_items(pdir, conn)

    summary = reduce_items(conn, Plan())

    assert summary["reductions_created"] == 3  # PCA + UMAP 2D + UMAP 8D
    assert summary["skipped"] == 0

    reductions = conn.execute(
        "SELECT * FROM reductions ORDER BY reduction_id"
    ).fetchall()
    assert len(reductions) == 3
    assert reductions[0]["method"] == "pca"
    assert reductions[1]["method"] == "umap"
    assert reductions[1]["target_dimensions"] == 2
    assert reductions[2]["method"] == "umap"
    assert reductions[2]["target_dimensions"] == 8


def test_reduce_stores_coordinates(project):
    pdir, conn = project
    _setup_items(pdir, conn)

    reduce_items(conn, Plan())

    # Check UMAP 2D coordinates.
    umap_2d = conn.execute(
        "SELECT * FROM reductions WHERE method = 'umap' AND target_dimensions = 2"
    ).fetchone()

    coords = conn.execute(
        "SELECT * FROM reduction_coordinates WHERE reduction_id = ?",
        (umap_2d["reduction_id"],),
    ).fetchall()
    assert len(coords) == 20

    # Each coordinate should be a 2-element JSON array.
    for c in coords:
        parsed = json.loads(c["coordinates_json"])
        assert len(parsed) == 2
        assert all(isinstance(v, float) for v in parsed)


def test_reduce_umap_8d_coordinates(project):
    pdir, conn = project
    _setup_items(pdir, conn)

    reduce_items(conn, Plan())

    umap_8d = conn.execute(
        "SELECT * FROM reductions WHERE method = 'umap' AND target_dimensions = 8"
    ).fetchone()

    coords = conn.execute(
        "SELECT * FROM reduction_coordinates WHERE reduction_id = ?",
        (umap_8d["reduction_id"],),
    ).fetchall()
    assert len(coords) == 20

    for c in coords:
        parsed = json.loads(c["coordinates_json"])
        assert len(parsed) == 8


# --- Idempotency ---

def test_reduce_is_idempotent(project):
    pdir, conn = project
    _setup_items(pdir, conn)

    reduce_items(conn, Plan())

    # Second call should skip everything.
    summary = reduce_items(conn, Plan())
    assert summary["reductions_created"] == 0
    assert summary["skipped"] == 3

    count = conn.execute("SELECT COUNT(*) FROM reductions").fetchone()[0]
    assert count == 3


# --- PCA disabled ---

def test_reduce_pca_disabled(project):
    pdir, conn = project
    _setup_items(pdir, conn)

    plan = Plan(reductions=[
        PCAReduction(enabled=False),
        UMAPReduction(target_dimensions=2),
    ])

    summary = reduce_items(conn, plan)

    assert summary["reductions_created"] == 1  # just UMAP 2D
    assert summary["skipped"] == 1  # PCA disabled

    reductions = conn.execute("SELECT * FROM reductions").fetchall()
    assert len(reductions) == 1
    assert reductions[0]["method"] == "umap"


# --- UMAP only ---

def test_reduce_umap_only(project):
    pdir, conn = project
    _setup_items(pdir, conn)

    plan = Plan(reductions=[
        UMAPReduction(target_dimensions=3),
    ])

    summary = reduce_items(conn, plan)
    assert summary["reductions_created"] == 1

    reduction = conn.execute("SELECT * FROM reductions").fetchone()
    assert reduction["method"] == "umap"
    assert reduction["target_dimensions"] == 3


# --- Empty embeddings ---

def test_reduce_no_embeddings(project):
    pdir, conn = project

    summary = reduce_items(conn, Plan())
    assert summary["reductions_created"] == 0
    assert summary["skipped"] == 0


# --- Params stored correctly ---

def test_reduce_stores_params(project):
    pdir, conn = project
    _setup_items(pdir, conn)

    reduce_items(conn, Plan())

    pca = conn.execute(
        "SELECT params_json FROM reductions WHERE method = 'pca'"
    ).fetchone()
    params = json.loads(pca["params_json"])
    assert params["random_state"] == 42

    umap = conn.execute(
        "SELECT params_json FROM reductions WHERE method = 'umap' LIMIT 1"
    ).fetchone()
    params = json.loads(umap["params_json"])
    assert params["random_state"] == 42


# --- Deterministic (seed 42) ---

def test_reduce_is_deterministic(project):
    """Running reduction twice on separate connections should produce identical coords."""
    pdir, conn = project
    _setup_items(pdir, conn)

    reduce_items(conn, Plan())

    coords_first = conn.execute(
        """
        SELECT rc.item_id, rc.coordinates_json
        FROM reduction_coordinates rc
        JOIN reductions r ON rc.reduction_id = r.reduction_id
        WHERE r.method = 'umap' AND r.target_dimensions = 2
        ORDER BY rc.item_id
        """
    ).fetchall()

    # Wipe reductions and redo.
    conn.execute("DELETE FROM reduction_coordinates")
    conn.execute("DELETE FROM reductions")
    conn.commit()

    reduce_items(conn, Plan())

    coords_second = conn.execute(
        """
        SELECT rc.item_id, rc.coordinates_json
        FROM reduction_coordinates rc
        JOIN reductions r ON rc.reduction_id = r.reduction_id
        WHERE r.method = 'umap' AND r.target_dimensions = 2
        ORDER BY rc.item_id
        """
    ).fetchall()

    for a, b in zip(coords_first, coords_second):
        assert a["item_id"] == b["item_id"]
        assert json.loads(a["coordinates_json"]) == json.loads(b["coordinates_json"])
