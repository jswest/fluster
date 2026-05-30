"""Tests for the SOM reduction path of reduce_items (issue #13, phase 1)."""

import json

import pytest

from fluster.config.plan import PCAReduction, Plan, SOMReduction
from fluster.pipeline.embed import embed_items
from fluster.pipeline.ingest import ingest_rows
from fluster.pipeline.materialize import materialize_items
from fluster.pipeline.reduce import _auto_grid_size, reduce_items


def _write_csv(path, header, *rows):
    csv_file = path / "data.csv"
    csv_file.write_text("\n".join([header] + list(rows)) + "\n")
    return csv_file


def _setup_items(pdir, conn, count=20):
    for i in range(count):
        (pdir / f"item{i}.txt").write_text(f"Item {i} with unique content about topic {i}.")
    rows = [f"item{i},{pdir}/item{i}.txt" for i in range(count)]
    csv_file = _write_csv(pdir, "name,file_path", *rows)
    ingest_rows(conn, csv_file, pdir)
    materialize_items(conn, pdir)
    embed_items(conn, Plan())


def _som_plan(**som_kwargs):
    return Plan(reductions=[PCAReduction(), SOMReduction(**som_kwargs)])


# --- Reduction + coordinates ---

def test_som_creates_reduction(project):
    pdir, conn = project
    _setup_items(pdir, conn)

    summary = reduce_items(conn, _som_plan(grid_x=4, grid_y=4, num_iteration=100))
    assert summary["reductions_created"] == 2  # PCA + SOM

    som = conn.execute(
        "SELECT * FROM reductions WHERE method = 'som'"
    ).fetchone()
    assert som is not None
    assert som["target_dimensions"] == 2
    params = json.loads(som["params_json"])
    assert params["grid_x"] == 4 and params["grid_y"] == 4
    assert params["random_state"] == 42


def test_som_coordinates_are_grid_cells(project):
    pdir, conn = project
    _setup_items(pdir, conn)
    reduce_items(conn, _som_plan(grid_x=5, grid_y=5, num_iteration=100))

    som = conn.execute("SELECT reduction_id FROM reductions WHERE method='som'").fetchone()
    coords = conn.execute(
        "SELECT coordinates_json FROM reduction_coordinates WHERE reduction_id = ?",
        (som["reduction_id"],),
    ).fetchall()
    assert len(coords) == 20

    for c in coords:
        i, j = json.loads(c["coordinates_json"])
        assert i == int(i) and j == int(j)
        assert 0 <= i < 5 and 0 <= j < 5


# --- Codebook (som_nodes) ---

def test_som_nodes_populated(project):
    pdir, conn = project
    _setup_items(pdir, conn)
    reduce_items(conn, _som_plan(grid_x=4, grid_y=4, num_iteration=100))

    som = conn.execute("SELECT reduction_id FROM reductions WHERE method='som'").fetchone()
    nodes = conn.execute(
        "SELECT * FROM som_nodes WHERE reduction_id = ? ORDER BY node_index",
        (som["reduction_id"],),
    ).fetchall()
    assert len(nodes) == 16  # 4 * 4

    # node_index, grid_i, grid_j are consistent.
    for n in nodes:
        assert n["node_index"] == n["grid_i"] * 4 + n["grid_j"]
        assert 0.0 <= n["umatrix_dist"] <= 1.0
        weight = json.loads(n["weight_json"])
        # PCA reduced 20 samples -> at most 20 components; codebook lives there.
        assert len(weight) == 20


# --- Idempotency ---

def test_som_is_idempotent(project):
    pdir, conn = project
    _setup_items(pdir, conn)
    plan = _som_plan(grid_x=4, grid_y=4, num_iteration=100)

    reduce_items(conn, plan)
    summary = reduce_items(conn, plan)
    assert summary["reductions_created"] == 0
    assert summary["skipped"] == 2

    n_som = conn.execute("SELECT COUNT(*) FROM reductions WHERE method='som'").fetchone()[0]
    assert n_som == 1
    n_nodes = conn.execute("SELECT COUNT(*) FROM som_nodes").fetchone()[0]
    assert n_nodes == 16


# --- Determinism (seed 42) ---

def test_som_is_deterministic(project):
    pdir, conn = project
    _setup_items(pdir, conn)
    plan = _som_plan(grid_x=4, grid_y=4, num_iteration=100)

    def _snapshot():
        coords = conn.execute(
            """
            SELECT rc.item_id, rc.coordinates_json
            FROM reduction_coordinates rc
            JOIN reductions r ON rc.reduction_id = r.reduction_id
            WHERE r.method = 'som' ORDER BY rc.item_id
            """
        ).fetchall()
        nodes = conn.execute(
            """
            SELECT n.node_index, n.weight_json
            FROM som_nodes n JOIN reductions r ON n.reduction_id = r.reduction_id
            WHERE r.method = 'som' ORDER BY n.node_index
            """
        ).fetchall()
        return (
            [(c["item_id"], c["coordinates_json"]) for c in coords],
            [(n["node_index"], n["weight_json"]) for n in nodes],
        )

    reduce_items(conn, plan)
    first = _snapshot()

    conn.execute("DELETE FROM som_nodes")
    conn.execute("DELETE FROM reduction_coordinates")
    conn.execute("DELETE FROM reductions")
    conn.commit()

    reduce_items(conn, plan)
    second = _snapshot()

    assert first == second


# --- Custom + auto grid sizing ---

def test_som_respects_custom_grid(project):
    pdir, conn = project
    _setup_items(pdir, conn)
    reduce_items(conn, _som_plan(grid_x=3, grid_y=6, num_iteration=50))

    som = conn.execute("SELECT reduction_id FROM reductions WHERE method='som'").fetchone()
    n_nodes = conn.execute(
        "SELECT COUNT(*) FROM som_nodes WHERE reduction_id = ?", (som["reduction_id"],)
    ).fetchone()[0]
    assert n_nodes == 18  # 3 * 6


def test_auto_grid_size_squarish():
    x, y = _auto_grid_size(1000)
    assert x == y
    assert 9 <= x <= 14  # ~sqrt(5*sqrt(1000)) ≈ 11
    # Never degenerate, even for tiny inputs.
    assert _auto_grid_size(1) == (2, 2)


def test_som_auto_grid_used_when_unset(project):
    pdir, conn = project
    _setup_items(pdir, conn)
    reduce_items(conn, _som_plan(num_iteration=50))

    som = conn.execute("SELECT params_json FROM reductions WHERE method='som'").fetchone()
    params = json.loads(som["params_json"])
    expected_x, expected_y = _auto_grid_size(20)
    assert params["grid_x"] == expected_x
    assert params["grid_y"] == expected_y


# --- Pre-SOM database guard ---

def test_som_on_pre_som_db_raises(project):
    pdir, conn = project
    _setup_items(pdir, conn)

    # Simulate a project created before SOM support: the reductions CHECK only
    # admits pca/umap, so a method='som' insert would fail deep in the pipeline.
    conn.executescript(
        """
        DROP TABLE reductions;
        CREATE TABLE reductions (
            reduction_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            embedding_reference TEXT NOT NULL,
            method              TEXT NOT NULL,
            target_dimensions   INTEGER NOT NULL,
            params_json         TEXT NOT NULL DEFAULT '{}',
            created_at          TEXT NOT NULL DEFAULT (datetime('now')),
            CHECK (method IN ('pca', 'umap')),
            CHECK (json_valid(params_json))
        );
        """
    )

    plan = Plan(reductions=[SOMReduction(grid_x=4, grid_y=4, num_iteration=50)])
    with pytest.raises(RuntimeError, match="predates SOM support"):
        reduce_items(conn, plan)
