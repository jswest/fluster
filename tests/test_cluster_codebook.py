"""Tests for SOM codebook clustering in cluster_items (issue #13, phase 2)."""

import json

import pytest

from fluster.config.plan import (
    ClusteringConfig,
    PCAReduction,
    Plan,
    SOMReduction,
    UMAPReduction,
)
from fluster.pipeline.cluster import cluster_items
from fluster.pipeline.embed import embed_items
from fluster.pipeline.ingest import ingest_rows
from fluster.pipeline.materialize import materialize_items
from fluster.pipeline.reduce import reduce_items


def _write_csv(path, header, *rows):
    csv_file = path / "data.csv"
    csv_file.write_text("\n".join([header] + list(rows)) + "\n")
    return csv_file


def _setup_som(pdir, conn, reductions, count=30):
    """Ingest/materialize/embed `count` items and run the given reductions."""
    for i in range(count):
        (pdir / f"item{i}.txt").write_text(
            f"Item {i} about {'science' if i % 2 == 0 else 'art'} topic {i}."
        )
    rows = [f"item{i},{pdir}/item{i}.txt" for i in range(count)]
    csv_file = _write_csv(pdir, "name,file_path", *rows)
    ingest_rows(conn, csv_file, pdir)
    materialize_items(conn, pdir)
    embed_items(conn, Plan())
    reduce_items(conn, Plan(reductions=reductions))


def _codebook_config(n_clusters=3):
    return ClusteringConfig(
        method="agglomerative",
        reduction="som_2d",
        target="codebook",
        params={"n_clusters": n_clusters},
    )


# --- Basic codebook run ---

def test_codebook_creates_run(project):
    pdir, conn = project
    _setup_som(pdir, conn, [PCAReduction(), SOMReduction(grid_x=4, grid_y=4, num_iteration=100)])

    summary = cluster_items(conn, Plan(clustering=[_codebook_config()]))
    assert summary["runs_created"] == 1

    run = conn.execute("SELECT * FROM cluster_runs").fetchone()
    assert run["method"] == "agglomerative"
    params = json.loads(run["params_json"])
    assert params["target"] == "codebook"
    assert params["n_clusters"] == 3

    # The run links to the SOM reduction.
    reduction = conn.execute(
        "SELECT method FROM reductions WHERE reduction_id = ?", (run["reduction_id"],)
    ).fetchone()
    assert reduction["method"] == "som"


def test_codebook_assigns_all_items(project):
    pdir, conn = project
    _setup_som(pdir, conn, [PCAReduction(), SOMReduction(grid_x=4, grid_y=4, num_iteration=100)])

    cluster_items(conn, Plan(clustering=[_codebook_config()]))

    assignments = conn.execute("SELECT * FROM cluster_assignments").fetchall()
    assert len(assignments) == 30
    for a in assignments:
        assert isinstance(a["cluster_id"], int)
        assert a["membership_probability"] == 1.0  # agglomerative


def test_codebook_propagates_by_cell(project):
    """Items sharing a SOM grid cell must share a cluster (propagation by BMU)."""
    pdir, conn = project
    _setup_som(pdir, conn, [PCAReduction(), SOMReduction(grid_x=4, grid_y=4, num_iteration=100)])

    cluster_items(conn, Plan(clustering=[_codebook_config()]))

    rows = conn.execute(
        """
        SELECT rc.coordinates_json AS cell, ca.cluster_id AS cluster_id
        FROM reduction_coordinates rc
        JOIN reductions r ON rc.reduction_id = r.reduction_id
        JOIN cluster_assignments ca ON ca.item_id = rc.item_id
        WHERE r.method = 'som'
        """
    ).fetchall()

    by_cell: dict[str, set[int]] = {}
    for row in rows:
        by_cell.setdefault(row["cell"], set()).add(row["cluster_id"])
    for cell, clusters in by_cell.items():
        assert len(clusters) == 1, f"cell {cell} split across clusters {clusters}"


# --- Guard: codebook requires a SOM ---

def test_codebook_requires_som_reduction(project):
    pdir, conn = project
    _setup_som(pdir, conn, [PCAReduction(), UMAPReduction(target_dimensions=2)])

    plan = Plan(clustering=[
        ClusteringConfig(
            method="agglomerative", reduction="umap_2d",
            target="codebook", params={"n_clusters": 3},
        )
    ])
    with pytest.raises(ValueError, match="requires a SOM reduction"):
        cluster_items(conn, plan)


# --- Idempotency + coexistence ---

def test_codebook_is_idempotent(project):
    pdir, conn = project
    _setup_som(pdir, conn, [PCAReduction(), SOMReduction(grid_x=4, grid_y=4, num_iteration=100)])

    plan = Plan(clustering=[_codebook_config()])
    cluster_items(conn, plan)
    summary = cluster_items(conn, plan)
    assert summary["runs_created"] == 0
    assert summary["skipped"] == 1
    assert conn.execute("SELECT COUNT(*) FROM cluster_runs").fetchone()[0] == 1


def test_codebook_and_coordinates_coexist(project):
    """A coordinates run and a codebook run can both exist over the same items."""
    pdir, conn = project
    _setup_som(
        pdir, conn,
        [PCAReduction(), UMAPReduction(target_dimensions=8),
         SOMReduction(grid_x=4, grid_y=4, num_iteration=100)],
    )

    plan = Plan(clustering=[
        ClusteringConfig(reduction="umap_8d", params={"min_cluster_size": 5}),
        _codebook_config(),
    ])
    summary = cluster_items(conn, plan)
    assert summary["runs_created"] == 2

    methods = {
        r["method"] for r in conn.execute("SELECT method FROM cluster_runs").fetchall()
    }
    assert methods == {"hdbscan", "agglomerative"}
