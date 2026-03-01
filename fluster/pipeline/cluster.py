"""cluster_items — HDBSCAN clustering on reduction coordinates."""

import json
import sqlite3

import hdbscan
import numpy as np

from fluster.config.plan import Plan


def _parse_reduction_ref(ref: str) -> tuple[str, int]:
    """Parse a reduction reference like 'umap_8d' into (method, target_dimensions)."""
    method, dims_str = ref.rsplit("_", 1)
    return method, int(dims_str.rstrip("d"))


def _find_reduction(
    conn: sqlite3.Connection, method: str, target_dims: int
) -> sqlite3.Row | None:
    """Find the matching reduction row."""
    return conn.execute(
        "SELECT reduction_id FROM reductions "
        "WHERE method = ? AND target_dimensions = ? "
        "ORDER BY reduction_id DESC LIMIT 1",
        (method, target_dims),
    ).fetchone()


def load_coordinates(
    conn: sqlite3.Connection, reduction_id: int
) -> tuple[list[int], np.ndarray]:
    """Load reduction coordinates as (item_ids, matrix)."""
    rows = conn.execute(
        "SELECT item_id, coordinates_json FROM reduction_coordinates "
        "WHERE reduction_id = ? ORDER BY item_id",
        (reduction_id,),
    ).fetchall()

    if not rows:
        return [], np.empty((0, 0))

    item_ids = [r["item_id"] for r in rows]
    coords = np.array(
        [json.loads(r["coordinates_json"]) for r in rows],
        dtype=np.float32,
    )
    return item_ids, coords


def _cluster_run_exists(
    conn: sqlite3.Connection, reduction_id: int, method: str, params: dict
) -> bool:
    """Check if a cluster run with these exact params already exists."""
    row = conn.execute(
        "SELECT 1 FROM cluster_runs "
        "WHERE reduction_id = ? AND method = ? AND params_json = ?",
        (reduction_id, method, json.dumps(params, sort_keys=True)),
    ).fetchone()
    return row is not None


def cluster_items(
    conn: sqlite3.Connection,
    plan: Plan,
) -> dict:
    """Run all configured clustering passes.

    Each ClusteringConfig in the plan specifies a reduction reference
    (e.g. 'umap_8d') and HDBSCAN params. Idempotent — skips runs
    that already exist with identical params.

    Returns a summary dict.
    """
    created = 0
    skipped = 0

    for cluster_config in plan.clustering:
        method_name, target_dims = _parse_reduction_ref(cluster_config.reduction)

        reduction = _find_reduction(conn, method_name, target_dims)
        if reduction is None:
            raise ValueError(
                f"Reduction '{cluster_config.reduction}' not found. "
                f"Run reduce_items first."
            )

        reduction_id = reduction["reduction_id"]
        params = cluster_config.params

        if _cluster_run_exists(conn, reduction_id, cluster_config.method, params):
            skipped += 1
            continue

        item_ids, coords = load_coordinates(conn, reduction_id)

        if len(item_ids) == 0:
            skipped += 1
            continue

        clusterer = hdbscan.HDBSCAN(**params)
        clusterer.fit(coords)

        labels = clusterer.labels_
        probabilities = clusterer.probabilities_

        cur = conn.execute(
            "INSERT INTO cluster_runs "
            "(reduction_id, method, params_json) VALUES (?, ?, ?)",
            (reduction_id, cluster_config.method, json.dumps(params, sort_keys=True)),
        )
        cluster_run_id = cur.lastrowid

        conn.executemany(
            "INSERT INTO cluster_assignments "
            "(cluster_run_id, item_id, cluster_id, membership_probability) "
            "VALUES (?, ?, ?, ?)",
            [
                (cluster_run_id, item_id, int(label), float(prob))
                for item_id, label, prob in zip(item_ids, labels, probabilities)
            ],
        )

        conn.commit()

        created += 1

    return {
        "runs_created": created,
        "skipped": skipped,
    }
