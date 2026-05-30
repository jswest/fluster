"""cluster_items — clustering on reduction coordinates (HDBSCAN or agglomerative)."""

import json
import sqlite3

import hdbscan
import numpy as np
from sklearn.cluster import AgglomerativeClustering

from fluster.config.plan import ClusteringConfig, Plan


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


def load_codebook(
    conn: sqlite3.Connection, reduction_id: int
) -> tuple[list[tuple[int, int]], np.ndarray]:
    """Load a SOM codebook as (grid_cells, weights_matrix).

    grid_cells[k] is the (grid_i, grid_j) of the k-th node; weights[k] is its
    weight vector. Rows are ordered by node_index.
    """
    rows = conn.execute(
        "SELECT grid_i, grid_j, weight_json FROM som_nodes "
        "WHERE reduction_id = ? ORDER BY node_index",
        (reduction_id,),
    ).fetchall()

    if not rows:
        return [], np.empty((0, 0))

    cells = [(r["grid_i"], r["grid_j"]) for r in rows]
    weights = np.array(
        [json.loads(r["weight_json"]) for r in rows],
        dtype=np.float32,
    )
    return cells, weights


def _run_clustering(
    method: str, matrix: np.ndarray, params: dict
) -> tuple[np.ndarray, np.ndarray]:
    """Cluster a matrix with the named method, returning (labels, probabilities).

    Only parameters relevant to the chosen method are passed through.
    """
    if method == "hdbscan":
        hdbscan_params = {
            k: v for k, v in params.items()
            if k in ('min_cluster_size', 'min_samples', 'cluster_selection_method', 'cluster_selection_epsilon')
        }
        clusterer = hdbscan.HDBSCAN(**hdbscan_params)
        clusterer.fit(matrix)
        return clusterer.labels_, clusterer.probabilities_
    elif method == "agglomerative":
        agglom_params = {
            k: v for k, v in params.items()
            if k in ('n_clusters', 'linkage')
        }
        labels = AgglomerativeClustering(**agglom_params).fit_predict(matrix)
        return labels, np.ones(len(labels), dtype=np.float64)
    else:
        raise ValueError(f"Unknown clustering method '{method}'")


def _store_cluster_run(
    conn: sqlite3.Connection,
    reduction_id: int,
    method: str,
    params: dict,
    item_ids: list[int],
    labels,
    probabilities,
) -> int:
    """Insert a cluster_run and its per-item assignments. Commits."""
    cursor = conn.execute(
        "INSERT INTO cluster_runs (reduction_id, method, params_json) VALUES (?, ?, ?)",
        (reduction_id, method, json.dumps(params, sort_keys=True)),
    )
    cluster_run_id = cursor.lastrowid

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
    return cluster_run_id


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


def _cluster_codebook(
    conn: sqlite3.Connection,
    reduction_id: int,
    cluster_config: ClusteringConfig,
) -> tuple[list[int], list, list] | None:
    """Cluster a SOM codebook and propagate node clusters to items by BMU.

    Returns (item_ids, labels, probabilities), or None if there is nothing to
    cluster (no codebook or no items).
    """
    cells, weights = load_codebook(conn, reduction_id)
    if len(cells) == 0:
        return None

    node_labels, node_probs = _run_clustering(
        cluster_config.method, weights, cluster_config.params
    )
    cell_to_cluster = {
        cell: (int(label), float(prob))
        for cell, label, prob in zip(cells, node_labels, node_probs)
    }

    item_ids, coords = load_coordinates(conn, reduction_id)
    if len(item_ids) == 0:
        return None

    labels, probabilities = [], []
    for grid_i, grid_j in coords:
        label, prob = cell_to_cluster[(int(grid_i), int(grid_j))]
        labels.append(label)
        probabilities.append(prob)
    return item_ids, labels, probabilities


def cluster_items(
    conn: sqlite3.Connection,
    plan: Plan,
) -> dict:
    """Run all configured clustering passes.

    Each ClusteringConfig names a reduction (e.g. 'umap_8d') and a clustering
    method. With target='coordinates' (default) the items' reduction coordinates
    are clustered directly. With target='codebook' the reduction must be a SOM:
    its node weights are clustered and each node's cluster propagates to the
    items whose best-matching unit it is (a two-level SOM).

    Idempotent — skips runs that already exist with identical params.
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

        # The codebook target is recorded in params so a codebook run never
        # collides with a coordinates run on the same reduction; coordinates
        # runs keep their params untouched.
        store_params = dict(cluster_config.params)
        if cluster_config.target == "codebook":
            store_params["target"] = "codebook"

        if _cluster_run_exists(conn, reduction_id, cluster_config.method, store_params):
            skipped += 1
            continue

        if cluster_config.target == "codebook":
            if method_name != "som":
                raise ValueError(
                    f"target='codebook' requires a SOM reduction, "
                    f"got '{cluster_config.reduction}'."
                )
            result = _cluster_codebook(conn, reduction_id, cluster_config)
        else:
            item_ids, coords = load_coordinates(conn, reduction_id)
            if len(item_ids) == 0:
                result = None
            else:
                labels, probabilities = _run_clustering(
                    cluster_config.method, coords, cluster_config.params
                )
                result = (item_ids, labels, probabilities)

        if result is None:
            skipped += 1
            continue

        item_ids, labels, probabilities = result
        _store_cluster_run(
            conn, reduction_id, cluster_config.method, store_params,
            item_ids, labels, probabilities,
        )
        created += 1

    return {
        "runs_created": created,
        "skipped": skipped,
    }
