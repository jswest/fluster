"""reduce_items — dimensionality reduction via PCA, UMAP, and/or SOM."""

import json
import math
import sqlite3
import struct
import warnings

import numpy as np
from minisom import MiniSom
from sklearn.decomposition import PCA
from umap import UMAP

from fluster.config.plan import PCAReduction, Plan, SOMReduction, UMAPReduction
from fluster.config.settings import SEED


def load_embedding_vectors(conn: sqlite3.Connection) -> tuple[list[int], np.ndarray]:
    """Load all embedding vectors and their associated item_ids.

    Returns (item_ids, vectors_matrix) where vectors_matrix is shape (n, dims).
    """
    # vec_embeddings may not exist if no embeddings have been created yet.
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='vec_embeddings'"
    ).fetchone()
    if not exists:
        return [], np.empty((0, 0))

    rows = conn.execute(
        """
        SELECT e.embedding_id, r.item_id, ve.vector
        FROM embeddings e
        JOIN representations r ON e.representation_id = r.representation_id
        JOIN vec_embeddings ve ON e.embedding_id = ve.embedding_id
        ORDER BY e.embedding_id
        """,
    ).fetchall()

    if not rows:
        return [], np.empty((0, 0))

    item_ids = [r["item_id"] for r in rows]
    first_vec = rows[0]["vector"]
    dimensions = len(first_vec) // 4  # 4 bytes per float32
    vectors = np.array(
        [struct.unpack(f"{dimensions}f", r["vector"]) for r in rows],
        dtype=np.float32,
    )
    return item_ids, vectors


def _reduction_exists(
    conn: sqlite3.Connection, model_name: str, method: str, target_dims: int
) -> bool:
    """Check if a reduction with these params already exists."""
    row = conn.execute(
        "SELECT 1 FROM reductions "
        "WHERE embedding_reference = ? AND method = ? AND target_dimensions = ?",
        (model_name, method, target_dims),
    ).fetchone()
    return row is not None


def _store_reduction(
    conn: sqlite3.Connection,
    model_name: str,
    method: str,
    target_dims: int,
    params: dict,
    item_ids: list[int],
    coordinates: np.ndarray,
) -> int:
    """Insert a reduction and its coordinates into the database."""
    cursor = conn.execute(
        "INSERT INTO reductions "
        "(embedding_reference, method, target_dimensions, params_json) "
        "VALUES (?, ?, ?, ?)",
        (model_name, method, target_dims, json.dumps(params)),
    )
    reduction_id = cursor.lastrowid

    conn.executemany(
        "INSERT INTO reduction_coordinates "
        "(reduction_id, item_id, coordinates_json) VALUES (?, ?, ?)",
        [
            (reduction_id, item_id, json.dumps(coords.tolist()))
            for item_id, coords in zip(item_ids, coordinates)
        ],
    )

    conn.commit()
    return reduction_id


def _assert_som_supported(conn: sqlite3.Connection) -> None:
    """Fail fast (and clearly) on pre-SOM project databases.

    SOM support widened the reductions.method CHECK to admit 'som'. We don't
    migrate old databases in place (SOM is opt-in), so a project created before
    this feature would reject the insert deep inside training. Catch it up front.
    """
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='reductions'"
    ).fetchone()
    if row is not None and "'som'" not in row[0]:
        raise RuntimeError(
            "This project's database predates SOM support, so it can't store a "
            "SOM reduction. Re-create the project with `fluster init` (and re-run "
            "the pipeline) to use the grid view."
        )


def _auto_grid_size(n_samples: int) -> tuple[int, int]:
    """A squarish grid with ~5*sqrt(n) total nodes (a common SOM heuristic)."""
    side = max(2, round(math.sqrt(5 * math.sqrt(max(n_samples, 1)))))
    return side, side


def _store_som_nodes(
    conn: sqlite3.Connection,
    reduction_id: int,
    weights: np.ndarray,
    distance_map: np.ndarray,
) -> None:
    """Persist the SOM codebook: one row per grid node with its weight vector
    (in the SOM's input space) and U-matrix distance."""
    grid_x, grid_y = weights.shape[0], weights.shape[1]
    conn.executemany(
        "INSERT INTO som_nodes "
        "(reduction_id, node_index, grid_i, grid_j, weight_json, umatrix_dist) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [
            (
                reduction_id,
                i * grid_y + j,
                i,
                j,
                json.dumps(weights[i, j].tolist()),
                float(distance_map[i, j]),
            )
            for i in range(grid_x)
            for j in range(grid_y)
        ],
    )
    conn.commit()


def reduce_items(
    conn: sqlite3.Connection,
    plan: Plan,
) -> dict:
    """Run all configured reductions on the current embeddings.

    Pipeline per the plan:
    1. Optional PCA pre-reduction
    2. UMAP reductions (typically 2D and 8D)
    3. Optional SOM grid reduction (opt-in; also writes a codebook to som_nodes)

    Each reduction is idempotent — skipped if it already exists.
    Returns a summary dict.
    """
    model_name = plan.embedding.model_name

    item_ids, vectors = load_embedding_vectors(conn)

    if len(item_ids) == 0:
        return {"reductions_created": 0, "skipped": 0}

    created = 0
    skipped = 0

    # The working matrix starts as the raw embeddings.
    # PCA may transform it before UMAP consumes it.
    working = vectors

    for reduction_config in plan.reductions:
        if isinstance(reduction_config, PCAReduction):
            target = min(
                reduction_config.target_dimensions,
                working.shape[0],
                working.shape[1],
            )

            if _reduction_exists(conn, model_name, "pca", target):
                # Load stored PCA coordinates as the working matrix for UMAP.
                reduction_id = conn.execute(
                    "SELECT reduction_id FROM reductions "
                    "WHERE embedding_reference = ? AND method = 'pca' "
                    "AND target_dimensions = ?",
                    (model_name, target),
                ).fetchone()["reduction_id"]
                stored = conn.execute(
                    "SELECT coordinates_json FROM reduction_coordinates "
                    "WHERE reduction_id = ? ORDER BY item_id",
                    (reduction_id,),
                ).fetchall()
                working = np.array(
                    [json.loads(r["coordinates_json"]) for r in stored],
                    dtype=np.float32,
                )
                skipped += 1
                continue

            pca = PCA(n_components=target, random_state=SEED)
            working = pca.fit_transform(working)

            _store_reduction(
                conn, model_name, "pca", target,
                {"n_components": target, "random_state": SEED},
                item_ids, working,
            )
            created += 1

        elif isinstance(reduction_config, UMAPReduction):
            target = reduction_config.target_dimensions
            random_state = reduction_config.random_state

            if _reduction_exists(conn, model_name, "umap", target):
                skipped += 1
                continue

            # UMAP needs n_neighbors <= n_samples; cap the configured value.
            n_samples = working.shape[0]
            n_neighbors = (
                min(reduction_config.n_neighbors, n_samples - 1)
                if n_samples > 1
                else 1
            )
            min_dist = reduction_config.min_dist

            # Spectral init fails when n_samples is very small; fall back to random.
            init = "spectral" if n_samples > n_neighbors + 1 else "random"

            reducer = UMAP(
                n_components=target,
                n_neighbors=n_neighbors,
                min_dist=min_dist,
                random_state=random_state,
                init=init,
            )
            # UMAP warns that n_jobs is overridden to 1 when random_state is
            # set — that's intentional for reproducibility, so suppress it.
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message="n_jobs value")
                coords = reducer.fit_transform(working)

            _store_reduction(
                conn, model_name, "umap", target,
                {
                    "n_components": target,
                    "n_neighbors": n_neighbors,
                    "min_dist": min_dist,
                    "random_state": random_state,
                },
                item_ids, coords,
            )
            created += 1

        elif isinstance(reduction_config, SOMReduction):
            # SOM is always a 2D grid (one reduction per model, like umap_2d).
            if _reduction_exists(conn, model_name, "som", 2):
                skipped += 1
                continue

            _assert_som_supported(conn)

            n_samples, input_len = working.shape
            auto_x, auto_y = _auto_grid_size(n_samples)
            grid_x = reduction_config.grid_x or auto_x
            grid_y = reduction_config.grid_y or auto_y

            som = MiniSom(
                grid_x,
                grid_y,
                input_len,
                sigma=reduction_config.sigma,
                learning_rate=reduction_config.learning_rate,
                random_seed=reduction_config.random_state,
            )
            som.random_weights_init(working)
            som.train(working, reduction_config.num_iteration, random_order=True)

            # Each item's coordinates are its best-matching unit (grid cell).
            coords = np.array([som.winner(v) for v in working], dtype=np.float32)

            reduction_id = _store_reduction(
                conn, model_name, "som", 2,
                {
                    "grid_x": grid_x,
                    "grid_y": grid_y,
                    "sigma": reduction_config.sigma,
                    "learning_rate": reduction_config.learning_rate,
                    "num_iteration": reduction_config.num_iteration,
                    "random_state": reduction_config.random_state,
                },
                item_ids, coords,
            )
            _store_som_nodes(conn, reduction_id, som.get_weights(), som.distance_map())
            created += 1

    return {"reductions_created": created, "skipped": skipped}
