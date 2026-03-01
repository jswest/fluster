"""reduce_items — dimensionality reduction via PCA and/or UMAP."""

import json
import sqlite3
import struct

import numpy as np
from sklearn.decomposition import PCA
from umap import UMAP

from fluster.config.plan import PCAReduction, Plan, UMAPReduction
from fluster.config.settings import SEED


def _load_embedding_vectors(conn: sqlite3.Connection) -> tuple[list[int], np.ndarray]:
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
    dims = len(first_vec) // 4  # float32
    vectors = np.array(
        [struct.unpack(f"{dims}f", r["vector"]) for r in rows],
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
    cur = conn.execute(
        "INSERT INTO reductions "
        "(embedding_reference, method, target_dimensions, params_json) "
        "VALUES (?, ?, ?, ?)",
        (model_name, method, target_dims, json.dumps(params)),
    )
    reduction_id = cur.lastrowid

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


def reduce_items(
    conn: sqlite3.Connection,
    plan: Plan,
) -> dict:
    """Run all configured reductions on the current embeddings.

    Pipeline per the plan:
    1. Optional PCA pre-reduction
    2. UMAP reductions (typically 2D and 8D)

    Each reduction is idempotent — skipped if it already exists.
    Returns a summary dict.
    """
    model_name = plan.embedding.model_name

    item_ids, vectors = _load_embedding_vectors(conn)

    if len(item_ids) == 0:
        return {"reductions_created": 0, "skipped": 0}

    created = 0
    skipped = 0

    # The working matrix starts as the raw embeddings.
    # PCA may transform it before UMAP consumes it.
    working = vectors

    for reduction_config in plan.reductions:
        if isinstance(reduction_config, PCAReduction):
            if not reduction_config.enabled:
                skipped += 1
                continue

            target = min(
                reduction_config.target_dimensions,
                working.shape[0],
                working.shape[1],
            )

            if _reduction_exists(conn, model_name, "pca", target):
                # Load stored PCA coordinates as the working matrix for UMAP.
                rid = conn.execute(
                    "SELECT reduction_id FROM reductions "
                    "WHERE embedding_reference = ? AND method = 'pca' "
                    "AND target_dimensions = ?",
                    (model_name, target),
                ).fetchone()["reduction_id"]
                stored = conn.execute(
                    "SELECT coordinates_json FROM reduction_coordinates "
                    "WHERE reduction_id = ? ORDER BY item_id",
                    (rid,),
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

            # UMAP needs n_neighbors <= n_samples.
            n_samples = working.shape[0]
            n_neighbors = min(15, n_samples - 1) if n_samples > 1 else 1

            # Spectral init fails when n_samples is very small; fall back to random.
            init = "spectral" if n_samples > n_neighbors + 1 else "random"

            reducer = UMAP(
                n_components=target,
                n_neighbors=n_neighbors,
                random_state=random_state,
                init=init,
            )
            coords = reducer.fit_transform(working)

            _store_reduction(
                conn, model_name, "umap", target,
                {
                    "n_components": target,
                    "n_neighbors": n_neighbors,
                    "random_state": random_state,
                },
                item_ids, coords,
            )
            created += 1

    return {"reductions_created": created, "skipped": skipped}
