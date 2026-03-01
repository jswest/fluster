"""select_exemplars — faux-medoid exemplar selection per cluster."""

import sqlite3

import numpy as np

from fluster.pipeline.reduce import load_embedding_vectors

# Default number of nearest-to-centroid candidates to consider.
_N_CANDIDATES = 20
# Default number of exemplars to select per cluster.
_TOP_K = 3


def _load_item_vectors(conn: sqlite3.Connection) -> dict[int, np.ndarray]:
    """Load all embedding vectors as a dict keyed by item_id."""
    item_ids, matrix = load_embedding_vectors(conn)
    if len(item_ids) == 0:
        return {}
    return dict(zip(item_ids, matrix))


def _exemplars_exist(conn: sqlite3.Connection, cluster_run_id: int) -> bool:
    """Check if exemplars have already been selected for this cluster run."""
    row = conn.execute(
        "SELECT 1 FROM cluster_exemplars WHERE cluster_run_id = ?",
        (cluster_run_id,),
    ).fetchone()
    return row is not None


def _select_for_cluster(
    member_ids: list[int],
    vectors: dict[int, np.ndarray],
    n_candidates: int,
    top_k: int,
) -> list[tuple[int, int, float]]:
    """Select exemplars for a single cluster.

    Algorithm:
    1. Compute centroid of cluster members in embedding space
    2. Pick top C nearest-to-centroid candidates
    3. Score each candidate by avg cosine similarity to all cluster members
    4. Return top K as (item_id, rank, score)
    """
    member_vecs = np.array([vectors[member_id] for member_id in member_ids], dtype=np.float32)
    centroid = member_vecs.mean(axis=0)

    # Cosine similarity to centroid for all members.
    centroid_norm = centroid / (np.linalg.norm(centroid) + 1e-10)
    member_norms = member_vecs / (np.linalg.norm(member_vecs, axis=1, keepdims=True) + 1e-10)
    centroid_sims = member_norms @ centroid_norm

    # Pick top N candidates nearest to centroid.
    candidate_count = min(n_candidates, len(member_ids))
    candidate_indices = np.argsort(-centroid_sims)[:candidate_count]

    # Score each candidate by avg similarity to all cluster members.
    scores = []
    for idx in candidate_indices:
        candidate_vec = member_norms[idx]
        sims = member_norms @ candidate_vec
        avg_sim = float(sims.mean())
        scores.append((member_ids[idx], avg_sim))

    # Sort by score descending, take top K.
    scores.sort(key=lambda x: -x[1])
    selection_count = min(top_k, len(scores))

    return [(item_id, rank + 1, score) for rank, (item_id, score) in enumerate(scores[:selection_count])]


def select_exemplars(
    conn: sqlite3.Connection,
    cluster_run_id: int,
    n_candidates: int = _N_CANDIDATES,
    top_k: int = _TOP_K,
) -> dict:
    """Select exemplars for every cluster in a cluster run.

    Idempotent — skips if exemplars already exist for this run.
    Returns a summary dict.
    """
    if _exemplars_exist(conn, cluster_run_id):
        return {"exemplars_created": 0, "skipped": True}

    vectors = _load_item_vectors(conn)
    if not vectors:
        return {"exemplars_created": 0, "skipped": False}

    # Load cluster assignments, excluding noise (cluster_id = -1).
    assignments = conn.execute(
        "SELECT item_id, cluster_id FROM cluster_assignments "
        "WHERE cluster_run_id = ? AND cluster_id >= 0 "
        "ORDER BY cluster_id, item_id",
        (cluster_run_id,),
    ).fetchall()

    # Group by cluster_id.
    clusters: dict[int, list[int]] = {}
    for a in assignments:
        clusters.setdefault(a["cluster_id"], []).append(a["item_id"])

    all_exemplars = []
    for cluster_id, member_ids in clusters.items():
        exemplars = _select_for_cluster(member_ids, vectors, n_candidates, top_k)
        for item_id, rank, score in exemplars:
            all_exemplars.append((cluster_run_id, cluster_id, item_id, rank, score))

    conn.executemany(
        "INSERT INTO cluster_exemplars "
        "(cluster_run_id, cluster_id, item_id, rank, score) "
        "VALUES (?, ?, ?, ?, ?)",
        all_exemplars,
    )
    conn.commit()

    return {"exemplars_created": len(all_exemplars), "skipped": False}
