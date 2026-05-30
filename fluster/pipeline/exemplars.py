"""select_exemplars — faux-medoid exemplar selection per cluster."""

import sqlite3

import numpy as np

from fluster.pipeline.reduce import load_embedding_vectors

# Default number of nearest-to-centroid candidates to consider.
_N_CANDIDATES = 20
# Default number of center exemplars (most typical) to select per cluster.
_TOP_K = 3
# Default number of outskirt exemplars (near the cluster's edges) to select per cluster.
_OUTSKIRTS_K = 2


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
    outskirts_k: int,
) -> list[tuple[int, str, int, float]]:
    """Select center and outskirt exemplars for a single cluster.

    Center exemplars are the cluster's most typical members; outskirt
    exemplars sit near its edges. Showing both to the labeler keeps labels
    from over-fitting to the dense core.

    Algorithm:
    1. Compute centroid of cluster members in embedding space.
    2. Center: of the C nearest-to-centroid candidates, take the top K by
       avg cosine similarity to all members. score = that avg similarity.
    3. Outskirt: take the members with the lowest centroid similarity
       (excluding center picks). score = centroid similarity; rank 1 is the
       most peripheral.
    4. Return each as (item_id, kind, rank, score), ranked 1.. within kind.
    """
    member_vecs = np.array([vectors[member_id] for member_id in member_ids], dtype=np.float32)
    centroid = member_vecs.mean(axis=0)

    # Cosine similarity to centroid for all members.
    centroid_norm = centroid / (np.linalg.norm(centroid) + 1e-10)
    member_norms = member_vecs / (np.linalg.norm(member_vecs, axis=1, keepdims=True) + 1e-10)
    centroid_sims = member_norms @ centroid_norm

    # --- Center: nearest-to-centroid candidates, scored by avg similarity to members.
    candidate_count = min(n_candidates, len(member_ids))
    candidate_indices = np.argsort(-centroid_sims)[:candidate_count]

    center_scores = []
    for idx in candidate_indices:
        avg_sim = float((member_norms @ member_norms[idx]).mean())
        center_scores.append((member_ids[idx], avg_sim))

    center_scores.sort(key=lambda x: -x[1])
    center = center_scores[: min(top_k, len(center_scores))]
    center_ids = {item_id for item_id, _ in center}

    selected = [
        (item_id, "center", rank + 1, score)
        for rank, (item_id, score) in enumerate(center)
    ]

    # --- Outskirts: lowest centroid similarity, excluding members already chosen as center.
    outskirts = []
    for idx in np.argsort(centroid_sims):
        item_id = member_ids[idx]
        if item_id in center_ids:
            continue
        outskirts.append((item_id, float(centroid_sims[idx])))
        if len(outskirts) >= outskirts_k:
            break

    selected += [
        (item_id, "outskirt", rank + 1, score)
        for rank, (item_id, score) in enumerate(outskirts)
    ]

    return selected


def select_exemplars(
    conn: sqlite3.Connection,
    cluster_run_id: int,
    n_candidates: int = _N_CANDIDATES,
    top_k: int = _TOP_K,
    outskirts_k: int = _OUTSKIRTS_K,
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
        exemplars = _select_for_cluster(member_ids, vectors, n_candidates, top_k, outskirts_k)
        for item_id, kind, rank, score in exemplars:
            all_exemplars.append((cluster_run_id, cluster_id, item_id, kind, rank, score))

    conn.executemany(
        "INSERT INTO cluster_exemplars "
        "(cluster_run_id, cluster_id, item_id, kind, rank, score) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        all_exemplars,
    )
    conn.commit()

    return {"exemplars_created": len(all_exemplars), "skipped": False}
