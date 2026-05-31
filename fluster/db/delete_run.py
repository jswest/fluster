"""delete_cluster_run — remove a single cluster run and its dependent rows."""

import sqlite3

# Tables that reference a cluster_run_id, listed child-first so each is emptied
# before the cluster_runs row it points at is removed (valid under
# foreign_keys=ON). Embeddings, reductions, and representations are left alone.
_DEPENDENT_TABLES = (
    "cluster_run_critiques",
    "cluster_summaries",
    "cluster_exemplars",
    "cluster_assignments",
)


def delete_cluster_run(conn: sqlite3.Connection, cluster_run_id: int) -> dict:
    """Delete one cluster run and every row that depends on it.

    Removes the run's assignments, exemplars, summaries (labels), and critiques,
    then the cluster_runs row itself. Does NOT touch embeddings, reductions, or
    representations, so the next `fluster run` can re-cluster without re-embedding.

    Raises ValueError if the run does not exist. Returns a summary dict mapping
    each table to the number of rows deleted.
    """
    exists = conn.execute(
        "SELECT 1 FROM cluster_runs WHERE cluster_run_id = ?",
        (cluster_run_id,),
    ).fetchone()
    if exists is None:
        raise ValueError(f"Cluster run {cluster_run_id} not found.")

    deleted = {}
    for table in _DEPENDENT_TABLES:
        cur = conn.execute(
            f"DELETE FROM {table} WHERE cluster_run_id = ?",
            (cluster_run_id,),
        )
        deleted[table] = cur.rowcount
    cur = conn.execute(
        "DELETE FROM cluster_runs WHERE cluster_run_id = ?",
        (cluster_run_id,),
    )
    deleted["cluster_runs"] = cur.rowcount
    conn.commit()

    return {"cluster_run_id": cluster_run_id, "deleted": deleted}
