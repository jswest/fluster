"""Export cluster run results to CSV."""

import csv
import io
import json
import sqlite3

from loguru import logger

EXPORT_HEADERS = [
    "row_id",
    "row_name",
    "cluster_id",
    "cluster_label",
    "umap2_x",
    "umap2_y",
    "metadata",
]

_EXPORT_SQL = """
SELECT r.row_id, r.row_name, ca.cluster_id, cs.label AS cluster_label,
       rc.coordinates_json, r.row_metadata_json
FROM cluster_assignments ca
JOIN items i ON ca.item_id = i.item_id
JOIN rows r ON i.row_id = r.row_id
LEFT JOIN cluster_summaries cs
  ON ca.cluster_run_id = cs.cluster_run_id AND ca.cluster_id = cs.cluster_id
  AND cs.cluster_summary_id = (
      SELECT MAX(cs2.cluster_summary_id) FROM cluster_summaries cs2
      WHERE cs2.cluster_run_id = ca.cluster_run_id AND cs2.cluster_id = ca.cluster_id
  )
LEFT JOIN reduction_coordinates rc
  ON ca.item_id = rc.item_id AND rc.reduction_id = ?
WHERE ca.cluster_run_id = ?
ORDER BY r.row_id
"""


def _get_umap2_reduction_id(
    conn: sqlite3.Connection, cluster_run_id: int
) -> int | None:
    """Return the UMAP-2D reduction matching this cluster run's embedding, or None."""
    row = conn.execute(
        "SELECT r2.reduction_id "
        "FROM cluster_runs cr "
        "JOIN reductions r1 ON cr.reduction_id = r1.reduction_id "
        "JOIN reductions r2 ON r1.embedding_reference = r2.embedding_reference "
        "WHERE cr.cluster_run_id = ? AND r2.method = 'umap' AND r2.target_dimensions = 2 "
        "ORDER BY r2.reduction_id DESC LIMIT 1",
        (cluster_run_id,),
    ).fetchone()
    return row["reduction_id"] if row else None


def export_cluster_run(
    conn: sqlite3.Connection,
    cluster_run_id: int,
) -> str:
    """Export a cluster run's assignments to CSV.

    Returns the CSV as a string.
    """
    # Validate cluster run exists.
    run = conn.execute(
        "SELECT cluster_run_id FROM cluster_runs WHERE cluster_run_id = ?",
        (cluster_run_id,),
    ).fetchone()
    if run is None:
        raise ValueError(f"Cluster run {cluster_run_id} not found.")

    umap2_id = _get_umap2_reduction_id(conn, cluster_run_id)
    rows = conn.execute(_EXPORT_SQL, (umap2_id, cluster_run_id)).fetchall()

    output_buffer = io.StringIO()
    writer = csv.writer(output_buffer)
    writer.writerow(EXPORT_HEADERS)

    for row in rows:
        coords = json.loads(row["coordinates_json"]) if row["coordinates_json"] else [None, None]
        writer.writerow([
            row["row_id"],
            row["row_name"],
            row["cluster_id"],
            row["cluster_label"] or "",
            coords[0] if coords[0] is not None else "",
            coords[1] if coords[1] is not None else "",
            row["row_metadata_json"],
        ])

    logger.info(f"Exported {len(rows)} rows from cluster run {cluster_run_id}")
    return output_buffer.getvalue()
