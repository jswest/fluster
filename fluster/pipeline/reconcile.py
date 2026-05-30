"""reconcile_labels — a global pass that renames cluster labels for consistency.

Per-cluster labeling (see label.py) only ever sees one cluster at a time, so the
label set can drift: duplicate labels, inconsistent granularity or voice, missed
sibling structure. This step shows the LLM every cluster's label information at once
and lets it revise labels for global consistency and disambiguation. It does not
re-cluster — only the labels change.
"""

import json
import sqlite3

from pydantic import BaseModel

from fluster.config.plan import LLMConfig
from fluster.llm.client import generate_json


class ReconciledLabel(BaseModel):
    cluster_id: int
    label: str
    short_label: str
    keywords: list[str]
    reconcile_rationale: str  # what changed and why, or "unchanged"


class ReconcileOutput(BaseModel):
    clusters: list[ReconciledLabel]


_PROMPT_TEMPLATE = """You are reviewing the labels for every cluster from a single clustering run so the label set reads consistently as a whole. Each cluster was labeled independently, so labels may collide, vary in granularity or voice, or miss obvious sibling relationships (e.g. "Cats" and "Dogs" would read better as "Domestic Felines" and "Domestic Canines").

Below are all {n_clusters} clusters with their current labels:

{cluster_blocks}

Revise labels ONLY where it improves consistency or disambiguation across the whole set: resolve duplicate or near-duplicate labels, align granularity and voice, and surface sibling structure. Keep labels that are already clear and distinct unchanged.

Respond with a JSON object {{"clusters": [...]}} containing exactly one entry per cluster above:
- "cluster_id": the cluster's id (integer)
- "label": the (possibly revised) descriptive label (1-5 words)
- "short_label": the (possibly revised) very short label (1-2 words)
- "keywords": an array of 3-5 keywords
- "reconcile_rationale": a brief note on what changed and why, or "unchanged"

Respond ONLY with the JSON object, no other text."""


def _get_cluster_label_info(
    conn: sqlite3.Connection, cluster_run_id: int
) -> list[dict]:
    """Return the latest label summary plus size for each non-noise cluster.

    Uses the same MAX(cluster_summary_id) latest-row-wins resolution as the
    downstream consumers (export.py, server.py, critique.py).
    """
    rows = conn.execute(
        """
        SELECT cs.cluster_summary_id, cs.cluster_id, cs.label_json,
               (SELECT COUNT(*) FROM cluster_assignments ca
                WHERE ca.cluster_run_id = cs.cluster_run_id
                  AND ca.cluster_id = cs.cluster_id) AS size
        FROM cluster_summaries cs
        WHERE cs.cluster_run_id = ? AND cs.cluster_id >= 0
          AND cs.cluster_summary_id IN (
              SELECT MAX(cluster_summary_id) FROM cluster_summaries
              WHERE cluster_run_id = ? GROUP BY cluster_id
          )
        ORDER BY cs.cluster_id
        """,
        (cluster_run_id, cluster_run_id),
    ).fetchall()

    return [
        {
            "cluster_summary_id": r["cluster_summary_id"],
            "cluster_id": r["cluster_id"],
            "size": r["size"],
            "label_json": json.loads(r["label_json"]),
        }
        for r in rows
    ]


def _format_clusters(clusters: list[dict]) -> str:
    """Render every cluster's label information into prompt blocks."""
    blocks = []
    for c in clusters:
        lj = c["label_json"]
        keywords = ", ".join(lj.get("keywords", []))
        blocks.append(
            f"Cluster {c['cluster_id']} (size {c['size']}):\n"
            f"  label: {lj.get('label', '')}\n"
            f"  short_label: {lj.get('short_label', '')}\n"
            f"  keywords: {keywords}\n"
            f"  rationale: {lj.get('rationale', '')}"
        )
    return "\n\n".join(blocks)


def _reconcile_exists(conn: sqlite3.Connection, cluster_run_id: int) -> bool:
    """Check whether this run's labels have already been reconciled."""
    row = conn.execute(
        "SELECT 1 FROM cluster_summaries "
        "WHERE cluster_run_id = ? AND json_extract(label_json, '$.reconciled') "
        "LIMIT 1",
        (cluster_run_id,),
    ).fetchone()
    return row is not None


def reconcile_labels(
    conn: sqlite3.Connection,
    cluster_run_id: int,
    config: LLMConfig,
    job_id: int | None = None,
) -> dict:
    """Rename cluster labels for global consistency using a single LLM call.

    Gathers every cluster's current label information, sends it all to the LLM
    in one call, and updates each cluster's summary in place with the revised
    label. The pre-reconcile label is preserved under label_json.original.
    Idempotent — skips runs whose labels are already reconciled, and runs with
    fewer than two clusters (nothing to disambiguate).

    Returns a summary dict.
    """
    if _reconcile_exists(conn, cluster_run_id):
        return {"reconciled": 0, "skipped": True}

    clusters = _get_cluster_label_info(conn, cluster_run_id)
    if len(clusters) < 2:
        return {"reconciled": 0, "skipped": True}

    prompt = _PROMPT_TEMPLATE.format(
        n_clusters=len(clusters),
        cluster_blocks=_format_clusters(clusters),
    )

    result = generate_json(
        task_name="reconcile_labels",
        schema_model=ReconcileOutput,
        prompt=prompt,
        inputs={"cluster_run_id": cluster_run_id, "n_clusters": len(clusters)},
        config=config,
        conn=conn,
        job_id=job_id,
    )
    revised = {r.cluster_id: r for r in result.clusters}

    for c in clusters:
        original = c["label_json"]
        r = revised.get(c["cluster_id"])
        if r is not None:
            label, short_label, keywords = r.label, r.short_label, r.keywords
            reconcile_rationale = r.reconcile_rationale
        else:
            # LLM omitted this cluster — keep its original label untouched.
            label = original.get("label", "")
            short_label = original.get("short_label", "")
            keywords = original.get("keywords", [])
            reconcile_rationale = "unchanged"

        new_label_json = {
            "label": label,
            "short_label": short_label,
            "rationale": original.get("rationale", ""),
            "keywords": keywords,
            "reconciled": True,
            "reconcile_rationale": reconcile_rationale,
            # The _reconcile_exists guard makes this run a no-op once a cluster
            # is reconciled, so "original" never nests a prior "original".
            "original": original,
        }

        conn.execute(
            "UPDATE cluster_summaries SET label = ?, label_json = ? "
            "WHERE cluster_summary_id = ?",
            (label, json.dumps(new_label_json), c["cluster_summary_id"]),
        )

    conn.commit()
    return {"reconciled": len(clusters), "skipped": False}
