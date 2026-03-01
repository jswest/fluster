"""critique_clusters — compute metrics and get LLM verdict on a cluster run."""

import json
import sqlite3
import statistics

import numpy as np
from pydantic import BaseModel
from sklearn.metrics import silhouette_score

from fluster.config.plan import LLMConfig
from fluster.llm.client import generate_json
from fluster.pipeline.cluster import load_coordinates


class CritiqueOutput(BaseModel):
    verdict: str
    quality_score: float
    recommendations: list[str]


_PROMPT_TEMPLATE = """You are evaluating the quality of a clustering run. Below are the metrics for this run, along with the cluster labels.

Metrics:
- Number of clusters: {n_clusters}
- Noise fraction: {noise_fraction:.2%}
- Total items: {total_items}
- Cluster sizes: min={size_min}, max={size_max}, median={size_median}
- Silhouette score: {silhouette}

Cluster labels:
{cluster_labels}

Respond with a JSON object containing:
- "verdict": A 1-3 sentence overall assessment of the clustering quality
- "quality_score": A float between 0.0 and 1.0 rating the clustering quality
- "recommendations": An array of 1-3 actionable suggestions for improvement

Respond ONLY with the JSON object, no other text."""


def _compute_metrics(
    conn: sqlite3.Connection, cluster_run_id: int
) -> dict:
    """Compute clustering quality metrics."""
    assignments = conn.execute(
        "SELECT item_id, cluster_id FROM cluster_assignments "
        "WHERE cluster_run_id = ?",
        (cluster_run_id,),
    ).fetchall()

    total = len(assignments)
    if total == 0:
        return {
            "n_clusters": 0,
            "noise_fraction": 0.0,
            "total_items": 0,
            "size_min": 0,
            "size_max": 0,
            "size_median": 0,
            "silhouette": None,
        }

    labels = [a["cluster_id"] for a in assignments]
    noise_count = sum(1 for l in labels if l == -1)
    non_noise_labels = [l for l in labels if l >= 0]
    n_clusters = len(set(non_noise_labels))

    # Cluster size distribution (non-noise only).
    sizes: dict[int, int] = {}
    for l in non_noise_labels:
        sizes[l] = sizes.get(l, 0) + 1
    size_values = list(sizes.values()) if sizes else [0]

    metrics: dict = {
        "n_clusters": n_clusters,
        "noise_fraction": noise_count / total,
        "total_items": total,
        "size_min": min(size_values),
        "size_max": max(size_values),
        "size_median": statistics.median(size_values),
        "silhouette": None,
    }

    # Silhouette score requires >= 2 clusters and >= 2 non-noise items.
    if n_clusters >= 2 and len(non_noise_labels) >= 2:
        # Load the reduction coordinates used for this cluster run.
        run = conn.execute(
            "SELECT reduction_id FROM cluster_runs WHERE cluster_run_id = ?",
            (cluster_run_id,),
        ).fetchone()
        item_ids, coords = load_coordinates(conn, run["reduction_id"])

        # Build arrays aligned to non-noise assignments only.
        id_to_idx = {iid: i for i, iid in enumerate(item_ids)}
        non_noise_coords = []
        non_noise_cluster_labels = []
        for a in assignments:
            if a["cluster_id"] >= 0 and a["item_id"] in id_to_idx:
                non_noise_coords.append(coords[id_to_idx[a["item_id"]]])
                non_noise_cluster_labels.append(a["cluster_id"])

        if len(set(non_noise_cluster_labels)) >= 2:
            metrics["silhouette"] = round(
                silhouette_score(np.array(non_noise_coords), non_noise_cluster_labels),
                3,
            )

    return metrics


def _get_cluster_labels(conn: sqlite3.Connection, cluster_run_id: int) -> str:
    """Format cluster labels for the prompt."""
    rows = conn.execute(
        "SELECT cluster_id, label FROM cluster_summaries "
        "WHERE cluster_run_id = ? ORDER BY cluster_id",
        (cluster_run_id,),
    ).fetchall()

    if not rows:
        return "(no labels available)"

    return "\n".join(f"- Cluster {r['cluster_id']}: {r['label']}" for r in rows)


def _critique_exists(conn: sqlite3.Connection, cluster_run_id: int) -> bool:
    """Check if a critique already exists for this run."""
    row = conn.execute(
        "SELECT 1 FROM cluster_run_critiques WHERE cluster_run_id = ?",
        (cluster_run_id,),
    ).fetchone()
    return row is not None


def critique_clusters(
    conn: sqlite3.Connection,
    cluster_run_id: int,
    config: LLMConfig,
    job_id: int | None = None,
) -> dict:
    """Critique a cluster run using computed metrics and an LLM verdict.

    Computes clustering quality metrics (n_clusters, noise fraction,
    size distribution, silhouette score), gathers cluster labels,
    and sends everything to the LLM for a structured verdict.
    Idempotent — skips if a critique already exists for this run.

    Returns a summary dict.
    """
    if _critique_exists(conn, cluster_run_id):
        return {"critiqued": False, "skipped": True}

    metrics = _compute_metrics(conn, cluster_run_id)
    cluster_labels = _get_cluster_labels(conn, cluster_run_id)

    prompt_metrics = {
        **metrics,
        "silhouette": f"{metrics['silhouette']:.3f}" if metrics["silhouette"] is not None else "N/A",
    }
    prompt = _PROMPT_TEMPLATE.format(
        cluster_labels=cluster_labels,
        **prompt_metrics,
    )

    result = generate_json(
        task_name="critique_clusters",
        schema_model=CritiqueOutput,
        prompt=prompt,
        inputs={
            "cluster_run_id": cluster_run_id,
            "metrics": metrics,
        },
        config=config,
        conn=conn,
        job_id=job_id,
    )

    critique_data = result.model_dump()
    critique_data["metrics"] = metrics

    conn.execute(
        "INSERT INTO cluster_run_critiques (cluster_run_id, critique_json) "
        "VALUES (?, ?)",
        (cluster_run_id, json.dumps(critique_data)),
    )
    conn.commit()

    return {"critiqued": True, "skipped": False}
