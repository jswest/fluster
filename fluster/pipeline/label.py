"""label_clusters — LLM-powered cluster labeling using exemplars."""

import json
import sqlite3

from pydantic import BaseModel

from fluster.config.plan import LLMConfig
from fluster.llm.client import generate_json


class ClusterLabel(BaseModel):
    label: str
    short_label: str
    rationale: str
    keywords: list[str]


_EXEMPLAR_TEXT_LIMIT = 500  # chars; keeps LLM prompt size manageable

_PROMPT_TEMPLATE = """You are labeling a cluster of items. Below are representative examples (exemplars) from the cluster, along with the cluster size.

Cluster size: {cluster_size}

Exemplar texts:
{exemplar_texts}

Respond with a JSON object containing:
- "label": A descriptive label for this cluster (1-5 words)
- "short_label": A very short label (1-2 words)
- "rationale": A brief explanation of why this label fits (1-2 sentences)
- "keywords": An array of 3-5 keywords that characterize this cluster

Respond ONLY with the JSON object, no other text."""


def _get_clusters(conn: sqlite3.Connection, cluster_run_id: int) -> list[tuple[int, int]]:
    """Get all non-noise (cluster_id, size) pairs for a run."""
    rows = conn.execute(
        "SELECT cluster_id, COUNT(*) AS size FROM cluster_assignments "
        "WHERE cluster_run_id = ? AND cluster_id >= 0 "
        "GROUP BY cluster_id ORDER BY cluster_id",
        (cluster_run_id,),
    ).fetchall()
    return [(r["cluster_id"], r["size"]) for r in rows]


def _get_exemplar_texts(
    conn: sqlite3.Connection, cluster_run_id: int, cluster_id: int
) -> list[str]:
    """Get embedding_text for each exemplar in a cluster, ordered by rank."""
    rows = conn.execute(
        """
        SELECT r.text
        FROM cluster_exemplars ce
        JOIN representations r ON r.item_id = ce.item_id
            AND r.representation_type = 'embedding_text'
        WHERE ce.cluster_run_id = ? AND ce.cluster_id = ?
        ORDER BY ce.rank
        """,
        (cluster_run_id, cluster_id),
    ).fetchall()
    return [r["text"][:_EXEMPLAR_TEXT_LIMIT] for r in rows]


def _label_exists(
    conn: sqlite3.Connection, cluster_run_id: int, cluster_id: int,
    config: LLMConfig,
) -> bool:
    """Check if a label already exists for this cluster from this LLM."""
    row = conn.execute(
        "SELECT 1 FROM cluster_summaries "
        "WHERE cluster_run_id = ? AND cluster_id = ? "
        "AND provider = ? AND model = ?",
        (cluster_run_id, cluster_id, config.provider.value, config.model),
    ).fetchone()
    return row is not None


def label_clusters(
    conn: sqlite3.Connection,
    cluster_run_id: int,
    config: LLMConfig,
    job_id: int | None = None,
) -> dict:
    """Label every cluster in a run using the LLM.

    For each cluster, gathers exemplar texts and cluster size,
    sends them to the LLM, and stores the structured label.
    Idempotent — skips clusters that already have a label.

    Returns a summary dict.
    """
    clusters = _get_clusters(conn, cluster_run_id)

    labeled = 0
    skipped = 0

    for cluster_id, cluster_size in clusters:
        if _label_exists(conn, cluster_run_id, cluster_id, config):
            skipped += 1
            continue
        exemplar_texts = _get_exemplar_texts(conn, cluster_run_id, cluster_id)

        if not exemplar_texts:
            skipped += 1
            continue

        prompt = _PROMPT_TEMPLATE.format(
            cluster_size=cluster_size,
            exemplar_texts="\n---\n".join(
                f"[Exemplar {i+1}]\n{text}" for i, text in enumerate(exemplar_texts)
            ),
        )

        result = generate_json(
            task_name="label_cluster",
            schema_model=ClusterLabel,
            prompt=prompt,
            inputs={
                "cluster_run_id": cluster_run_id,
                "cluster_id": cluster_id,
                "cluster_size": cluster_size,
            },
            config=config,
            conn=conn,
            job_id=job_id,
        )

        conn.execute(
            "INSERT INTO cluster_summaries "
            "(cluster_run_id, cluster_id, provider, model, label, label_json) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                cluster_run_id,
                cluster_id,
                config.provider.value,
                config.model,
                result.label,
                json.dumps(result.model_dump()),
            ),
        )
        conn.commit()
        labeled += 1

    return {"labeled": labeled, "skipped": skipped}
