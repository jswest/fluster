"""merge_clusters — LLM-driven, non-destructive merging of cluster runs.

Given a finished, labeled cluster run, an LLM reads every cluster's label +
rationale + keywords and decides which clusters should be folded together. The
merges are applied into a NEW derived ``cluster_runs`` row (``method="merged"``)
that records its parent and the merge groups it applied in ``params_json`` — the
source run is never mutated, so merges are non-destructive and stackable.

A merged run points at the same ``reduction_id`` as its parent, so every
downstream consumer (exemplars, labeling, critique, export, server) works
against it with no special-casing. Noise (``cluster_id = -1``) is always carried
over and never merged.
"""

import json
import sqlite3

from pydantic import BaseModel

from fluster.config.plan import LLMConfig
from fluster.llm.client import generate_json
from fluster.pipeline.exemplars import select_exemplars
from fluster.pipeline.label import label_clusters

# Minimum number of resulting non-noise clusters; the auto-merge will never
# collapse a run below this floor.
_MIN_RESULTING_CLUSTERS = 2


class MergeGroup(BaseModel):
    cluster_ids: list[int]
    rationale: str


class MergeDecision(BaseModel):
    merges: list[MergeGroup]


_PROMPT_TEMPLATE = """You are reviewing the clusters of a clustering run to decide which clusters describe the SAME underlying concept and should be merged.

Below is every cluster with its label, rationale, keywords, and size:
{clusters}

Merge only clusters that are clearly redundant — different facets or wordings of the same concept. Be conservative: when in doubt, leave clusters separate. Do not merge clusters that are merely related or adjacent. It is perfectly acceptable to return no merges.

Respond with a JSON object containing:
- "merges": An array of merge groups. Each group is an object with:
    - "cluster_ids": An array of 2 or more cluster IDs (integers) to fold together
    - "rationale": A brief explanation (1-2 sentences) of why they describe the same concept

Every cluster ID may appear in at most one group. Respond ONLY with the JSON object, no other text."""


def _get_run(conn: sqlite3.Connection, cluster_run_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM cluster_runs WHERE cluster_run_id = ?",
        (cluster_run_id,),
    ).fetchone()


def _find_merged_child(
    conn: sqlite3.Connection, parent_id: int, config: LLMConfig
) -> int | None:
    """Return an existing merged child's run id for this parent + LLM, if any."""
    row = conn.execute(
        "SELECT cluster_run_id FROM cluster_runs "
        "WHERE method = 'merged' "
        "AND json_extract(params_json, '$.parent_cluster_run_id') = ? "
        "AND json_extract(params_json, '$.provider') = ? "
        "AND json_extract(params_json, '$.model') = ? "
        "ORDER BY cluster_run_id DESC LIMIT 1",
        (parent_id, config.provider.value, config.model),
    ).fetchone()
    return row["cluster_run_id"] if row else None


def _gather_clusters(
    conn: sqlite3.Connection, cluster_run_id: int
) -> list[dict]:
    """Gather (cluster_id, size, label, rationale, keywords) for non-noise clusters.

    Uses the latest summary per cluster (max cluster_summary_id).
    """
    rows = conn.execute(
        """
        SELECT ca.cluster_id, COUNT(*) AS size, cs.label, cs.label_json
        FROM cluster_assignments ca
        JOIN cluster_summaries cs
          ON cs.cluster_run_id = ca.cluster_run_id AND cs.cluster_id = ca.cluster_id
        WHERE ca.cluster_run_id = ? AND ca.cluster_id >= 0
          AND cs.cluster_summary_id = (
              SELECT MAX(cs2.cluster_summary_id) FROM cluster_summaries cs2
              WHERE cs2.cluster_run_id = ca.cluster_run_id AND cs2.cluster_id = ca.cluster_id
          )
        GROUP BY ca.cluster_id, cs.label, cs.label_json
        ORDER BY ca.cluster_id
        """,
        (cluster_run_id,),
    ).fetchall()

    clusters = []
    for r in rows:
        label_data = json.loads(r["label_json"])
        clusters.append({
            "cluster_id": r["cluster_id"],
            "size": r["size"],
            "label": r["label"],
            "rationale": label_data.get("rationale", ""),
            "keywords": label_data.get("keywords", []),
        })
    return clusters


def _format_clusters(clusters: list[dict]) -> str:
    lines = []
    for c in clusters:
        keywords = ", ".join(c["keywords"])
        lines.append(
            f"- Cluster {c['cluster_id']} (size {c['size']}): {c['label']}\n"
            f"    rationale: {c['rationale']}\n"
            f"    keywords: {keywords}"
        )
    return "\n".join(lines)


def _sanitize_merges(
    merges: list[MergeGroup], valid_ids: list[int]
) -> list[tuple[list[int], str]]:
    """Filter LLM-proposed merge groups against the guardrails.

    - Drop ids that are noise/unknown (not in valid_ids).
    - A cluster may appear in at most one group (first occurrence wins).
    - Drop groups left with fewer than 2 ids.
    - Never collapse below _MIN_RESULTING_CLUSTERS resulting clusters: drop
      groups (largest first) until the floor is satisfied.

    Returns (sorted cluster-id group, rationale) pairs — the rationale is
    carried through so the audit trail stays correct even when sanitizing
    strips ids from a group.
    """
    valid = set(valid_ids)
    seen: set[int] = set()
    groups: list[tuple[list[int], str]] = []
    for group in merges:
        ids = []
        for cid in group.cluster_ids:
            if cid in valid and cid not in seen:
                ids.append(cid)
                seen.add(cid)
        if len(ids) >= 2:
            groups.append((sorted(ids), group.rationale))

    # Enforce the resulting-cluster floor, dropping the most aggressive groups
    # first so we keep as many of the smaller, safer merges as possible.
    def _resulting_count(gs: list[tuple[list[int], str]]) -> int:
        return len(valid_ids) - sum(len(ids) - 1 for ids, _ in gs)

    groups.sort(key=lambda g: len(g[0]), reverse=True)
    while groups and _resulting_count(groups) < _MIN_RESULTING_CLUSTERS:
        groups.pop(0)

    return groups


def _build_id_map(valid_ids: list[int], groups: list[list[int]]) -> dict[int, int]:
    """Map old non-noise cluster ids to contiguous new ids starting at 0.

    Each merge group collapses to one new id anchored at its smallest member;
    non-merged ids keep their own slot. Iteration is ascending for determinism.
    """
    representative: dict[int, int] = {}
    for group in groups:
        anchor = min(group)
        for cid in group:
            representative[cid] = anchor

    old_to_new: dict[int, int] = {}
    next_new = 0
    for old in sorted(valid_ids):
        rep = representative.get(old, old)
        if rep not in old_to_new:
            old_to_new[rep] = next_new
            next_new += 1
        if old != rep:
            old_to_new[old] = old_to_new[rep]
    return old_to_new


def merge_clusters(
    conn: sqlite3.Connection,
    cluster_run_id: int,
    config: LLMConfig,
    job_id: int | None = None,
    force: bool = False,
) -> dict:
    """Auto-merge clusters of a labeled run into a new derived 'merged' run.

    Idempotent — unless ``force`` is set, returns an existing merged child for
    this parent + LLM instead of making a new one. Returns a summary dict.
    """
    run = _get_run(conn, cluster_run_id)
    if run is None:
        raise ValueError(f"Cluster run {cluster_run_id} not found.")

    if not force:
        existing = _find_merged_child(conn, cluster_run_id, config)
        if existing is not None:
            return {"merged": False, "skipped": True, "cluster_run_id": existing}

    clusters = _gather_clusters(conn, cluster_run_id)
    if not clusters:
        raise ValueError(
            f"Cluster run {cluster_run_id} has no cluster summaries; "
            "run labeling first."
        )

    valid_ids = [c["cluster_id"] for c in clusters]

    decision = generate_json(
        task_name="merge_clusters",
        schema_model=MergeDecision,
        prompt=_PROMPT_TEMPLATE.format(clusters=_format_clusters(clusters)),
        inputs={"cluster_run_id": cluster_run_id, "cluster_ids": valid_ids},
        config=config,
        conn=conn,
        job_id=job_id,
    )

    groups = _sanitize_merges(decision.merges, valid_ids)
    if not groups:
        return {"merged": False, "skipped": True, "reason": "no valid merges"}

    old_to_new = _build_id_map(valid_ids, [ids for ids, _ in groups])

    # Record applied merges with their resulting (new) cluster id for auditing.
    # ids is sorted ascending, so ids[0] is the group's anchor (its new id).
    applied = [
        {
            "new_cluster_id": old_to_new[ids[0]],
            "source_cluster_ids": ids,
            "rationale": rationale,
        }
        for ids, rationale in groups
    ]

    params = {
        "parent_cluster_run_id": cluster_run_id,
        "merges": applied,
        "provider": config.provider.value,
        "model": config.model,
    }
    cursor = conn.execute(
        "INSERT INTO cluster_runs (reduction_id, method, params_json) "
        "VALUES (?, 'merged', ?)",
        (run["reduction_id"], json.dumps(params, sort_keys=True)),
    )
    new_run_id = cursor.lastrowid

    source_assignments = conn.execute(
        "SELECT item_id, cluster_id, membership_probability "
        "FROM cluster_assignments WHERE cluster_run_id = ?",
        (cluster_run_id,),
    ).fetchall()
    conn.executemany(
        "INSERT INTO cluster_assignments "
        "(cluster_run_id, item_id, cluster_id, membership_probability) "
        "VALUES (?, ?, ?, ?)",
        [
            (
                new_run_id,
                a["item_id"],
                old_to_new[a["cluster_id"]] if a["cluster_id"] >= 0 else -1,
                a["membership_probability"],
            )
            for a in source_assignments
        ],
    )
    conn.commit()

    # Fresh exemplars + labels for the renumbered clusters.
    select_exemplars(conn, new_run_id)
    label_clusters(conn, new_run_id, config, job_id=job_id)

    return {
        "merged": True,
        "skipped": False,
        "cluster_run_id": new_run_id,
        "merges": applied,
        "n_clusters_before": len(valid_ids),
        "n_clusters_after": len(set(old_to_new.values())),
    }
