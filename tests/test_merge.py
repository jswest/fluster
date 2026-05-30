"""Tests for merge_clusters (issue #1 — automatic LLM cluster merging)."""

import json
from unittest.mock import patch

from fluster.config.plan import LLMConfig, LLMProvider, Plan
from fluster.pipeline.embed import embed_items
from fluster.pipeline.ingest import ingest_rows
from fluster.pipeline.materialize import materialize_items
from fluster.pipeline.merge import (
    MergeGroup,
    _build_id_map,
    _sanitize_merges,
    merge_clusters,
)
from fluster.pipeline.reduce import reduce_items


# --- Pure logic: guardrails (_sanitize_merges) ---

def _g(*ids, rationale="x"):
    return MergeGroup(cluster_ids=list(ids), rationale=rationale)


def test_sanitize_drops_noise_and_unknown():
    # -1 (noise) and 99 (unknown) are dropped; the lone survivor leaves <2 -> group dropped.
    groups = _sanitize_merges([_g(0, -1, 99)], valid_ids=[0, 1, 2, 3])
    assert groups == []


def test_sanitize_drops_single_id_group():
    assert _sanitize_merges([_g(1)], valid_ids=[0, 1, 2, 3]) == []


def test_sanitize_dedupes_across_groups():
    # 1 appears twice; first group keeps it, second is left with only [2] and dropped.
    groups = _sanitize_merges([_g(0, 1), _g(1, 2)], valid_ids=[0, 1, 2, 3])
    assert groups == [([0, 1], "x")]


def test_sanitize_floor_refuses_collapsing_everything():
    # Merging all 4 clusters would yield 1 resulting cluster (< floor of 2) -> dropped.
    assert _sanitize_merges([_g(0, 1, 2, 3)], valid_ids=[0, 1, 2, 3]) == []


def test_sanitize_keeps_valid_merge():
    assert _sanitize_merges([_g(2, 0)], valid_ids=[0, 1, 2, 3]) == [([0, 2], "x")]


def test_sanitize_carries_rationale_through_stripping():
    # 99 is stripped but the rationale must survive on the sanitized group.
    groups = _sanitize_merges([_g(0, 2, 99, rationale="dupes")], valid_ids=[0, 1, 2, 3])
    assert groups == [([0, 2], "dupes")]


# --- Pure logic: id remapping (_build_id_map) ---

def test_build_id_map_contiguous_without_merges():
    assert _build_id_map([0, 1, 2], groups=[]) == {0: 0, 1: 1, 2: 2}


def test_build_id_map_collapses_group_at_smallest_member():
    # Merge {0,2}; ids stay contiguous from 0, anchored at the group's min.
    assert _build_id_map([0, 1, 2, 3], groups=[[0, 2]]) == {0: 0, 1: 1, 2: 0, 3: 2}


# --- Integration helpers ---

def _write_csv(path, header, *rows):
    csv_file = path / "data.csv"
    csv_file.write_text("\n".join([header] + list(rows)) + "\n")
    return csv_file


def _llm_config():
    return LLMConfig(provider=LLMProvider.openai, model="gpt-5-mini")


def _label_response():
    return json.dumps({
        "label": "Some Topic",
        "short_label": "Topic",
        "rationale": "Items share a theme.",
        "keywords": ["alpha", "beta"],
    })


def _responder(decision):
    """side_effect: decision JSON for the merge prompt, label JSON otherwise."""
    def _fn(prompt, config):
        if '"merges"' in prompt:
            return json.dumps(decision)
        return _label_response()
    return _fn


# Source layout: clusters 0-3 with 5/5/5/4 members, item 19 is noise (-1).
_SOURCE_CLUSTERS = {0: range(0, 5), 1: range(5, 10), 2: range(10, 15), 3: range(15, 19)}
_NOISE_ITEM = 19
_SOURCE_COUNT = 20


def _setup_source_run(pdir, conn):
    """Embed/reduce real items, then insert a synthetic labeled source run."""
    for i in range(_SOURCE_COUNT):
        (pdir / f"item{i}.txt").write_text(
            f"Item {i} about {'science' if i % 2 == 0 else 'art'} topic {i}."
        )
    rows = [f"item{i},{pdir}/item{i}.txt" for i in range(_SOURCE_COUNT)]
    ingest_rows(conn, _write_csv(pdir, "name,file_path", *rows), pdir)
    materialize_items(conn, pdir)
    plan = Plan()
    embed_items(conn, plan)
    reduce_items(conn, plan)

    reduction_id = conn.execute("SELECT reduction_id FROM reductions LIMIT 1").fetchone()[0]
    item_ids = [r["item_id"] for r in conn.execute("SELECT item_id FROM items ORDER BY item_id")]

    cursor = conn.execute(
        "INSERT INTO cluster_runs (reduction_id, method, params_json) VALUES (?, 'hdbscan', '{}')",
        (reduction_id,),
    )
    run_id = cursor.lastrowid

    cluster_of = {}
    for cid, members in _SOURCE_CLUSTERS.items():
        for m in members:
            cluster_of[m] = cid
    cluster_of[_NOISE_ITEM] = -1

    conn.executemany(
        "INSERT INTO cluster_assignments (cluster_run_id, item_id, cluster_id, membership_probability) "
        "VALUES (?, ?, ?, 1.0)",
        [(run_id, item_ids[i], cluster_of[i]) for i in range(_SOURCE_COUNT)],
    )
    for cid in _SOURCE_CLUSTERS:
        conn.execute(
            "INSERT INTO cluster_summaries "
            "(cluster_run_id, cluster_id, provider, model, label, label_json) "
            "VALUES (?, ?, 'openai', 'gpt-5-mini', ?, ?)",
            (run_id, cid, f"Cluster {cid}",
             json.dumps({"rationale": f"about topic {cid}", "keywords": [f"k{cid}"]})),
        )
    conn.commit()
    return run_id


# --- Integration: end-to-end merge ---

@patch("fluster.llm.client._call_openai")
def test_merge_creates_run_with_fresh_summaries(mock_call, project):
    pdir, conn = project
    run_id = _setup_source_run(pdir, conn)
    mock_call.side_effect = _responder({"merges": [{"cluster_ids": [0, 2], "rationale": "same"}]})

    summary = merge_clusters(conn, run_id, _llm_config())

    assert summary["merged"] is True
    new_id = summary["cluster_run_id"]

    run = conn.execute("SELECT * FROM cluster_runs WHERE cluster_run_id = ?", (new_id,)).fetchone()
    assert run["method"] == "merged"
    params = json.loads(run["params_json"])
    assert params["parent_cluster_run_id"] == run_id  # parent linkage

    # Fresh exemplars + summaries exist for every merged cluster.
    n_clusters = summary["n_clusters_after"]
    summ = conn.execute(
        "SELECT COUNT(DISTINCT cluster_id) AS n FROM cluster_summaries WHERE cluster_run_id = ?",
        (new_id,),
    ).fetchone()["n"]
    exem = conn.execute(
        "SELECT COUNT(DISTINCT cluster_id) AS n FROM cluster_exemplars WHERE cluster_run_id = ?",
        (new_id,),
    ).fetchone()["n"]
    assert summ == n_clusters
    assert exem == n_clusters


@patch("fluster.llm.client._call_openai")
def test_merge_rewrites_assignments_and_carries_noise(mock_call, project):
    pdir, conn = project
    run_id = _setup_source_run(pdir, conn)
    mock_call.side_effect = _responder({"merges": [{"cluster_ids": [0, 2], "rationale": "same"}]})

    new_id = merge_clusters(conn, run_id, _llm_config())["cluster_run_id"]

    counts = dict(conn.execute(
        "SELECT cluster_id, COUNT(*) FROM cluster_assignments WHERE cluster_run_id = ? GROUP BY cluster_id",
        (new_id,),
    ).fetchall())
    # 0 and 2 fold into new cluster 0 (5+5); old 1 -> 1; old 3 -> 2; noise stays -1.
    assert counts == {0: 10, 1: 5, 2: 4, -1: 1}


@patch("fluster.llm.client._call_openai")
def test_merge_is_idempotent(mock_call, project):
    pdir, conn = project
    run_id = _setup_source_run(pdir, conn)
    mock_call.side_effect = _responder({"merges": [{"cluster_ids": [0, 2], "rationale": "same"}]})

    first = merge_clusters(conn, run_id, _llm_config())
    calls_after_first = mock_call.call_count

    second = merge_clusters(conn, run_id, _llm_config())
    assert second["skipped"] is True
    assert second["cluster_run_id"] == first["cluster_run_id"]
    assert mock_call.call_count == calls_after_first  # no LLM call on the skip

    forced = merge_clusters(conn, run_id, _llm_config(), force=True)
    assert forced["merged"] is True
    assert forced["cluster_run_id"] != first["cluster_run_id"]


@patch("fluster.llm.client._call_openai")
def test_merge_refusing_all_collapse_creates_no_run(mock_call, project):
    pdir, conn = project
    run_id = _setup_source_run(pdir, conn)
    mock_call.side_effect = _responder({"merges": [{"cluster_ids": [0, 1, 2, 3], "rationale": "all"}]})

    summary = merge_clusters(conn, run_id, _llm_config())
    assert summary["skipped"] is True
    assert summary.get("reason") == "no valid merges"
    merged = conn.execute("SELECT COUNT(*) FROM cluster_runs WHERE method = 'merged'").fetchone()[0]
    assert merged == 0


@patch("fluster.llm.client._call_openai")
def test_merge_records_rationale_after_stripping(mock_call, project):
    # The LLM proposes an unknown id (99) that gets stripped; the recorded
    # applied-merge rationale must still be preserved (audit-trail regression).
    pdir, conn = project
    run_id = _setup_source_run(pdir, conn)
    mock_call.side_effect = _responder(
        {"merges": [{"cluster_ids": [0, 2, 99], "rationale": "clearly duplicates"}]}
    )

    new_id = merge_clusters(conn, run_id, _llm_config())["cluster_run_id"]

    params = json.loads(conn.execute(
        "SELECT params_json FROM cluster_runs WHERE cluster_run_id = ?", (new_id,)
    ).fetchone()["params_json"])
    assert params["merges"][0]["source_cluster_ids"] == [0, 2]
    assert params["merges"][0]["rationale"] == "clearly duplicates"


@patch("fluster.llm.client._call_openai")
def test_merge_logs_llm_call(mock_call, project):
    pdir, conn = project
    run_id = _setup_source_run(pdir, conn)
    mock_call.side_effect = _responder({"merges": [{"cluster_ids": [0, 2], "rationale": "same"}]})

    merge_clusters(conn, run_id, _llm_config())

    logs = conn.execute(
        "SELECT * FROM llm_calls WHERE task_name = 'merge_clusters'"
    ).fetchall()
    assert len(logs) == 1
    assert logs[0]["provider"] == "openai"


def test_merge_without_summaries_raises(project):
    import pytest
    pdir, conn = project
    # A run with assignments but no summaries.
    conn.execute("INSERT INTO reductions (embedding_reference, method, target_dimensions) VALUES ('e', 'umap', 8)")
    rid = conn.execute("SELECT reduction_id FROM reductions LIMIT 1").fetchone()[0]
    conn.execute("INSERT INTO cluster_runs (reduction_id, method, params_json) VALUES (?, 'hdbscan', '{}')", (rid,))
    run_id = conn.execute("SELECT cluster_run_id FROM cluster_runs LIMIT 1").fetchone()[0]
    conn.commit()

    with pytest.raises(ValueError, match="no cluster summaries"):
        merge_clusters(conn, run_id, _llm_config())
