"""run_pipeline — orchestrate the full clustering pipeline under a tracked job."""

import sqlite3
from pathlib import Path

from fluster.config.plan import Plan
from fluster.jobs.manager import is_cancel_requested, log_job, mark_canceled, update_progress
from fluster.pipeline.cluster import cluster_items
from fluster.pipeline.critique import critique_clusters
from fluster.pipeline.embed import embed_items
from fluster.pipeline.exemplars import select_exemplars
from fluster.pipeline.label import label_clusters
from fluster.pipeline.materialize import materialize_items
from fluster.pipeline.reduce import reduce_items


class PipelineCancelled(Exception):
    """Raised when a pipeline job is cancelled between steps."""

    def __init__(self, job_id: int):
        self.job_id = job_id
        super().__init__(f"Job {job_id} was cancelled.")


def _check_cancel(conn: sqlite3.Connection, job_id: int) -> None:
    if is_cancel_requested(conn, job_id):
        mark_canceled(conn, job_id)
        raise PipelineCancelled(job_id)


def _get_all_cluster_run_ids(conn: sqlite3.Connection) -> list[int]:
    rows = conn.execute(
        "SELECT cluster_run_id FROM cluster_runs ORDER BY cluster_run_id"
    ).fetchall()
    return [r["cluster_run_id"] for r in rows]


def run_pipeline(
    conn: sqlite3.Connection,
    pdir: Path,
    plan: Plan,
    job_id: int,
) -> dict:
    """Execute the full pipeline: materialize → embed → reduce → cluster → exemplars → label → critique.

    Does NOT manage job lifecycle (create/start/succeed/fail) — the caller
    handles that. Raises PipelineCancelled if cancellation is requested.
    """
    completed = 0
    total_steps: int | None = None

    def _step(name: str) -> None:
        nonlocal completed
        completed += 1
        update_progress(conn, job_id, {
            "step": name,
            "completed_steps": completed,
            "total_steps": total_steps,
        })

    # Step 1: materialize
    log_job(conn, job_id, "Starting materialize_items")
    result = materialize_items(conn, pdir)
    log_job(conn, job_id, "materialize_items complete", payload=result)
    _step("materialize")
    _check_cancel(conn, job_id)

    # Step 2: embed
    log_job(conn, job_id, "Starting embed_items")
    result = embed_items(conn, plan, job_id=job_id)
    log_job(conn, job_id, "embed_items complete", payload=result)
    _step("embed")
    _check_cancel(conn, job_id)

    # Step 3: reduce
    log_job(conn, job_id, "Starting reduce_items")
    result = reduce_items(conn, plan)
    log_job(conn, job_id, "reduce_items complete", payload=result)
    _step("reduce")
    _check_cancel(conn, job_id)

    # Step 4: cluster
    log_job(conn, job_id, "Starting cluster_items")
    result = cluster_items(conn, plan)
    log_job(conn, job_id, "cluster_items complete", payload=result)

    cluster_run_ids = _get_all_cluster_run_ids(conn)
    total_steps = 4 + 3 * len(cluster_run_ids)
    _step("cluster")
    _check_cancel(conn, job_id)

    # Steps 5-7: per cluster_run
    for crid in cluster_run_ids:
        log_job(conn, job_id, f"Starting select_exemplars for cluster_run {crid}")
        result = select_exemplars(conn, crid)
        log_job(conn, job_id, f"select_exemplars complete for cluster_run {crid}", payload=result)
        _step("exemplars")
        _check_cancel(conn, job_id)

        log_job(conn, job_id, f"Starting label_clusters for cluster_run {crid}")
        result = label_clusters(conn, crid, plan.llm, job_id=job_id)
        log_job(conn, job_id, f"label_clusters complete for cluster_run {crid}", payload=result)
        _step("label")
        _check_cancel(conn, job_id)

        log_job(conn, job_id, f"Starting critique_clusters for cluster_run {crid}")
        result = critique_clusters(conn, crid, plan.llm, job_id=job_id)
        log_job(conn, job_id, f"critique_clusters complete for cluster_run {crid}", payload=result)
        _step("critique")
        _check_cancel(conn, job_id)

    return {"completed_steps": completed, "total_steps": total_steps}
