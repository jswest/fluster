"""FastAPI server — single-project HTTP API for fluster."""

import json
import sqlite3
from typing import Generator

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from fluster import __version__
from fluster.config.project import project_dir, project_exists
from fluster.db.connection import connect
from fluster.jobs.manager import create_job, get_active_job, get_job, request_cancel

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    status: str = "ok"
    version: str
    project_name: str


class CreateJobRequest(BaseModel):
    job_type: str
    input_params: dict = Field(default_factory=dict)


class CreateJobResponse(BaseModel):
    job_id: int


class JobResponse(BaseModel):
    job_id: int
    job_type: str
    status: str
    input_params: dict = Field(default_factory=dict)
    progress: dict = Field(default_factory=dict)
    cancel_requested_at: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    error_message: str | None = None
    created_at: str


class CancelResponse(BaseModel):
    job_id: int
    cancel_requested: bool


class ClusterRunSummary(BaseModel):
    cluster_run_id: int
    reduction_id: int
    method: str
    params: dict
    created_at: str
    n_clusters: int
    n_items: int


class ClusterAssignment(BaseModel):
    item_id: int
    cluster_id: int
    membership_probability: float


class ClusterSummary(BaseModel):
    cluster_id: int
    label: str
    label_json: dict


class ClusterRunDetail(BaseModel):
    cluster_run_id: int
    reduction_id: int
    method: str
    params: dict
    created_at: str
    assignments: list[ClusterAssignment]
    labels: list[ClusterSummary]
    critique: dict | None = None


# ---------------------------------------------------------------------------
# Dependency
# ---------------------------------------------------------------------------


def get_conn(request: Request) -> Generator[sqlite3.Connection, None, None]:
    conn = connect(request.app.state.project_dir)
    try:
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _row_to_job_response(row: sqlite3.Row) -> JobResponse:
    return JobResponse(
        job_id=row["job_id"],
        job_type=row["job_type"],
        status=row["status"],
        input_params=json.loads(row["input_params_json"]),
        progress=json.loads(row["progress_json"]),
        cancel_requested_at=row["cancel_requested_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        error_message=row["error_message"],
        created_at=row["created_at"],
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

router = APIRouter()


@router.get("/health")
def health(request: Request) -> HealthResponse:
    return HealthResponse(
        version=__version__,
        project_name=request.app.state.project_dir.name,
    )


@router.post("/jobs", status_code=201)
def create_job_endpoint(
    body: CreateJobRequest,
    conn: sqlite3.Connection = Depends(get_conn),
) -> CreateJobResponse:
    active = get_active_job(conn)
    if active:
        raise HTTPException(
            status_code=409,
            detail=f"A job is already active (job_id={active['job_id']}). "
            "Only one job can run at a time.",
        )

    job_id = create_job(conn, body.job_type, body.input_params)
    return CreateJobResponse(job_id=job_id)


@router.get("/jobs/{job_id}")
def get_job_endpoint(
    job_id: int,
    conn: sqlite3.Connection = Depends(get_conn),
) -> JobResponse:
    row = get_job(conn, job_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Job not found.")
    return _row_to_job_response(row)


@router.post("/jobs/{job_id}/cancel")
def cancel_job_endpoint(
    job_id: int,
    conn: sqlite3.Connection = Depends(get_conn),
) -> CancelResponse:
    row = get_job(conn, job_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Job not found.")

    if row["status"] not in ("queued", "running"):
        return CancelResponse(job_id=job_id, cancel_requested=False)

    request_cancel(conn, job_id)
    return CancelResponse(job_id=job_id, cancel_requested=True)


@router.get("/cluster-runs")
def list_cluster_runs(
    conn: sqlite3.Connection = Depends(get_conn),
) -> list[ClusterRunSummary]:
    rows = conn.execute(
        "SELECT cr.*, "
        "  (SELECT COUNT(DISTINCT ca.cluster_id) FROM cluster_assignments ca "
        "   WHERE ca.cluster_run_id = cr.cluster_run_id AND ca.cluster_id >= 0) AS n_clusters, "
        "  (SELECT COUNT(*) FROM cluster_assignments ca "
        "   WHERE ca.cluster_run_id = cr.cluster_run_id) AS n_items "
        "FROM cluster_runs cr ORDER BY cr.cluster_run_id"
    ).fetchall()
    return [
        ClusterRunSummary(
            cluster_run_id=r["cluster_run_id"],
            reduction_id=r["reduction_id"],
            method=r["method"],
            params=json.loads(r["params_json"]),
            created_at=r["created_at"],
            n_clusters=r["n_clusters"],
            n_items=r["n_items"],
        )
        for r in rows
    ]


@router.get("/cluster-runs/{cluster_run_id}")
def get_cluster_run(
    cluster_run_id: int,
    conn: sqlite3.Connection = Depends(get_conn),
) -> ClusterRunDetail:
    run = conn.execute(
        "SELECT * FROM cluster_runs WHERE cluster_run_id = ?",
        (cluster_run_id,),
    ).fetchone()
    if run is None:
        raise HTTPException(status_code=404, detail="Cluster run not found.")

    assignment_rows = conn.execute(
        "SELECT item_id, cluster_id, membership_probability "
        "FROM cluster_assignments WHERE cluster_run_id = ? "
        "ORDER BY cluster_id, item_id",
        (cluster_run_id,),
    ).fetchall()
    assignments = [
        ClusterAssignment(
            item_id=a["item_id"],
            cluster_id=a["cluster_id"],
            membership_probability=a["membership_probability"],
        )
        for a in assignment_rows
    ]

    label_rows = conn.execute(
        "SELECT cluster_id, label, label_json FROM cluster_summaries "
        "WHERE cluster_run_id = ? ORDER BY cluster_id",
        (cluster_run_id,),
    ).fetchall()
    labels = [
        ClusterSummary(
            cluster_id=l["cluster_id"],
            label=l["label"],
            label_json=json.loads(l["label_json"]),
        )
        for l in label_rows
    ]

    critique_row = conn.execute(
        "SELECT critique_json FROM cluster_run_critiques "
        "WHERE cluster_run_id = ?",
        (cluster_run_id,),
    ).fetchone()
    critique = json.loads(critique_row["critique_json"]) if critique_row else None

    return ClusterRunDetail(
        cluster_run_id=run["cluster_run_id"],
        reduction_id=run["reduction_id"],
        method=run["method"],
        params=json.loads(run["params_json"]),
        created_at=run["created_at"],
        assignments=assignments,
        labels=labels,
        critique=critique,
    )


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def create_app(project_name: str) -> FastAPI:
    if not project_exists(project_name):
        raise ValueError(f"Project '{project_name}' does not exist.")

    app = FastAPI(
        title="fluster",
        version=__version__,
    )
    app.state.project_dir = project_dir(project_name)
    app.include_router(router)
    return app
