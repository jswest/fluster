"""Job lifecycle management — create, start, finish, cancel, log."""

import json
import sqlite3
from datetime import datetime, timezone


def create_job(
    conn: sqlite3.Connection,
    job_type: str,
    input_params: dict | None = None,
) -> int:
    cur = conn.execute(
        "INSERT INTO jobs (job_type, input_params_json) VALUES (?, ?)",
        (job_type, json.dumps(input_params or {})),
    )
    conn.commit()
    return cur.lastrowid


def start_job(conn: sqlite3.Connection, job_id: int) -> None:
    conn.execute(
        "UPDATE jobs SET status = 'running', started_at = datetime('now') "
        "WHERE job_id = ? AND status = 'queued'",
        (job_id,),
    )
    conn.commit()


def succeed_job(conn: sqlite3.Connection, job_id: int) -> None:
    conn.execute(
        "UPDATE jobs SET status = 'succeeded', finished_at = datetime('now') "
        "WHERE job_id = ? AND status = 'running'",
        (job_id,),
    )
    conn.commit()


def fail_job(conn: sqlite3.Connection, job_id: int, error: str) -> None:
    conn.execute(
        "UPDATE jobs SET status = 'failed', finished_at = datetime('now'), "
        "error_message = ? WHERE job_id = ? AND status = 'running'",
        (error, job_id),
    )
    conn.commit()


def request_cancel(conn: sqlite3.Connection, job_id: int) -> None:
    conn.execute(
        "UPDATE jobs SET cancel_requested_at = datetime('now') "
        "WHERE job_id = ? AND status IN ('queued', 'running')",
        (job_id,),
    )
    conn.commit()


def is_cancel_requested(conn: sqlite3.Connection, job_id: int) -> bool:
    row = conn.execute(
        "SELECT cancel_requested_at FROM jobs WHERE job_id = ?",
        (job_id,),
    ).fetchone()
    return row is not None and row["cancel_requested_at"] is not None


def mark_canceled(conn: sqlite3.Connection, job_id: int) -> None:
    conn.execute(
        "UPDATE jobs SET status = 'canceled', finished_at = datetime('now') "
        "WHERE job_id = ? AND status IN ('queued', 'running')",
        (job_id,),
    )
    conn.commit()


def update_progress(conn: sqlite3.Connection, job_id: int, progress: dict) -> None:
    conn.execute(
        "UPDATE jobs SET progress_json = ? WHERE job_id = ?",
        (json.dumps(progress), job_id),
    )
    conn.commit()


def get_job(conn: sqlite3.Connection, job_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
    ).fetchone()


def log_job(
    conn: sqlite3.Connection,
    job_id: int,
    message: str,
    level: str = "info",
    payload: dict | None = None,
) -> None:
    conn.execute(
        "INSERT INTO job_logs (job_id, level, message, payload_json) "
        "VALUES (?, ?, ?, ?)",
        (job_id, level, message, json.dumps(payload) if payload else None),
    )
    conn.commit()


def get_job_logs(conn: sqlite3.Connection, job_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM job_logs WHERE job_id = ? ORDER BY job_log_id",
        (job_id,),
    ).fetchall()
