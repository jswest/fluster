"""Tests for CLI commands: run, jobs, job, cancel."""

from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from fluster.cli import app
from fluster.config import settings
from fluster.config.project import project_dir, set_active_project
from fluster.db.connection import connect
from fluster.jobs.manager import create_job, get_job, start_job, succeed_job
from fluster.pipeline.run import PipelineCancelled

runner = CliRunner()


# --- fluster run ---


@patch("fluster.cli.run_pipeline", return_value={"completed_steps": 7, "total_steps": 7})
def test_run_happy_path(mock_pipeline, named_project):
    result = runner.invoke(app, ["run"])
    assert result.exit_code == 0
    mock_pipeline.assert_called_once()


def test_run_project_not_found(tmp_path, monkeypatch):
    home = tmp_path / ".fluster"
    monkeypatch.setattr(settings, "FLUSTER_HOME", home)
    monkeypatch.setattr(settings, "PROJECTS_DIR", home / "projects")
    monkeypatch.setattr(settings, "ACTIVE_PROJECT_FILE", home / "active_project")
    set_active_project("nope")
    result = runner.invoke(app, ["run"])
    assert result.exit_code == 1


@patch("fluster.cli.run_pipeline", side_effect=RuntimeError("boom"))
def test_run_failure_marks_job_failed(mock_pipeline, named_project):
    result = runner.invoke(app, ["run"])
    assert result.exit_code == 1

    conn = connect(project_dir(named_project))
    row = get_job(conn, 1)
    conn.close()
    assert row["status"] == "failed"
    assert "boom" in row["error_message"]


@patch("fluster.cli.run_pipeline", side_effect=PipelineCancelled(1))
def test_run_cancellation(mock_pipeline, named_project):
    result = runner.invoke(app, ["run"])
    assert result.exit_code == 1


# --- fluster jobs ---


def test_jobs_empty(named_project):
    result = runner.invoke(app, ["jobs"])
    assert result.exit_code == 0
    assert "No jobs" in result.output


def test_jobs_lists_entries(named_project):
    conn = connect(project_dir(named_project))
    create_job(conn, "full_run")
    create_job(conn, "full_run")
    conn.close()

    result = runner.invoke(app, ["jobs"])
    assert result.exit_code == 0
    assert "full_run" in result.output


# --- fluster job ---


def test_job_shows_details(named_project):
    conn = connect(project_dir(named_project))
    job_id = create_job(conn, "full_run")
    start_job(conn, job_id)
    conn.close()

    result = runner.invoke(app, ["job", str(job_id)])
    assert result.exit_code == 0
    assert "full_run" in result.output
    assert "running" in result.output


def test_job_not_found(named_project):
    result = runner.invoke(app, ["job", "9999"])
    assert result.exit_code == 1


# --- fluster cancel ---


def test_cancel_running_job(named_project):
    conn = connect(project_dir(named_project))
    job_id = create_job(conn, "full_run")
    start_job(conn, job_id)
    conn.close()

    result = runner.invoke(app, ["cancel", str(job_id)])
    assert result.exit_code == 0

    conn = connect(project_dir(named_project))
    row = get_job(conn, job_id)
    conn.close()
    assert row["cancel_requested_at"] is not None


def test_cancel_finished_job(named_project):
    conn = connect(project_dir(named_project))
    job_id = create_job(conn, "full_run")
    start_job(conn, job_id)
    succeed_job(conn, job_id)
    conn.close()

    result = runner.invoke(app, ["cancel", str(job_id)])
    assert result.exit_code == 0
    # Warning goes to stderr via loguru, not stdout. Just verify no error exit.
    conn = connect(project_dir(named_project))
    row = get_job(conn, job_id)
    conn.close()
    assert row["cancel_requested_at"] is None  # Should NOT be set on finished job.
