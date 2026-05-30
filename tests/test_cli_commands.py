"""Tests for CLI commands: run, jobs, job, cancel, chill."""

import os
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from fluster.cli import app, _client_build_is_stale
from fluster.config import settings
from fluster.config.project import create_project, project_dir, project_exists, set_active_project
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


# --- fluster reset ---


def test_reset_drops_and_clears_derived_tables(named_project):
    pdir = project_dir(named_project)
    conn = connect(pdir)
    conn.execute(
        "INSERT INTO reductions (embedding_reference, method, target_dimensions) "
        "VALUES ('emb', 'pca', 2)"
    )
    conn.commit()
    conn.close()

    # DROP + recreate must not error under foreign_keys=ON (correct child-first order).
    result = runner.invoke(app, ["reset"])
    assert result.exit_code == 0

    conn = connect(pdir)
    # The probe row is gone and the derived tables are queryable again (recreated).
    assert conn.execute("SELECT COUNT(*) FROM reductions").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM cluster_exemplars").fetchone()[0] == 0
    conn.close()


# --- fluster delete ---


def test_delete_happy_path(named_project):
    result = runner.invoke(app, ["delete", "test-proj"], input="y\n")
    assert result.exit_code == 0
    assert not project_exists("test-proj")


def test_delete_nonexistent_project(named_project):
    result = runner.invoke(app, ["delete", "ghost"])
    assert result.exit_code == 1


def test_delete_aborted(named_project):
    result = runner.invoke(app, ["delete", "test-proj"], input="n\n")
    assert result.exit_code == 1  # typer.confirm(abort=True) exits with 1
    assert project_exists("test-proj")


# --- fluster plan: UMAP options (issue #8) ---


def test_plan_sets_umap_options(named_project):
    from fluster.config.plan import load_plan

    # Prompt order: provider, model, method, 4 hdbscan params,
    # UMAP n_neighbors, UMAP min_dist, target_dims x2, caption confirm.
    inp = "\n\n\n\n\n\n\n25\n0.2\n\n\ny\n"
    result = runner.invoke(app, ["plan"], input=inp)
    assert result.exit_code == 0, result.output

    plan = load_plan(project_dir(named_project) / settings.PLAN_YAML)
    umaps = [r for r in plan.reductions if r.method == "umap"]
    assert umaps
    assert all(r.n_neighbors == 25 for r in umaps)
    assert all(r.min_dist == 0.2 for r in umaps)


# --- fluster chill ---

_BUILD_EPOCH = 1_000_000_000  # fixed base mtime so freshness is deterministic


def _make_client_dir(tmp_path, *, built=True, stale=False):
    """Build a fake client/ tree with controlled mtimes for staleness tests."""
    client = tmp_path / "client"
    (client / "src").mkdir(parents=True)
    src = client / "src" / "app.css"
    src.write_text("/* x */")
    config = client / "package.json"
    config.write_text("{}")

    if built:
        build_file = client / "build" / "index.js"
        build_file.parent.mkdir(parents=True)
        build_file.write_text("// built")
        os.utime(build_file, (_BUILD_EPOCH, _BUILD_EPOCH))
        src_mtime = _BUILD_EPOCH + 10 if stale else _BUILD_EPOCH - 10
        os.utime(src, (src_mtime, src_mtime))
        os.utime(config, (src_mtime, src_mtime))
    return client


def _chill_cmds(mock_run):
    """The command list passed to each subprocess.run call, in order."""
    return [call.args[0] for call in mock_run.call_args_list]


def test_client_build_is_stale_when_missing(tmp_path):
    assert _client_build_is_stale(_make_client_dir(tmp_path, built=False)) is True


def test_client_build_is_stale_when_source_newer(tmp_path):
    assert _client_build_is_stale(_make_client_dir(tmp_path, built=True, stale=True)) is True


def test_client_build_is_fresh_when_build_newer(tmp_path):
    assert _client_build_is_stale(_make_client_dir(tmp_path, built=True, stale=False)) is False


@patch("fluster.cli.subprocess.run")
def test_chill_stale_auto_rebuilds(mock_run, named_project, tmp_path, monkeypatch):
    mock_run.return_value = MagicMock(returncode=0)
    monkeypatch.setattr(settings, "CLIENT_DIR", _make_client_dir(tmp_path, built=True, stale=True))
    result = runner.invoke(app, ["chill"])
    assert result.exit_code == 0
    assert _chill_cmds(mock_run) == [["npm", "run", "build"], ["node", "build/index.js"]]


@patch("fluster.cli.subprocess.run")
def test_chill_fresh_serves_without_rebuild(mock_run, named_project, tmp_path, monkeypatch):
    mock_run.return_value = MagicMock(returncode=0)
    monkeypatch.setattr(settings, "CLIENT_DIR", _make_client_dir(tmp_path, built=True, stale=False))
    result = runner.invoke(app, ["chill"])
    assert result.exit_code == 0
    assert _chill_cmds(mock_run) == [["node", "build/index.js"]]


@patch("fluster.cli.subprocess.run")
def test_chill_no_rebuild_serves_stale(mock_run, named_project, tmp_path, monkeypatch):
    mock_run.return_value = MagicMock(returncode=0)
    monkeypatch.setattr(settings, "CLIENT_DIR", _make_client_dir(tmp_path, built=True, stale=True))
    result = runner.invoke(app, ["chill", "--no-rebuild"])
    assert result.exit_code == 0
    assert _chill_cmds(mock_run) == [["node", "build/index.js"]]


@patch("fluster.cli.subprocess.run")
def test_chill_no_rebuild_missing_build_errors(mock_run, named_project, tmp_path, monkeypatch):
    mock_run.return_value = MagicMock(returncode=0)
    monkeypatch.setattr(settings, "CLIENT_DIR", _make_client_dir(tmp_path, built=False))
    result = runner.invoke(app, ["chill", "--no-rebuild"])
    assert result.exit_code == 1
    mock_run.assert_not_called()


@patch("fluster.cli.subprocess.run")
def test_chill_failed_rebuild_exits_before_serving(mock_run, named_project, tmp_path, monkeypatch):
    mock_run.return_value = MagicMock(returncode=2)
    monkeypatch.setattr(settings, "CLIENT_DIR", _make_client_dir(tmp_path, built=True, stale=True))
    result = runner.invoke(app, ["chill"])
    assert result.exit_code == 2
    assert _chill_cmds(mock_run) == [["npm", "run", "build"]]


@patch("fluster.cli.subprocess.run")
def test_chill_dev_mode_skips_staleness(mock_run, named_project, tmp_path, monkeypatch):
    mock_run.return_value = MagicMock(returncode=0)
    monkeypatch.setattr(settings, "CLIENT_DIR", _make_client_dir(tmp_path, built=False))
    result = runner.invoke(app, ["chill", "--dev"])
    assert result.exit_code == 0
    assert _chill_cmds(mock_run) == [["npm", "run", "dev", "--", "--port", "3000", "--host", "127.0.0.1"]]
