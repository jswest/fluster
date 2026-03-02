"""Shared test fixtures."""

import pytest

from fluster.config import settings
from fluster.config.project import create_project, set_active_project
from fluster.db.connection import connect


@pytest.fixture
def project(tmp_path):
    """Set up a minimal project directory with a DB connection.

    Yields (project_dir, connection). Used by pipeline tests.
    """
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    conn = connect(tmp_path)
    yield tmp_path, conn
    conn.close()


@pytest.fixture
def named_project(tmp_path, monkeypatch):
    """Create a real project via create_project with monkeypatched paths.

    Sets the active project. Yields the project name string. Used by CLI/server tests.
    """
    home = tmp_path / ".fluster"
    monkeypatch.setattr(settings, "FLUSTER_HOME", home)
    monkeypatch.setattr(settings, "PROJECTS_DIR", home / "projects")
    monkeypatch.setattr(settings, "ACTIVE_PROJECT_FILE", home / "active_project")
    create_project("test-proj")
    set_active_project("test-proj")
    return "test-proj"
