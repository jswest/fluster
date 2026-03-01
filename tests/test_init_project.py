"""Tests for fluster init (Phase 1)."""

from pathlib import Path

from fluster.config.project import (
    create_project,
    list_projects,
    project_exists,
)
from fluster.config.plan import Plan, load_plan
from fluster.config import settings

import yaml
import pytest


@pytest.fixture(autouse=True)
def tmp_fluster_home(tmp_path, monkeypatch):
    """Redirect FLUSTER_HOME to a temp directory for all tests."""
    home = tmp_path / ".fluster"
    monkeypatch.setattr(settings, "FLUSTER_HOME", home)
    monkeypatch.setattr(settings, "PROJECTS_DIR", home / "projects")
    return home


def test_create_project_creates_structure(tmp_fluster_home):
    pdir = create_project("test-proj")

    assert pdir.exists()
    assert (pdir / "project.yaml").is_file()
    assert (pdir / "plan.yaml").is_file()
    assert (pdir / "artifacts").is_dir()


def test_create_project_writes_valid_yaml(tmp_fluster_home):
    pdir = create_project("test-proj")

    project_meta = yaml.safe_load((pdir / "project.yaml").read_text())
    assert project_meta["name"] == "test-proj"

    plan = load_plan(pdir / "plan.yaml")
    assert plan == Plan()


def test_project_exists(tmp_fluster_home):
    assert not project_exists("nope")
    create_project("yep")
    assert project_exists("yep")


def test_create_duplicate_raises(tmp_fluster_home):
    create_project("dup")
    with pytest.raises(FileExistsError):
        create_project("dup")


def test_list_projects(tmp_fluster_home):
    assert list_projects() == []
    create_project("alpha")
    create_project("beta")
    assert list_projects() == ["alpha", "beta"]
