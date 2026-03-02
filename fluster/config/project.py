"""Project layout: creation, validation, and path helpers."""

from pathlib import Path

import yaml

from fluster.config import settings
from fluster.config.plan import Plan, save_plan


def project_dir(project_name: str) -> Path:
    return settings.PROJECTS_DIR / project_name


def ensure_workspace() -> None:
    settings.PROJECTS_DIR.mkdir(parents=True, exist_ok=True)


def project_exists(project_name: str) -> bool:
    return (project_dir(project_name) / settings.PROJECT_YAML).is_file()


def list_projects() -> list[str]:
    if not settings.PROJECTS_DIR.exists():
        return []
    return sorted(
        d.name for d in settings.PROJECTS_DIR.iterdir()
        if d.is_dir() and (d / settings.PROJECT_YAML).is_file()
    )


def set_active_project(project_name: str) -> None:
    """Write the active project name to disk."""
    settings.FLUSTER_HOME.mkdir(parents=True, exist_ok=True)
    settings.ACTIVE_PROJECT_FILE.write_text(project_name)


def get_active_project() -> str | None:
    """Read the active project name, or None if not set."""
    if settings.ACTIVE_PROJECT_FILE.is_file():
        name = settings.ACTIVE_PROJECT_FILE.read_text().strip()
        return name if name else None
    return None


def create_project(project_name: str) -> Path:
    """Create a new project directory with default files.

    Returns the project directory path.
    Raises FileExistsError if the project already exists.
    """
    ensure_workspace()
    project_path = project_dir(project_name)

    if project_exists(project_name):
        raise FileExistsError(f"Project '{project_name}' already exists at {project_path}")

    project_path.mkdir(parents=True)
    (project_path / settings.ARTIFACTS_DIR).mkdir()

    # project.yaml
    project_meta = {"name": project_name}
    (project_path / settings.PROJECT_YAML).write_text(
        yaml.dump(project_meta, default_flow_style=False)
    )

    # plan.yaml
    save_plan(Plan(), project_path / settings.PLAN_YAML)

    return project_path
