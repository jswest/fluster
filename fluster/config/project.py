"""Project layout: creation, validation, and path helpers."""

from pathlib import Path

import yaml

from fluster.config import settings


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


def default_plan() -> dict:
    return {
        "embedding": {
            "model_name": "all-MiniLM-L6-v2",
            "max_tokens": 256,
        },
        "reductions": [
            {"method": "pca", "enabled": True},
            {"method": "umap", "target_dimensions": 2, "random_state": 42},
            {"method": "umap", "target_dimensions": 8, "random_state": 42},
        ],
        "clustering": [
            {
                "method": "hdbscan",
                "reduction": "umap_8d",
                "params": {"min_cluster_size": 5},
            },
        ],
        "llm": {
            "provider": "openai",
            "model": "gpt-4o-mini",
            "max_llm_calls": 200,
        },
    }


def create_project(project_name: str) -> Path:
    """Create a new project directory with default files.

    Returns the project directory path.
    Raises FileExistsError if the project already exists.
    """
    ensure_workspace()
    pdir = project_dir(project_name)

    if project_exists(project_name):
        raise FileExistsError(f"Project '{project_name}' already exists at {pdir}")

    pdir.mkdir(parents=True)
    (pdir / settings.ARTIFACTS_DIR).mkdir()

    # project.yaml
    project_meta = {"name": project_name}
    (pdir / settings.PROJECT_YAML).write_text(
        yaml.dump(project_meta, default_flow_style=False)
    )

    # plan.yaml
    (pdir / settings.PLAN_YAML).write_text(
        yaml.dump(default_plan(), default_flow_style=False)
    )

    return pdir
