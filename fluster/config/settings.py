"""Global paths and constants."""

from pathlib import Path

FLUSTER_HOME = Path.home() / ".fluster"
PROJECTS_DIR = FLUSTER_HOME / "projects"
SEED = 42

PROJECT_YAML = "project.yaml"
PLAN_YAML = "plan.yaml"
PROJECT_DB = "project.db"
ARTIFACTS_DIR = "artifacts"
