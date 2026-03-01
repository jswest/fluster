"""Global paths and constants."""

from pathlib import Path

FLUSTER_HOME = Path.home() / ".fluster"
PROJECTS_DIR = FLUSTER_HOME / "projects"
SEED = 42

PROJECT_YAML = "project.yaml"
PLAN_YAML = "plan.yaml"
PROJECT_DB = "project.db"
ARTIFACTS_DIR = "artifacts"

# Assumes development checkout (client/ lives next to fluster/ package dir).
CLIENT_DIR = Path(__file__).resolve().parent.parent.parent / "client"
