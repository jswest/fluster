"""Global paths and constants."""

from pathlib import Path

FLUSTER_HOME = Path.home() / ".fluster"
PROJECTS_DIR = FLUSTER_HOME / "projects"
SEED = 42

PROJECT_YAML = "project.yaml"
PLAN_YAML = "plan.yaml"
PROJECT_DB = "project.db"
ARTIFACTS_DIR = "artifacts"
SECRETS_FILE = FLUSTER_HOME / "secrets.yaml"
ACTIVE_PROJECT_FILE = FLUSTER_HOME / "active_project"

# Assumes development checkout (client/ lives next to fluster/ package dir).
CLIENT_DIR = Path(__file__).resolve().parent.parent.parent / "client"
