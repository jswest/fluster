"""SQLite connection helper with WAL, JSON1, and sqlite-vec enforcement."""

import sqlite3
from pathlib import Path

import sqlite_vec
from loguru import logger

from fluster.config import settings
from fluster.db.schema import apply_schema


def connect(project_dir: Path) -> sqlite3.Connection:
    """Open (or create) the project database with WAL, JSON1, and sqlite-vec.

    Raises RuntimeError if JSON1 or sqlite-vec is not available.
    """
    db_path = project_dir / settings.PROJECT_DB
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    _assert_json1(conn)
    _load_sqlite_vec(conn)
    _ensure_schema_version(conn)
    apply_schema(conn)

    logger.debug(f"Connected to {db_path}")
    return conn


def _assert_json1(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("SELECT json_valid('{}')").fetchone()
    except sqlite3.OperationalError as exc:
        raise RuntimeError(
            "SQLite JSON1 extension is required but not available. "
            "Rebuild or install a SQLite build that includes JSON1."
        ) from exc


def _load_sqlite_vec(conn: sqlite3.Connection) -> None:
    try:
        conn.enable_load_extension(True)
        sqlite_vec.load(conn)
        conn.enable_load_extension(False)
    except AttributeError as exc:
        raise RuntimeError(
            "Python's sqlite3 module was compiled without extension loading support. "
            "Install Python via uv (`uv python install 3.11`) or Homebrew to get "
            "a build with SQLITE_ENABLE_LOAD_EXTENSION."
        ) from exc
    except sqlite3.OperationalError as exc:
        raise RuntimeError(
            f"Failed to load sqlite-vec extension: {exc}"
        ) from exc


def _ensure_schema_version(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            version     INTEGER PRIMARY KEY,
            applied_at  TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.execute("INSERT OR IGNORE INTO schema_version (version) VALUES (1)")
    conn.commit()
