"""SQLite connection helper with WAL + JSON1 enforcement."""

import sqlite3
from pathlib import Path

from loguru import logger

from fluster.config import settings


def connect(project_dir: Path) -> sqlite3.Connection:
    """Open (or create) the project database with WAL and JSON1.

    Raises RuntimeError if JSON1 is not available.
    """
    db_path = project_dir / settings.PROJECT_DB
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    _assert_json1(conn)
    _ensure_schema_version(conn)

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


def _ensure_schema_version(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            version     INTEGER PRIMARY KEY,
            applied_at  TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.execute("INSERT OR IGNORE INTO schema_version (version) VALUES (1)")
    conn.commit()
