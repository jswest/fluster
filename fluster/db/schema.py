"""Table definitions (Phases 3–4)."""

import sqlite3

CORE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS rows (
    row_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    row_name            TEXT,
    row_metadata_json   TEXT NOT NULL DEFAULT '{}',
    source_row_number   INTEGER,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    CHECK (json_valid(row_metadata_json))
);

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id     TEXT PRIMARY KEY,
    original_path   TEXT NOT NULL,
    stored_path     TEXT NOT NULL,
    mime_type       TEXT,
    bytes           INTEGER NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS items (
    item_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    row_id      INTEGER NOT NULL UNIQUE,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (row_id) REFERENCES rows (row_id)
);

CREATE TABLE IF NOT EXISTS item_artifacts (
    item_id     INTEGER NOT NULL,
    artifact_id TEXT NOT NULL,
    role        TEXT NOT NULL DEFAULT 'source',
    PRIMARY KEY (item_id, artifact_id),
    FOREIGN KEY (item_id)     REFERENCES items (item_id),
    FOREIGN KEY (artifact_id) REFERENCES artifacts (artifact_id)
);

CREATE TABLE IF NOT EXISTS representations (
    representation_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id             INTEGER NOT NULL,
    representation_type TEXT NOT NULL,
    text                TEXT NOT NULL,
    text_hash           TEXT NOT NULL,
    model_name          TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (item_id) REFERENCES items (item_id),
    CHECK (representation_type IN ('embedding_text', 'extracted_text', 'summary'))
);
"""

JOBS_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type            TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'queued',
    input_params_json   TEXT NOT NULL DEFAULT '{}',
    progress_json       TEXT NOT NULL DEFAULT '{}',
    cancel_requested_at TEXT,
    started_at          TEXT,
    finished_at         TEXT,
    error_message       TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'canceled')),
    CHECK (json_valid(input_params_json)),
    CHECK (json_valid(progress_json))
);

CREATE TABLE IF NOT EXISTS job_logs (
    job_log_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          INTEGER NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    level           TEXT NOT NULL DEFAULT 'info',
    message         TEXT NOT NULL,
    payload_json    TEXT,
    FOREIGN KEY (job_id) REFERENCES jobs (job_id),
    CHECK (level IN ('debug', 'info', 'warning', 'error'))
);
"""


def apply_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(CORE_TABLES_SQL)
    conn.executescript(JOBS_TABLES_SQL)
