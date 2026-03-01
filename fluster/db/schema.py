"""Core table definitions (Phase 3)."""

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


def apply_core_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(CORE_TABLES_SQL)
