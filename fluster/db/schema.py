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


EMBEDDINGS_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS embeddings (
    embedding_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    representation_id   INTEGER NOT NULL,
    model_name          TEXT NOT NULL,
    dimensions          INTEGER NOT NULL,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (representation_id) REFERENCES representations (representation_id)
);
"""

# vec0 virtual tables can't use IF NOT EXISTS, so we handle that in code.
VEC_EMBEDDINGS_DDL = """
CREATE VIRTUAL TABLE vec_embeddings USING vec0(
    embedding_id INTEGER PRIMARY KEY,
    vector float[{dimensions}]
);
"""


REDUCTIONS_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS reductions (
    reduction_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    embedding_reference TEXT NOT NULL,
    method              TEXT NOT NULL,
    target_dimensions   INTEGER NOT NULL,
    params_json         TEXT NOT NULL DEFAULT '{}',
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    CHECK (method IN ('pca', 'umap')),
    CHECK (json_valid(params_json))
);

CREATE TABLE IF NOT EXISTS reduction_coordinates (
    reduction_id    INTEGER NOT NULL,
    item_id         INTEGER NOT NULL,
    coordinates_json TEXT NOT NULL,
    PRIMARY KEY (reduction_id, item_id),
    FOREIGN KEY (reduction_id) REFERENCES reductions (reduction_id),
    FOREIGN KEY (item_id)      REFERENCES items (item_id),
    CHECK (json_valid(coordinates_json))
);
"""


CLUSTERING_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS cluster_runs (
    cluster_run_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    reduction_id    INTEGER NOT NULL,
    method          TEXT NOT NULL,
    params_json     TEXT NOT NULL DEFAULT '{}',
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (reduction_id) REFERENCES reductions (reduction_id),
    CHECK (method IN ('hdbscan')),
    CHECK (json_valid(params_json))
);

CREATE TABLE IF NOT EXISTS cluster_assignments (
    cluster_run_id          INTEGER NOT NULL,
    item_id                 INTEGER NOT NULL,
    cluster_id              INTEGER NOT NULL,
    membership_probability  REAL NOT NULL,
    PRIMARY KEY (cluster_run_id, item_id),
    FOREIGN KEY (cluster_run_id) REFERENCES cluster_runs (cluster_run_id),
    FOREIGN KEY (item_id)        REFERENCES items (item_id)
);
"""


EXEMPLARS_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS cluster_exemplars (
    cluster_run_id  INTEGER NOT NULL,
    cluster_id      INTEGER NOT NULL,
    item_id         INTEGER NOT NULL,
    rank            INTEGER NOT NULL,
    score           REAL NOT NULL,
    PRIMARY KEY (cluster_run_id, cluster_id, item_id),
    FOREIGN KEY (cluster_run_id) REFERENCES cluster_runs (cluster_run_id),
    FOREIGN KEY (item_id)        REFERENCES items (item_id)
);
"""


LLM_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS llm_calls (
    llm_call_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id              INTEGER,
    task_name           TEXT NOT NULL,
    provider            TEXT NOT NULL,
    model               TEXT NOT NULL,
    input_json          TEXT NOT NULL,
    output_raw_text     TEXT,
    output_parsed_json  TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (job_id) REFERENCES jobs (job_id),
    CHECK (json_valid(input_json)),
    CHECK (output_parsed_json IS NULL OR json_valid(output_parsed_json))
);
"""


LABEL_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS cluster_summaries (
    cluster_summary_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    cluster_run_id      INTEGER NOT NULL,
    cluster_id          INTEGER NOT NULL,
    provider            TEXT NOT NULL,
    model               TEXT NOT NULL,
    label               TEXT NOT NULL,
    label_json          TEXT NOT NULL,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (cluster_run_id) REFERENCES cluster_runs (cluster_run_id),
    UNIQUE (cluster_run_id, cluster_id, provider, model),
    CHECK (json_valid(label_json))
);
"""


CRITIQUE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS cluster_run_critiques (
    critique_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    cluster_run_id  INTEGER NOT NULL,
    provider        TEXT NOT NULL,
    model           TEXT NOT NULL,
    critique_json   TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (cluster_run_id) REFERENCES cluster_runs (cluster_run_id),
    UNIQUE (cluster_run_id, provider, model),
    CHECK (json_valid(critique_json))
);
"""


def apply_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(CORE_TABLES_SQL)
    conn.executescript(JOBS_TABLES_SQL)
    conn.executescript(EMBEDDINGS_TABLES_SQL)
    conn.executescript(REDUCTIONS_TABLES_SQL)
    conn.executescript(CLUSTERING_TABLES_SQL)
    conn.executescript(EXEMPLARS_TABLES_SQL)
    conn.executescript(LLM_TABLES_SQL)
    conn.executescript(LABEL_TABLES_SQL)
    conn.executescript(CRITIQUE_TABLES_SQL)


def ensure_vec_table(conn: sqlite3.Connection, dimensions: int) -> None:
    """Create the vec_embeddings virtual table if it doesn't exist."""
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='vec_embeddings'"
    ).fetchone()
    if not exists:
        # vec0 DDL doesn't support parameterized dimensions; value is always int from model.
        conn.execute(VEC_EMBEDDINGS_DDL.format(dimensions=dimensions))
