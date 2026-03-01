"""Tests for SQLite bootstrap (Phase 2)."""

import sqlite3

import pytest

from fluster.db.connection import connect


def test_connect_returns_connection(tmp_path):
    conn = connect(tmp_path)
    assert conn is not None
    conn.close()


def test_wal_mode_enabled(tmp_path):
    conn = connect(tmp_path)
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode == "wal"
    conn.close()


def test_foreign_keys_enabled(tmp_path):
    conn = connect(tmp_path)
    fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
    assert fk == 1
    conn.close()


def test_json1_available(tmp_path):
    conn = connect(tmp_path)
    result = conn.execute("SELECT json_valid('{\"a\": 1}')").fetchone()[0]
    assert result == 1
    conn.close()


def test_schema_version_table_created(tmp_path):
    conn = connect(tmp_path)
    row = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
    assert row[0] == 1
    conn.close()


def test_schema_version_idempotent(tmp_path):
    conn1 = connect(tmp_path)
    conn1.close()

    conn2 = connect(tmp_path)
    row = conn2.execute("SELECT COUNT(*) FROM schema_version").fetchone()
    assert row[0] == 1
    conn2.close()


def test_sqlite_vec_loaded(tmp_path):
    conn = connect(tmp_path)
    version = conn.execute("SELECT vec_version()").fetchone()[0]
    assert version.startswith("v")
    conn.close()


def test_sqlite_vec_functional(tmp_path):
    conn = connect(tmp_path)
    # Create a vec0 virtual table and verify it works.
    conn.execute("CREATE VIRTUAL TABLE test_vec USING vec0(embedding float[3])")
    conn.execute(
        "INSERT INTO test_vec (rowid, embedding) VALUES (1, ?)",
        ["[1.0, 2.0, 3.0]"],
    )
    row = conn.execute("SELECT COUNT(*) FROM test_vec").fetchone()
    assert row[0] == 1
    conn.close()


def test_extension_loading_disabled_after_connect(tmp_path):
    conn = connect(tmp_path)
    # enable_load_extension should have been turned off after loading vec0.
    with pytest.raises(sqlite3.OperationalError):
        conn.load_extension("nonexistent")
    conn.close()


def test_row_factory_returns_rows(tmp_path):
    conn = connect(tmp_path)
    row = conn.execute("SELECT 1 AS val").fetchone()
    assert row["val"] == 1
    conn.close()
