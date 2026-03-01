"""Tests for SQLite bootstrap (Phase 2)."""

from pathlib import Path

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


def test_row_factory_returns_rows(tmp_path):
    conn = connect(tmp_path)
    row = conn.execute("SELECT 1 AS val").fetchone()
    assert row["val"] == 1
    conn.close()
