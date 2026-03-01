"""Tests for core tables (Phase 3)."""

import json

import pytest

from fluster.db.connection import connect


@pytest.fixture
def conn(tmp_path):
    c = connect(tmp_path)
    yield c
    c.close()


EXPECTED_TABLES = ["rows", "artifacts", "items", "item_artifacts", "representations"]


def test_core_tables_exist(conn):
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    for name in EXPECTED_TABLES:
        assert name in tables, f"Missing table: {name}"


def test_insert_row(conn):
    conn.execute(
        "INSERT INTO rows (row_name, row_metadata_json, source_row_number) "
        "VALUES (?, ?, ?)",
        ("test-row", '{"key": "value"}', 1),
    )
    row = conn.execute("SELECT * FROM rows WHERE row_name = 'test-row'").fetchone()
    assert row["row_name"] == "test-row"
    assert json.loads(row["row_metadata_json"]) == {"key": "value"}
    assert row["source_row_number"] == 1
    assert row["created_at"] is not None


def test_row_metadata_json_check(conn):
    with pytest.raises(Exception):
        conn.execute(
            "INSERT INTO rows (row_metadata_json) VALUES (?)",
            ("not-json",),
        )


def test_insert_artifact(conn):
    conn.execute(
        "INSERT INTO artifacts (artifact_id, original_path, stored_path, mime_type, bytes) "
        "VALUES (?, ?, ?, ?, ?)",
        ("abcdef1234", "/tmp/test.txt", "ab/abcdef.txt", "text/plain", 42),
    )
    row = conn.execute(
        "SELECT * FROM artifacts WHERE artifact_id = 'abcdef1234'"
    ).fetchone()
    assert row["original_path"] == "/tmp/test.txt"
    assert row["bytes"] == 42


def test_artifact_id_unique(conn):
    conn.execute(
        "INSERT INTO artifacts (artifact_id, original_path, stored_path, mime_type, bytes) "
        "VALUES (?, ?, ?, ?, ?)",
        ("abcdef", "/tmp/a.txt", "ab/abcdef.txt", "text/plain", 10),
    )
    with pytest.raises(Exception):
        conn.execute(
            "INSERT INTO artifacts (artifact_id, original_path, stored_path, mime_type, bytes) "
            "VALUES (?, ?, ?, ?, ?)",
            ("abcdef", "/tmp/b.txt", "ab/abcdef.txt", "text/plain", 20),
        )


def test_item_row_one_to_one(conn):
    cur = conn.execute("INSERT INTO rows (row_name) VALUES ('r1')")
    row_id = cur.lastrowid

    conn.execute("INSERT INTO items (row_id) VALUES (?)", (row_id,))

    # Inserting another item with the same row_id should fail (UNIQUE).
    with pytest.raises(Exception):
        conn.execute("INSERT INTO items (row_id) VALUES (?)", (row_id,))


def test_item_artifacts_composite_pk(conn):
    cur = conn.execute("INSERT INTO rows (row_name) VALUES ('r1')")
    row_id = cur.lastrowid
    cur = conn.execute("INSERT INTO items (row_id) VALUES (?)", (row_id,))
    item_id = cur.lastrowid

    artifact_id = "abc123sha"
    conn.execute(
        "INSERT INTO artifacts (artifact_id, original_path, stored_path, mime_type, bytes) "
        "VALUES (?, ?, ?, ?, ?)",
        (artifact_id, "/tmp/a.txt", "ab/abc.txt", "text/plain", 5),
    )

    conn.execute(
        "INSERT INTO item_artifacts (item_id, artifact_id, role) VALUES (?, ?, ?)",
        (item_id, artifact_id, "source"),
    )
    # Duplicate PK should fail.
    with pytest.raises(Exception):
        conn.execute(
            "INSERT INTO item_artifacts (item_id, artifact_id, role) VALUES (?, ?, ?)",
            (item_id, artifact_id, "source"),
        )


def test_representation_type_check(conn):
    cur = conn.execute("INSERT INTO rows (row_name) VALUES ('r1')")
    row_id = cur.lastrowid
    cur = conn.execute("INSERT INTO items (row_id) VALUES (?)", (row_id,))
    item_id = cur.lastrowid

    # Valid type should work.
    conn.execute(
        "INSERT INTO representations (item_id, representation_type, text, text_hash) "
        "VALUES (?, ?, ?, ?)",
        (item_id, "embedding_text", "hello world", "abc123"),
    )

    # Invalid type should fail.
    with pytest.raises(Exception):
        conn.execute(
            "INSERT INTO representations (item_id, representation_type, text, text_hash) "
            "VALUES (?, ?, ?, ?)",
            (item_id, "invalid_type", "hello world", "abc123"),
        )


def test_representations_model_name_nullable(conn):
    cur = conn.execute("INSERT INTO rows (row_name) VALUES ('r1')")
    row_id = cur.lastrowid
    cur = conn.execute("INSERT INTO items (row_id) VALUES (?)", (row_id,))
    item_id = cur.lastrowid

    conn.execute(
        "INSERT INTO representations (item_id, representation_type, text, text_hash, model_name) "
        "VALUES (?, ?, ?, ?, ?)",
        (item_id, "summary", "a summary", "hash1", "gpt-4o"),
    )
    conn.execute(
        "INSERT INTO representations (item_id, representation_type, text, text_hash) "
        "VALUES (?, ?, ?, ?)",
        (item_id, "extracted_text", "extracted", "hash2"),
    )

    rows = conn.execute(
        "SELECT model_name FROM representations WHERE item_id = ?", (item_id,)
    ).fetchall()
    assert rows[0]["model_name"] == "gpt-4o"
    assert rows[1]["model_name"] is None


def test_foreign_key_enforcement(conn):
    """FK constraints should reject references to nonexistent rows."""
    # item with nonexistent row_id should fail.
    with pytest.raises(Exception):
        conn.execute("INSERT INTO items (row_id) VALUES (?)", (9999,))


def test_schema_idempotent(conn):
    """Calling apply_schema twice on the same db should not fail."""
    from fluster.db.schema import apply_schema
    apply_schema(conn)
    tables = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN "
        "('rows','artifacts','items','item_artifacts','representations')"
    ).fetchone()[0]
    assert tables == 5
