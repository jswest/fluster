"""Tests for ingest_rows (Phase 6)."""

import json
from pathlib import Path

import pytest

from fluster.db.connection import connect
from fluster.pipeline.ingest import ingest_rows


@pytest.fixture
def project(tmp_path):
    """Set up a minimal project directory with a db connection."""
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    conn = connect(tmp_path)
    yield tmp_path, conn
    conn.close()


def _write_csv(path: Path, header: str, *rows: str) -> Path:
    csv_file = path / "data.csv"
    csv_file.write_text("\n".join([header] + list(rows)) + "\n")
    return csv_file


# --- Basic ingestion ---

def test_ingest_simple_csv(project):
    pdir, conn = project
    csv_file = _write_csv(pdir, "name,color", "apple,red", "banana,yellow")

    summary = ingest_rows(conn, csv_file, pdir)

    assert summary["rows_created"] == 2
    assert summary["items_created"] == 2

    rows = conn.execute("SELECT * FROM rows ORDER BY source_row_number").fetchall()
    assert len(rows) == 2
    assert rows[0]["row_name"] == "apple"
    assert json.loads(rows[0]["row_metadata_json"]) == {"color": "red"}
    assert rows[0]["source_row_number"] == 1
    assert rows[1]["row_name"] == "banana"


def test_ingest_creates_items_one_to_one(project):
    pdir, conn = project
    csv_file = _write_csv(pdir, "name", "a", "b", "c")

    ingest_rows(conn, csv_file, pdir)

    items = conn.execute(
        "SELECT i.item_id, r.row_name FROM items i "
        "JOIN rows r ON i.row_id = r.row_id ORDER BY i.item_id"
    ).fetchall()
    assert len(items) == 3
    assert items[0]["row_name"] == "a"


def test_ingest_no_name_column(project):
    pdir, conn = project
    csv_file = _write_csv(pdir, "color,size", "red,big", "blue,small")

    ingest_rows(conn, csv_file, pdir)

    rows = conn.execute("SELECT * FROM rows ORDER BY source_row_number").fetchall()
    assert rows[0]["row_name"] is None
    assert json.loads(rows[0]["row_metadata_json"]) == {"color": "red", "size": "big"}


def test_ingest_all_metadata_preserved(project):
    pdir, conn = project
    csv_file = _write_csv(
        pdir, "name,category,score,notes",
        "item1,A,95,good stuff"
    )

    ingest_rows(conn, csv_file, pdir)

    row = conn.execute("SELECT * FROM rows").fetchone()
    meta = json.loads(row["row_metadata_json"])
    assert meta == {"category": "A", "score": "95", "notes": "good stuff"}


# --- Artifact ingestion ---

def test_ingest_with_file_artifact(project):
    pdir, conn = project

    # Create a file to reference.
    test_file = pdir / "hello.txt"
    test_file.write_text("Hello, world!")

    csv_file = _write_csv(pdir, "name,document", f"doc1,{test_file}")

    summary = ingest_rows(conn, csv_file, pdir)
    assert summary["artifacts_linked"] == 1

    artifact = conn.execute("SELECT * FROM artifacts").fetchone()
    assert artifact["mime_type"] == "text/plain"
    assert artifact["bytes"] == test_file.stat().st_size

    # Verify the file was copied to the content-addressed store.
    stored = pdir / "artifacts" / artifact["stored_path"]
    assert stored.is_file()
    assert stored.read_text() == "Hello, world!"


def test_ingest_with_relative_path_artifact(project):
    pdir, conn = project

    # Create a file next to the CSV.
    test_file = pdir / "report.pdf"
    test_file.write_bytes(b"%PDF-fake-content")

    # Use a relative path in the CSV.
    csv_file = _write_csv(pdir, "name,file", "r1,report.pdf")

    summary = ingest_rows(conn, csv_file, pdir)
    assert summary["artifacts_linked"] == 1


def test_ingest_deduplicates_artifacts(project):
    pdir, conn = project

    # Same file referenced by two rows.
    test_file = pdir / "shared.txt"
    test_file.write_text("shared content")

    csv_file = _write_csv(
        pdir, "name,doc",
        f"a,{test_file}",
        f"b,{test_file}",
    )

    summary = ingest_rows(conn, csv_file, pdir)
    # Both rows reference the artifact, but only one copy stored.
    assert summary["artifacts_linked"] == 2  # 2 references

    artifacts = conn.execute("SELECT COUNT(*) FROM artifacts").fetchone()[0]
    assert artifacts == 1  # 1 unique artifact

    links = conn.execute("SELECT COUNT(*) FROM item_artifacts").fetchone()[0]
    assert links == 2  # 2 item-artifact links


def test_artifact_content_addressed_path(project):
    pdir, conn = project

    test_file = pdir / "test.txt"
    test_file.write_text("content for hashing")

    csv_file = _write_csv(pdir, "name,file", f"x,{test_file}")
    ingest_rows(conn, csv_file, pdir)

    artifact = conn.execute("SELECT * FROM artifacts").fetchone()
    sha = artifact["artifact_id"]
    stored_path = artifact["stored_path"]

    # Should match pattern: <sha[:2]>/<sha>.<ext>
    assert stored_path == f"{sha[:2]}/{sha}.txt"


# --- Edge cases ---

def test_ingest_empty_csv(project):
    pdir, conn = project
    csv_file = _write_csv(pdir, "name,color")  # header only

    summary = ingest_rows(conn, csv_file, pdir)
    assert summary["rows_created"] == 0


def test_ingest_missing_csv(project):
    pdir, conn = project
    with pytest.raises(FileNotFoundError):
        ingest_rows(conn, pdir / "nonexistent.csv", pdir)


def test_ingest_nonexistent_file_reference_ignored(project):
    """Cell values that look like paths but don't exist should be treated as data."""
    pdir, conn = project
    csv_file = _write_csv(pdir, "name,file", "x,/tmp/no_such_file_12345.txt")

    summary = ingest_rows(conn, csv_file, pdir)
    assert summary["artifacts_linked"] == 0
    assert summary["rows_created"] == 1
