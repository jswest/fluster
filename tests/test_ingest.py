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

    test_file = pdir / "apple.txt"
    test_file.write_text("apple data")

    csv_file = _write_csv(
        pdir, "name,color,file_path",
        f"apple,red,{test_file}",
    )

    summary = ingest_rows(conn, csv_file, pdir)

    assert summary["rows_created"] == 1
    assert summary["items_created"] == 1

    row = conn.execute("SELECT * FROM rows").fetchone()
    assert row["row_name"] == "apple"
    assert json.loads(row["row_metadata_json"]) == {"color": "red"}
    assert row["source_row_number"] == 1


def test_ingest_creates_items_one_to_one(project):
    pdir, conn = project

    for name in ("a", "b", "c"):
        (pdir / f"{name}.txt").write_text(f"{name} data")

    csv_file = _write_csv(
        pdir, "name,file_path",
        f"a,{pdir}/a.txt",
        f"b,{pdir}/b.txt",
        f"c,{pdir}/c.txt",
    )

    ingest_rows(conn, csv_file, pdir)

    items = conn.execute(
        "SELECT i.item_id, r.row_name FROM items i "
        "JOIN rows r ON i.row_id = r.row_id ORDER BY i.item_id"
    ).fetchall()
    assert len(items) == 3
    assert items[0]["row_name"] == "a"


def test_ingest_metadata_excludes_special_columns(project):
    pdir, conn = project

    test_file = pdir / "item.txt"
    test_file.write_text("data")

    csv_file = _write_csv(
        pdir, "name,category,score,file_path",
        f"item1,A,95,{test_file}",
    )

    ingest_rows(conn, csv_file, pdir)

    row = conn.execute("SELECT * FROM rows").fetchone()
    meta = json.loads(row["row_metadata_json"])
    # name and file_path should NOT be in metadata.
    assert meta == {"category": "A", "score": "95"}


# --- file_path header requirement ---

def test_ingest_missing_file_path_header(project):
    pdir, conn = project
    csv_file = _write_csv(pdir, "name,color", "apple,red")

    with pytest.raises(ValueError, match="file_path"):
        ingest_rows(conn, csv_file, pdir)


# --- Artifact ingestion ---

def test_ingest_with_file_artifact(project):
    pdir, conn = project

    test_file = pdir / "hello.txt"
    test_file.write_text("Hello, world!")

    csv_file = _write_csv(pdir, "name,file_path", f"doc1,{test_file}")

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

    test_file = pdir / "report.pdf"
    test_file.write_bytes(b"%PDF-fake-content")

    csv_file = _write_csv(pdir, "name,file_path", "r1,report.pdf")

    summary = ingest_rows(conn, csv_file, pdir)
    assert summary["artifacts_linked"] == 1


def test_ingest_deduplicates_artifacts(project):
    pdir, conn = project

    test_file = pdir / "shared.txt"
    test_file.write_text("shared content")

    csv_file = _write_csv(
        pdir, "name,file_path",
        f"a,{test_file}",
        f"b,{test_file}",
    )

    summary = ingest_rows(conn, csv_file, pdir)
    assert summary["artifacts_linked"] == 2  # 2 references

    artifacts = conn.execute("SELECT COUNT(*) FROM artifacts").fetchone()[0]
    assert artifacts == 1  # 1 unique artifact

    links = conn.execute("SELECT COUNT(*) FROM item_artifacts").fetchone()[0]
    assert links == 2  # 2 item-artifact links


def test_artifact_content_addressed_path(project):
    pdir, conn = project

    test_file = pdir / "test.txt"
    test_file.write_text("content for hashing")

    csv_file = _write_csv(pdir, "name,file_path", f"x,{test_file}")
    ingest_rows(conn, csv_file, pdir)

    artifact = conn.execute("SELECT * FROM artifacts").fetchone()
    sha = artifact["artifact_id"]
    stored_path = artifact["stored_path"]

    assert stored_path == f"{sha[:2]}/{sha}.txt"


def test_artifact_role_is_source(project):
    pdir, conn = project

    test_file = pdir / "doc.txt"
    test_file.write_text("document")

    csv_file = _write_csv(pdir, "name,file_path", f"x,{test_file}")
    ingest_rows(conn, csv_file, pdir)

    link = conn.execute("SELECT * FROM item_artifacts").fetchone()
    assert link["role"] == "source"


# --- Edge cases ---

def test_ingest_empty_csv(project):
    pdir, conn = project
    csv_file = _write_csv(pdir, "name,file_path")  # header only

    summary = ingest_rows(conn, csv_file, pdir)
    assert summary["rows_created"] == 0


def test_ingest_missing_csv(project):
    pdir, conn = project
    with pytest.raises(FileNotFoundError):
        ingest_rows(conn, pdir / "nonexistent.csv", pdir)


def test_ingest_nonexistent_file_path_errors(project):
    """A file_path value that doesn't resolve should raise an error."""
    pdir, conn = project
    csv_file = _write_csv(
        pdir, "name,file_path", "x,/tmp/no_such_file_12345.txt"
    )

    with pytest.raises(FileNotFoundError, match="Row 1"):
        ingest_rows(conn, csv_file, pdir)


def test_ingest_empty_file_path_skipped(project):
    """Rows with an empty file_path should still be created, just no artifact."""
    pdir, conn = project
    csv_file = _write_csv(pdir, "name,file_path", "x,")

    summary = ingest_rows(conn, csv_file, pdir)
    assert summary["rows_created"] == 1
    assert summary["artifacts_linked"] == 0
