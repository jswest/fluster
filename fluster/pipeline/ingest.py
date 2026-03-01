"""ingest_rows — CSV to rows + items + artifacts."""

import csv
import hashlib
import json
import mimetypes
import shutil
import sqlite3
from pathlib import Path


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _store_artifact(
    file_path: Path, artifacts_dir: Path
) -> tuple[str, str, str | None, int]:
    """Copy a file into the content-addressed artifact store.

    Returns (sha256, stored_path_relative, mime_type, file_bytes).
    """
    sha = _sha256_file(file_path)
    mime, _ = mimetypes.guess_type(str(file_path))
    ext = file_path.suffix or ""
    dest_dir = artifacts_dir / sha[:2]
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{sha}{ext}"

    if not dest.exists():
        shutil.copy2(file_path, dest)

    stored_relative = f"{sha[:2]}/{sha}{ext}"
    return sha, stored_relative, mime, file_path.stat().st_size


def _resolve_file(value: str, csv_dir: Path) -> Path | None:
    """If a cell value looks like a file path and the file exists, return it."""
    if not value or len(value) > 1024:
        return None
    candidate = Path(value)
    if candidate.is_absolute() and candidate.is_file():
        return candidate
    relative = csv_dir / candidate
    if relative.is_file():
        return relative
    return None


def ingest_rows(
    conn: sqlite3.Connection,
    csv_path: Path,
    project_dir: Path,
) -> dict:
    """Ingest a CSV file into the project database.

    Returns a summary dict with counts.
    """
    csv_path = csv_path.resolve()
    csv_dir = csv_path.parent
    artifacts_dir = project_dir / "artifacts"

    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError("CSV file has no header row")

        rows_created = 0
        items_created = 0
        artifacts_linked = 0

        for row_number, csv_row in enumerate(reader, start=1):
            # Determine row_name: use 'name' column if present.
            row_name = csv_row.get("name")

            # Everything else is metadata.
            metadata = {
                k: v for k, v in csv_row.items() if k != "name"
            }

            cur = conn.execute(
                "INSERT INTO rows (row_name, row_metadata_json, source_row_number) "
                "VALUES (?, ?, ?)",
                (row_name, json.dumps(metadata), row_number),
            )
            row_id = cur.lastrowid
            rows_created += 1

            cur = conn.execute(
                "INSERT INTO items (row_id) VALUES (?)", (row_id,)
            )
            item_id = cur.lastrowid
            items_created += 1

            # Check each cell for file references.
            for col_name, cell_value in csv_row.items():
                if not cell_value:
                    continue
                file_path = _resolve_file(cell_value, csv_dir)
                if file_path is None:
                    continue

                sha, stored_path, mime_type, file_bytes = _store_artifact(
                    file_path, artifacts_dir
                )

                # Insert artifact if not already present (content-addressed).
                conn.execute(
                    "INSERT OR IGNORE INTO artifacts "
                    "(artifact_id, original_path, stored_path, mime_type, bytes) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (sha, str(file_path), stored_path, mime_type, file_bytes),
                )

                conn.execute(
                    "INSERT OR IGNORE INTO item_artifacts (item_id, artifact_id, role) "
                    "VALUES (?, ?, ?)",
                    (item_id, sha, col_name),
                )
                artifacts_linked += 1

        conn.commit()

    return {
        "rows_created": rows_created,
        "items_created": items_created,
        "artifacts_linked": artifacts_linked,
    }
