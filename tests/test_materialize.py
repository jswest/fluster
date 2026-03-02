"""Tests for materialize_items (Phase 7)."""

import hashlib
import json
from unittest.mock import MagicMock

import pytest
from PIL import Image

from fluster.db.connection import connect
from fluster.pipeline.ingest import ingest_rows
from fluster.pipeline import materialize as materialize_mod
from fluster.pipeline.materialize import materialize_items



def _write_csv(path, header, *rows):
    csv_file = path / "data.csv"
    csv_file.write_text("\n".join([header] + list(rows)) + "\n")
    return csv_file


def _ingest_and_materialize(pdir, conn, header, *rows):
    csv_file = _write_csv(pdir, header, *rows)
    ingest_rows(conn, csv_file, pdir)
    return materialize_items(conn, pdir)


# --- Basic materialization ---

def test_materialize_creates_representations(project):
    pdir, conn = project

    test_file = pdir / "hello.txt"
    test_file.write_text("Hello, world!")

    summary = _ingest_and_materialize(
        pdir, conn, "name,file_path", f"greeting,{test_file}"
    )

    assert summary["materialized"] == 1
    assert summary["skipped"] == 0

    rep = conn.execute(
        "SELECT * FROM representations WHERE representation_type = 'embedding_text'"
    ).fetchone()
    assert rep is not None
    assert "greeting" in rep["text"]
    assert "Hello, world!" in rep["text"]
    assert rep["text_hash"] is not None


def test_materialize_includes_row_name(project):
    pdir, conn = project

    test_file = pdir / "doc.txt"
    test_file.write_text("some content")

    _ingest_and_materialize(pdir, conn, "name,file_path", f"my-doc,{test_file}")

    rep = conn.execute("SELECT text FROM representations").fetchone()
    assert rep["text"].startswith("my-doc")


def test_materialize_includes_metadata(project):
    pdir, conn = project

    test_file = pdir / "doc.txt"
    test_file.write_text("content")

    _ingest_and_materialize(
        pdir, conn, "name,category,score,file_path",
        f"item1,Science,95,{test_file}",
    )

    rep = conn.execute("SELECT text FROM representations").fetchone()
    text = rep["text"]
    assert "category: Science" in text
    assert "score: 95" in text


def test_materialize_includes_extracted_text(project):
    pdir, conn = project

    test_file = pdir / "article.txt"
    test_file.write_text("The quick brown fox jumps over the lazy dog.")

    _ingest_and_materialize(pdir, conn, "name,file_path", f"fox,{test_file}")

    rep = conn.execute("SELECT text FROM representations").fetchone()
    assert "The quick brown fox" in rep["text"]


def test_materialize_no_name_column(project):
    pdir, conn = project

    test_file = pdir / "doc.txt"
    test_file.write_text("just text")

    _ingest_and_materialize(pdir, conn, "tag,file_path", f"info,{test_file}")

    rep = conn.execute("SELECT text FROM representations").fetchone()
    assert "tag: info" in rep["text"]
    assert "just text" in rep["text"]


# --- Idempotency ---

def test_materialize_is_idempotent(project):
    pdir, conn = project

    test_file = pdir / "doc.txt"
    test_file.write_text("content")

    _ingest_and_materialize(pdir, conn, "name,file_path", f"doc,{test_file}")

    # Second materialize should find nothing new.
    summary = materialize_items(conn, pdir)
    assert summary["materialized"] == 0

    count = conn.execute("SELECT COUNT(*) FROM representations").fetchone()[0]
    assert count == 1


# --- Edge cases ---

def test_materialize_empty_artifact_uses_metadata(project):
    """An item with an empty/binary artifact should still get metadata in embedding_text."""
    pdir, conn = project

    # Binary file — can't extract text.
    bin_file = pdir / "data.bin"
    bin_file.write_bytes(b"\x00\x01\x02\x03")

    _ingest_and_materialize(
        pdir, conn, "name,category,file_path",
        f"binary-item,Science,{bin_file}",
    )

    rep = conn.execute("SELECT text FROM representations").fetchone()
    assert "binary-item" in rep["text"]
    assert "category: Science" in rep["text"]


def test_materialize_empty_file_path(project):
    """Items with no artifact should still materialize from name/metadata."""
    pdir, conn = project

    csv_file = _write_csv(pdir, "name,category,file_path", "thing,Art,")
    ingest_rows(conn, csv_file, pdir)
    summary = materialize_items(conn, pdir)

    assert summary["materialized"] == 1

    rep = conn.execute("SELECT text FROM representations").fetchone()
    assert "thing" in rep["text"]
    assert "category: Art" in rep["text"]


def test_materialize_multiple_items(project):
    pdir, conn = project

    for name in ("a", "b", "c"):
        (pdir / f"{name}.txt").write_text(f"{name} content")

    _ingest_and_materialize(
        pdir, conn, "name,file_path",
        f"a,{pdir}/a.txt",
        f"b,{pdir}/b.txt",
        f"c,{pdir}/c.txt",
    )

    count = conn.execute("SELECT COUNT(*) FROM representations").fetchone()[0]
    assert count == 3


def test_text_hash_is_sha256(project):
    pdir, conn = project

    test_file = pdir / "doc.txt"
    test_file.write_text("same content")

    _ingest_and_materialize(pdir, conn, "name,file_path", f"doc,{test_file}")

    rep = conn.execute("SELECT text, text_hash FROM representations").fetchone()

    expected = hashlib.sha256(rep["text"].encode("utf-8")).hexdigest()
    assert rep["text_hash"] == expected


def test_materialize_preserves_falsy_metadata(project):
    """Metadata values like '0' should not be dropped."""
    pdir, conn = project

    test_file = pdir / "doc.txt"
    test_file.write_text("content")

    _ingest_and_materialize(
        pdir, conn, "name,score,file_path",
        f"item,0,{test_file}",
    )

    rep = conn.execute("SELECT text FROM representations").fetchone()
    assert "score: 0" in rep["text"]


# --- Image captioning ---

def _create_test_image(path, width=4, height=4):
    """Create a minimal valid PNG image for testing."""
    img = Image.new("RGB", (width, height), color=(255, 0, 0))
    img.save(path, format="PNG")
    return path


def _patch_caption(monkeypatch, caption_text="A test image of a red square"):
    """Patch _load_caption_model and _caption_image to avoid loading FastVLM."""
    sentinel = (MagicMock(name="fake-model"), MagicMock(name="fake-processor"), "cpu")
    monkeypatch.setattr(materialize_mod, "_load_caption_model", lambda: sentinel)
    calls = []

    def _fake_caption(stored_path, project_dir, model, processor, device):
        calls.append(stored_path)
        return caption_text

    monkeypatch.setattr(materialize_mod, "_caption_image", _fake_caption)
    return calls


def test_materialize_image_creates_caption(project, monkeypatch):
    """An image artifact should be captioned and included in embedding_text."""
    pdir, conn = project

    img_file = pdir / "photo.png"
    _create_test_image(img_file)

    calls = _patch_caption(monkeypatch)

    summary = _ingest_and_materialize(
        pdir, conn, "name,file_path", f"my-photo,{img_file}"
    )

    assert summary["materialized"] == 1
    assert len(calls) == 1

    rep = conn.execute("SELECT text FROM representations").fetchone()
    assert "my-photo" in rep["text"]
    assert "A test image of a red square" in rep["text"]


def test_materialize_mixed_text_and_image(project, monkeypatch):
    """Both text and image items should materialize in the same batch."""
    pdir, conn = project

    txt_file = pdir / "doc.txt"
    txt_file.write_text("Some document content")

    img_file = pdir / "photo.jpg"
    _create_test_image(img_file)

    _patch_caption(monkeypatch)

    summary = _ingest_and_materialize(
        pdir, conn, "name,file_path",
        f"doc,{txt_file}",
        f"photo,{img_file}",
    )

    assert summary["materialized"] == 2

    reps = conn.execute(
        "SELECT text FROM representations ORDER BY representation_id"
    ).fetchall()

    # Text item has extracted text
    assert "Some document content" in reps[0]["text"]
    # Image item has caption
    assert "A test image of a red square" in reps[1]["text"]


def test_materialize_image_with_metadata(project, monkeypatch):
    """Image item should include both metadata and caption in embedding_text."""
    pdir, conn = project

    img_file = pdir / "sunset.png"
    _create_test_image(img_file)

    _patch_caption(monkeypatch)

    _ingest_and_materialize(
        pdir, conn, "name,category,file_path",
        f"sunset,Nature,{img_file}",
    )

    rep = conn.execute("SELECT text FROM representations").fetchone()
    assert "sunset" in rep["text"]
    assert "category: Nature" in rep["text"]
    assert "A test image of a red square" in rep["text"]


def test_materialize_image_caption_failure_falls_back(project, monkeypatch):
    """If captioning fails, the item should still materialize from name/metadata."""
    pdir, conn = project

    img_file = pdir / "broken.png"
    _create_test_image(img_file)

    _patch_caption(monkeypatch, caption_text="")

    summary = _ingest_and_materialize(
        pdir, conn, "name,category,file_path",
        f"broken-img,Art,{img_file}",
    )

    assert summary["materialized"] == 1

    rep = conn.execute("SELECT text FROM representations").fetchone()
    assert "broken-img" in rep["text"]
    assert "category: Art" in rep["text"]


def test_materialize_image_empty_caption_no_metadata_gets_filename(project, monkeypatch):
    """An image with no name, no metadata, and empty caption should use filename as fallback."""
    pdir, conn = project

    img_file = pdir / "empty.png"
    _create_test_image(img_file)

    _patch_caption(monkeypatch, caption_text="")

    summary = _ingest_and_materialize(
        pdir, conn, "file_path", f"{img_file}"
    )

    assert summary["materialized"] == 1
    assert summary["skipped"] == 0

    rep = conn.execute("SELECT text FROM representations").fetchone()
    assert "empty" in rep["text"]


def test_materialize_caption_images_false_skips_model(project, monkeypatch):
    """caption_images=False skips captioning; item still gets representation from name."""
    pdir, conn = project

    img_file = pdir / "photo.png"
    _create_test_image(img_file)

    load_calls = []
    monkeypatch.setattr(
        materialize_mod, "_load_caption_model",
        lambda: load_calls.append(1) or MagicMock(),
    )

    csv_file = _write_csv(pdir, "name,file_path", f"my-photo,{img_file}")
    ingest_rows(conn, csv_file, pdir)
    summary = materialize_items(conn, pdir, caption_images=False)

    assert summary["materialized"] == 1
    assert len(load_calls) == 0

    rep = conn.execute("SELECT text FROM representations").fetchone()
    assert "my-photo" in rep["text"]


def test_materialize_caption_images_false_no_name_gets_filename(project, monkeypatch):
    """caption_images=False with no name/metadata uses filename as fallback."""
    pdir, conn = project

    img_file = pdir / "landscape.png"
    _create_test_image(img_file)

    load_calls = []
    monkeypatch.setattr(
        materialize_mod, "_load_caption_model",
        lambda: load_calls.append(1) or MagicMock(),
    )

    csv_file = _write_csv(pdir, "file_path", f"{img_file}")
    ingest_rows(conn, csv_file, pdir)
    summary = materialize_items(conn, pdir, caption_images=False)

    assert summary["materialized"] == 1
    assert len(load_calls) == 0

    rep = conn.execute("SELECT text FROM representations").fetchone()
    assert "landscape" in rep["text"]
