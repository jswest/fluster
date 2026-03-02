"""Tests for embed_items (Phase 8)."""

from unittest.mock import MagicMock

import pytest
from PIL import Image

from fluster.config.plan import Plan
from fluster.db.connection import connect
from fluster.db.schema import ensure_vec_table
from fluster.jobs.manager import create_job, request_cancel
from fluster.pipeline import embed as embed_mod
from fluster.pipeline import materialize as materialize_mod
from fluster.pipeline.embed import embed_items, _BATCH_SIZE, _VISION_MODEL
from fluster.pipeline.ingest import ingest_rows
from fluster.pipeline.materialize import materialize_items



def _write_csv(path, header, *rows):
    csv_file = path / "data.csv"
    csv_file.write_text("\n".join([header] + list(rows)) + "\n")
    return csv_file


def _ingest_materialize(pdir, conn, header, *rows):
    csv_file = _write_csv(pdir, header, *rows)
    ingest_rows(conn, csv_file, pdir)
    materialize_items(conn, pdir)


def _setup_single_item(pdir, conn):
    test_file = pdir / "doc.txt"
    test_file.write_text("Hello, world!")
    _ingest_materialize(pdir, conn, "name,file_path", f"greeting,{test_file}")


# --- Basic embedding ---

def test_embed_creates_embeddings(project):
    pdir, conn = project
    _setup_single_item(pdir, conn)
    plan = Plan()

    summary = embed_items(conn, plan)

    assert summary["embedded"] == 1
    assert summary["total"] == 1
    assert summary["model_name"] == plan.embedding.model_name
    assert summary["dimensions"] > 0


def test_embed_stores_vector_in_vec_table(project):
    pdir, conn = project
    _setup_single_item(pdir, conn)
    plan = Plan()

    embed_items(conn, plan)

    row = conn.execute(
        "SELECT embedding_id, vector FROM vec_embeddings"
    ).fetchone()
    assert row is not None

    # Vector should be a blob of floats matching the model dimensions.
    vector_bytes = row["vector"]
    assert len(vector_bytes) > 0
    float_count = len(vector_bytes) // 4  # float32
    assert float_count == 768  # nomic-embed-text-v1.5 produces 768-d vectors


def test_embed_links_to_representation(project):
    pdir, conn = project
    _setup_single_item(pdir, conn)
    plan = Plan()

    embed_items(conn, plan)

    emb = conn.execute("SELECT * FROM embeddings").fetchone()
    assert emb is not None
    assert emb["model_name"] == plan.embedding.model_name
    assert emb["dimensions"] > 0

    # Verify the FK link.
    rep = conn.execute(
        "SELECT * FROM representations WHERE representation_id = ?",
        (emb["representation_id"],),
    ).fetchone()
    assert rep is not None


# --- Idempotency ---

def test_embed_is_idempotent(project):
    pdir, conn = project
    _setup_single_item(pdir, conn)
    plan = Plan()

    embed_items(conn, plan)

    # Second call should embed nothing new.
    summary = embed_items(conn, plan)
    assert summary["embedded"] == 0

    count = conn.execute("SELECT COUNT(*) FROM embeddings").fetchone()[0]
    assert count == 1


# --- Multiple items ---

def test_embed_multiple_items(project):
    pdir, conn = project

    for name in ("a", "b", "c"):
        (pdir / f"{name}.txt").write_text(f"{name} content")

    _ingest_materialize(
        pdir, conn, "name,file_path",
        f"a,{pdir}/a.txt",
        f"b,{pdir}/b.txt",
        f"c,{pdir}/c.txt",
    )
    plan = Plan()

    summary = embed_items(conn, plan)
    assert summary["embedded"] == 3

    count = conn.execute("SELECT COUNT(*) FROM embeddings").fetchone()[0]
    assert count == 3

    vec_count = conn.execute("SELECT COUNT(*) FROM vec_embeddings").fetchone()[0]
    assert vec_count == 3


# --- Cancellation ---

def test_embed_respects_cancellation(project):
    pdir, conn = project

    # Create enough items to span multiple batches.
    names = [f"item{i}" for i in range(_BATCH_SIZE + 5)]
    for name in names:
        (pdir / f"{name}.txt").write_text(f"{name} content")

    rows = [f"{name},{pdir}/{name}.txt" for name in names]
    _ingest_materialize(pdir, conn, "name,file_path", *rows)

    plan = Plan()
    job_id = create_job(conn, "embed")

    # Request cancel before starting — should stop after first batch.
    request_cancel(conn, job_id)

    summary = embed_items(conn, plan, job_id=job_id)

    # Should have embedded only the first batch.
    assert summary["embedded"] == _BATCH_SIZE


# --- Progress tracking ---

def test_embed_updates_job_progress(project):
    pdir, conn = project
    _setup_single_item(pdir, conn)
    plan = Plan()
    job_id = create_job(conn, "embed")

    embed_items(conn, plan, job_id=job_id)

    import json
    job = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
    progress = json.loads(job["progress_json"])
    assert progress["embedded"] == 1
    assert progress["total"] == 1


# --- vec0 similarity search works ---

def test_vec_similarity_search(project):
    pdir, conn = project

    for name in ("cat", "dog"):
        (pdir / f"{name}.txt").write_text(f"A {name} is a pet animal.")

    _ingest_materialize(
        pdir, conn, "name,file_path",
        f"cat,{pdir}/cat.txt",
        f"dog,{pdir}/dog.txt",
    )
    plan = Plan()
    embed_items(conn, plan)

    # Get one vector to use as query.
    row = conn.execute(
        "SELECT vector FROM vec_embeddings LIMIT 1"
    ).fetchone()

    results = conn.execute(
        "SELECT embedding_id, distance FROM vec_embeddings "
        "WHERE vector MATCH ? ORDER BY distance LIMIT 2",
        (row["vector"],),
    ).fetchall()

    assert len(results) == 2
    # First result should be identical (distance ~0).
    assert results[0]["distance"] < 0.01


# --- Image embedding ---

def _create_test_image(path, width=4, height=4):
    img = Image.new("RGB", (width, height), color=(255, 0, 0))
    img.save(path, format="PNG")
    return path


def _mock_caption_model():
    model = MagicMock()
    model.caption.return_value = {"caption": "A test image"}
    return model


def _fake_embed_images(conn, image_reps, project_dir, dimensions):
    """Test stand-in for _embed_images that inserts fake 768D vectors."""
    import torch
    import torch.nn.functional as F

    torch.manual_seed(42)
    embedded = 0
    for rep in image_reps:
        vec = torch.randn(768)
        vec = F.normalize(vec, p=2, dim=0).numpy()
        cursor = conn.execute(
            "INSERT INTO embeddings (representation_id, model_name, dimensions) "
            "VALUES (?, ?, ?)",
            (rep["representation_id"], _VISION_MODEL, dimensions),
        )
        embedding_id = cursor.lastrowid
        conn.execute(
            "INSERT INTO vec_embeddings (embedding_id, vector) VALUES (?, ?)",
            (embedding_id, vec.tobytes()),
        )
        embedded += 1
    conn.commit()
    return embedded


def _setup_image_item(pdir, conn, monkeypatch):
    """Ingest and materialize a single image item with mocked captioning."""
    img_file = pdir / "photo.png"
    _create_test_image(img_file)

    mock_caption = _mock_caption_model()
    monkeypatch.setattr(materialize_mod, "_caption_model", mock_caption)

    csv_file = pdir / "data.csv"
    csv_file.write_text(f"name,file_path\nphoto,{img_file}\n")
    ingest_rows(conn, csv_file, pdir)
    materialize_items(conn, pdir)


def test_embed_image_uses_vision_model(project, monkeypatch):
    """Image items should be embedded with the vision model, not the text model."""
    pdir, conn = project
    _setup_image_item(pdir, conn, monkeypatch)

    monkeypatch.setattr(embed_mod, "_embed_images", _fake_embed_images)
    ensure_vec_table(conn, 768)

    plan = Plan()
    summary = embed_items(conn, plan, project_dir=pdir)

    assert summary["embedded"] == 1

    emb = conn.execute("SELECT * FROM embeddings").fetchone()
    assert emb["model_name"] == _VISION_MODEL
    assert emb["dimensions"] == 768


def test_embed_mixed_text_and_image(project, monkeypatch):
    """Mixed project: text items get text model, image items get vision model."""
    pdir, conn = project

    txt_file = pdir / "doc.txt"
    txt_file.write_text("Some text content")

    img_file = pdir / "photo.png"
    _create_test_image(img_file)

    mock_caption = _mock_caption_model()
    monkeypatch.setattr(materialize_mod, "_caption_model", mock_caption)

    csv_file = pdir / "data.csv"
    csv_file.write_text(f"name,file_path\ndoc,{txt_file}\nphoto,{img_file}\n")
    ingest_rows(conn, csv_file, pdir)
    materialize_items(conn, pdir)

    monkeypatch.setattr(embed_mod, "_embed_images", _fake_embed_images)

    plan = Plan()
    summary = embed_items(conn, plan, project_dir=pdir)

    assert summary["embedded"] == 2

    embs = conn.execute(
        "SELECT model_name FROM embeddings ORDER BY embedding_id"
    ).fetchall()
    model_names = {e["model_name"] for e in embs}
    assert plan.embedding.model_name in model_names
    assert _VISION_MODEL in model_names


def test_embed_image_idempotent(project, monkeypatch):
    """Image embeddings should not be re-created on second run."""
    pdir, conn = project
    _setup_image_item(pdir, conn, monkeypatch)

    monkeypatch.setattr(embed_mod, "_embed_images", _fake_embed_images)
    ensure_vec_table(conn, 768)

    plan = Plan()
    embed_items(conn, plan, project_dir=pdir)

    # Second run: no image reps should be found
    summary = embed_items(conn, plan, project_dir=pdir)
    assert summary["embedded"] == 0

    count = conn.execute("SELECT COUNT(*) FROM embeddings").fetchone()[0]
    assert count == 1


def test_embed_no_project_dir_skips_images(project, monkeypatch):
    """Without project_dir, image items should be silently skipped."""
    pdir, conn = project
    _setup_image_item(pdir, conn, monkeypatch)

    plan = Plan()
    summary = embed_items(conn, plan)

    # No embeddings created — image items need project_dir
    assert summary["embedded"] == 0
