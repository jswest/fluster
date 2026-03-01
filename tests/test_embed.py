"""Tests for embed_items (Phase 8)."""

import pytest

from fluster.config.plan import Plan
from fluster.db.connection import connect
from fluster.jobs.manager import create_job, request_cancel
from fluster.pipeline.embed import embed_items, _BATCH_SIZE
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
    assert summary["model_name"] == "all-MiniLM-L6-v2"
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
    assert float_count == 384  # all-MiniLM-L6-v2 produces 384-d vectors


def test_embed_links_to_representation(project):
    pdir, conn = project
    _setup_single_item(pdir, conn)
    plan = Plan()

    embed_items(conn, plan)

    emb = conn.execute("SELECT * FROM embeddings").fetchone()
    assert emb is not None
    assert emb["model_name"] == "all-MiniLM-L6-v2"
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
