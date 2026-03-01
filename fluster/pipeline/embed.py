"""embed_items — batch-embed representations using sentence-transformers."""

import sqlite3

from sentence_transformers import SentenceTransformer

from fluster.config.plan import Plan
from fluster.db.schema import ensure_vec_table
from fluster.jobs.manager import is_cancel_requested, update_progress

_BATCH_SIZE = 64


def embed_items(
    conn: sqlite3.Connection,
    plan: Plan,
    job_id: int | None = None,
) -> dict:
    """Embed all representations that don't yet have an embedding.

    Loads the model specified in plan.embedding, encodes in batches,
    stores vectors in both the embeddings table and vec_embeddings virtual table.
    Checks for cancellation between batches when job_id is provided.

    Returns a summary dict with counts.
    """
    model_name = plan.embedding.model_name
    model = SentenceTransformer(model_name)
    dimensions = model.get_sentence_embedding_dimension()

    ensure_vec_table(conn, dimensions)

    # Find representations that don't yet have an embedding for this model.
    reps = conn.execute(
        """
        SELECT r.representation_id, r.text
        FROM representations r
        WHERE r.representation_type = 'embedding_text'
          AND r.representation_id NOT IN (
              SELECT representation_id FROM embeddings
              WHERE model_name = ?
          )
        ORDER BY r.representation_id
        """,
        (model_name,),
    ).fetchall()

    embedded = 0
    total = len(reps)

    for batch_start in range(0, total, _BATCH_SIZE):
        batch = reps[batch_start : batch_start + _BATCH_SIZE]
        texts = [r["text"] for r in batch]
        vectors = model.encode(texts, normalize_embeddings=True)

        for rep, vector in zip(batch, vectors):
            cursor = conn.execute(
                "INSERT INTO embeddings (representation_id, model_name, dimensions) "
                "VALUES (?, ?, ?)",
                (rep["representation_id"], model_name, dimensions),
            )
            embedding_id = cursor.lastrowid

            conn.execute(
                "INSERT INTO vec_embeddings (embedding_id, vector) VALUES (?, ?)",
                (embedding_id, vector.tobytes()),
            )
            embedded += 1

        conn.commit()

        if job_id is not None:
            update_progress(conn, job_id, {
                "embedded": embedded,
                "total": total,
            })

        # Check cancellation between batches.
        if job_id is not None and is_cancel_requested(conn, job_id):
            break

    return {
        "embedded": embedded,
        "total": total,
        "model_name": model_name,
        "dimensions": dimensions,
    }
