"""embed_items — batch-embed representations using sentence-transformers."""

import logging
import os
import sqlite3
import warnings
from pathlib import Path

os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
warnings.filterwarnings("ignore", message=".*unauthenticated.*")
logging.getLogger("sentence_transformers").setLevel(logging.WARNING)
logging.getLogger("huggingface_hub").setLevel(logging.WARNING)
logging.getLogger("transformers").setLevel(logging.WARNING)

from loguru import logger
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

from fluster.config.plan import Plan
from fluster.db.schema import ensure_vec_table
from fluster.jobs.manager import is_cancel_requested, update_progress

_BATCH_SIZE = 8
_VISION_MODEL = "nomic-ai/nomic-embed-vision-v1.5"


def _get_text_reps(conn: sqlite3.Connection, text_model_name: str) -> list:
    """Get non-image representations that haven't been embedded by the text model."""
    return conn.execute(
        """
        SELECT r.representation_id, r.text
        FROM representations r
        WHERE r.representation_type = 'embedding_text'
          AND r.item_id NOT IN (
              SELECT DISTINCT ia.item_id
              FROM item_artifacts ia
              JOIN artifacts a ON ia.artifact_id = a.artifact_id
              WHERE a.mime_type LIKE 'image/%'
          )
          AND r.representation_id NOT IN (
              SELECT representation_id FROM embeddings
              WHERE model_name = ?
          )
        ORDER BY r.representation_id
        """,
        (text_model_name,),
    ).fetchall()


def _get_image_reps(conn: sqlite3.Connection, vision_model_name: str) -> list:
    """Get image representations that haven't been embedded by the vision model."""
    return conn.execute(
        """
        SELECT r.representation_id, r.item_id, MIN(a.stored_path) AS stored_path
        FROM representations r
        JOIN item_artifacts ia ON ia.item_id = r.item_id
        JOIN artifacts a ON ia.artifact_id = a.artifact_id
        WHERE r.representation_type = 'embedding_text'
          AND a.mime_type LIKE 'image/%'
          AND r.representation_id NOT IN (
              SELECT representation_id FROM embeddings
              WHERE model_name = ?
          )
        GROUP BY r.representation_id
        ORDER BY r.representation_id
        """,
        (vision_model_name,),
    ).fetchall()


def _embed_images(
    conn: sqlite3.Connection,
    image_reps: list,
    project_dir: Path,
    dimensions: int,
) -> int:
    """Embed image items using nomic-embed-vision-v1.5.

    Returns the number of items embedded.
    """
    import torch
    import torch.nn.functional as F
    from PIL import Image
    from transformers import AutoImageProcessor, AutoModel

    logger.info("Loading nomic-embed-vision-v1.5 for image embedding...")
    processor = AutoImageProcessor.from_pretrained(_VISION_MODEL)
    vision_model = AutoModel.from_pretrained(_VISION_MODEL, trust_remote_code=True)
    # Workaround: transformers >=5.1.0 expects all_tied_weights_keys but
    # trust_remote_code models may skip post_init() where it's set.
    if not hasattr(vision_model, "all_tied_weights_keys"):
        vision_model.all_tied_weights_keys = {}
    vision_model.eval()

    embedded = 0
    batch_iter = tqdm(
        range(0, len(image_reps), _BATCH_SIZE),
        desc="Embedding images",
        unit="batch",
        disable=len(image_reps) == 0,
    )

    for batch_start in batch_iter:
        batch = image_reps[batch_start : batch_start + _BATCH_SIZE]

        images = []
        for rep in batch:
            img_path = project_dir / "artifacts" / rep["stored_path"]
            img = Image.open(img_path).convert("RGB")
            images.append(img)

        inputs = processor(images, return_tensors="pt")
        with torch.no_grad():
            outputs = vision_model(**inputs)

        vectors = F.normalize(outputs.last_hidden_state[:, 0], p=2, dim=1)

        for rep, vector in zip(batch, vectors):
            vec_np = vector.cpu().numpy()
            cursor = conn.execute(
                "INSERT INTO embeddings (representation_id, model_name, dimensions) "
                "VALUES (?, ?, ?)",
                (rep["representation_id"], _VISION_MODEL, dimensions),
            )
            embedding_id = cursor.lastrowid
            conn.execute(
                "INSERT INTO vec_embeddings (embedding_id, vector) VALUES (?, ?)",
                (embedding_id, vec_np.tobytes()),
            )
            embedded += 1

        conn.commit()

    del vision_model, processor
    return embedded


def embed_items(
    conn: sqlite3.Connection,
    plan: Plan,
    project_dir: Path | None = None,
    job_id: int | None = None,
) -> dict:
    """Embed all representations that don't yet have an embedding.

    Text items are embedded with the model specified in plan.embedding.
    Image items are embedded with nomic-embed-vision-v1.5 (same 768D space).
    Checks for cancellation between batches when job_id is provided.

    Returns a summary dict with counts.
    """
    text_model_name = plan.embedding.model_name
    max_tokens = plan.embedding.max_tokens

    text_reps = _get_text_reps(conn, text_model_name)
    image_reps = _get_image_reps(conn, _VISION_MODEL) if project_dir else []

    total = len(text_reps) + len(image_reps)
    embedded = 0
    dimensions = 0

    # --- Text pass ---
    if text_reps:
        model = SentenceTransformer(text_model_name, trust_remote_code=True)
        tokenizer = model.tokenizer
        dimensions = model.get_sentence_embedding_dimension()
        ensure_vec_table(conn, dimensions)

        batch_iter = tqdm(
            range(0, len(text_reps), _BATCH_SIZE),
            desc="Embedding text",
            unit="batch",
            disable=len(text_reps) == 0,
        )

        for batch_start in batch_iter:
            batch = text_reps[batch_start : batch_start + _BATCH_SIZE]
            prefix = plan.embedding.task_prefix
            prefix_len = len(tokenizer.encode(prefix, add_special_tokens=False))
            text_budget = max_tokens - prefix_len

            texts = []
            for r in batch:
                text = r["text"]
                tokens = tokenizer.encode(text, add_special_tokens=False)
                if len(tokens) > text_budget:
                    text = tokenizer.decode(tokens[:text_budget])
                texts.append(prefix + text)
            vectors = model.encode(texts, normalize_embeddings=True)

            for rep, vector in zip(batch, vectors):
                cursor = conn.execute(
                    "INSERT INTO embeddings (representation_id, model_name, dimensions) "
                    "VALUES (?, ?, ?)",
                    (rep["representation_id"], text_model_name, dimensions),
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

            if job_id is not None and is_cancel_requested(conn, job_id):
                break

        del model

    # --- Image pass ---
    if image_reps and project_dir is not None:
        if not dimensions:
            # No text items were embedded; ensure vec table with 768D (nomic vision output).
            dimensions = 768
            ensure_vec_table(conn, dimensions)

        embedded += _embed_images(conn, image_reps, project_dir, dimensions)

        if job_id is not None:
            update_progress(conn, job_id, {
                "embedded": embedded,
                "total": total,
            })

    return {
        "embedded": embedded,
        "total": total,
        "model_name": text_model_name,
        "dimensions": dimensions,
    }
