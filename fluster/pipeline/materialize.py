"""materialize_items — extract text from artifacts and build embedding_text."""

import hashlib
import json
import sqlite3
from pathlib import Path

from loguru import logger
from tqdm import tqdm

_caption_model = None


def _is_image(mime_type: str | None) -> bool:
    return mime_type is not None and mime_type.startswith("image/")


def _load_caption_model():
    """Lazy-load moondream2 for image captioning. Cached after first call."""
    global _caption_model
    if _caption_model is not None:
        return _caption_model

    import warnings

    import torch
    import transformers
    from transformers import AutoModelForCausalLM, PreTrainedModel

    logger.info("Loading moondream2 caption model...")

    # Workaround: moondream2's HfMoondream.__init__ doesn't call post_init(),
    # but transformers >=5.1.0 expects all_tied_weights_keys to exist during
    # from_pretrained(). Temporarily patch PreTrainedModel.__init__ to set
    # the missing attribute so from_pretrained() doesn't crash.
    _orig_init = PreTrainedModel.__init__

    def _patched_init(self, *args, **kwargs):
        _orig_init(self, *args, **kwargs)
        if not hasattr(self, "all_tied_weights_keys"):
            self.all_tied_weights_keys = {}

    PreTrainedModel.__init__ = _patched_init

    prev_verbosity = transformers.logging.get_verbosity()
    transformers.logging.set_verbosity_error()
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="`torch_dtype` is deprecated")
            warnings.filterwarnings("ignore", message=".*unauthenticated.*")

            # moondream2 is incompatible with MPS (NaN, mixed-dtype crashes).
            # Use CUDA when available, otherwise CPU.
            if torch.cuda.is_available():
                device = "cuda"
                load_kwargs = {"dtype": torch.float16}
            else:
                device = "cpu"
                load_kwargs = {"dtype": torch.float32}

            _caption_model = AutoModelForCausalLM.from_pretrained(
                "vikhyatk/moondream2",
                revision="2025-06-21",
                trust_remote_code=True,
                **load_kwargs,
            ).to(device)
    finally:
        PreTrainedModel.__init__ = _orig_init
        transformers.logging.set_verbosity(prev_verbosity)

    logger.info(f"moondream2 loaded on {_caption_model.device} ({_caption_model.dtype})")
    _caption_model.eval()
    return _caption_model


def _caption_image(stored_path: str, project_dir: Path, model) -> str:
    """Generate a text caption for an image using moondream2."""
    from PIL import Image

    full_path = project_dir / "artifacts" / stored_path
    try:
        img = Image.open(full_path).convert("RGB")
        result = model.caption(img, length="normal")
        # moondream2 returns a dict with "caption" key or a string
        if isinstance(result, dict):
            return result.get("caption", "")
        return str(result)
    except Exception as e:
        logger.warning(f"Failed to caption image {full_path}: {e}")
        return ""


def _extract_text(stored_path: str, project_dir: Path) -> str:
    """Extract text content from a stored artifact. Text files only."""
    full_path = project_dir / "artifacts" / stored_path
    try:
        return full_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        logger.warning(f"Artifact not found: {full_path}")
        return ""
    except UnicodeDecodeError:
        return ""


def _build_embedding_text(
    row_name: str | None,
    metadata: dict,
    extracted_text: str,
) -> str:
    """Assemble the embedding_text from available components."""
    parts = []
    if row_name:
        parts.append(row_name)
    if metadata:
        meta_str = ", ".join(f"{k}: {v}" for k, v in metadata.items() if v != "")
        if meta_str:
            parts.append(meta_str)
    if extracted_text:
        parts.append(extracted_text)
    return "\n\n".join(parts)


def _text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def materialize_items(
    conn: sqlite3.Connection,
    project_dir: Path,
) -> dict:
    """Build embedding_text representations for all items that lack one.

    Returns a summary dict with counts.
    """
    # Find items that don't yet have an embedding_text representation.
    items = conn.execute("""
        SELECT i.item_id, r.row_name, r.row_metadata_json
        FROM items i
        JOIN rows r ON i.row_id = r.row_id
        WHERE i.item_id NOT IN (
            SELECT item_id FROM representations
            WHERE representation_type = 'embedding_text'
        )
        ORDER BY i.item_id
    """).fetchall()

    materialized = 0
    skipped = 0

    for item in tqdm(items, desc="Materializing", unit="item", disable=len(items) == 0):
        item_id = item["item_id"]
        row_name = item["row_name"]
        metadata = json.loads(item["row_metadata_json"])

        # Get all artifacts for this item.
        artifacts = conn.execute(
            "SELECT a.stored_path, a.mime_type FROM item_artifacts ia "
            "JOIN artifacts a ON ia.artifact_id = a.artifact_id "
            "WHERE ia.item_id = ?",
            (item_id,),
        ).fetchall()

        # Extract text (or caption) from all artifacts.
        extracted_parts = []
        for artifact in artifacts:
            if _is_image(artifact["mime_type"]):
                model = _load_caption_model()
                text = _caption_image(artifact["stored_path"], project_dir, model)
            else:
                text = _extract_text(artifact["stored_path"], project_dir)
            if text.strip():
                extracted_parts.append(text.strip())

        extracted_text = "\n\n".join(extracted_parts)

        embedding_text = _build_embedding_text(row_name, metadata, extracted_text)

        if not embedding_text.strip():
            skipped += 1
            continue

        conn.execute(
            "INSERT INTO representations "
            "(item_id, representation_type, text, text_hash) "
            "VALUES (?, 'embedding_text', ?, ?)",
            (item_id, embedding_text, _text_hash(embedding_text)),
        )
        materialized += 1

    conn.commit()

    return {
        "materialized": materialized,
        "skipped": skipped,
    }
