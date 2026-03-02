"""materialize_items — extract text from artifacts and build embedding_text."""

import hashlib
import json
import sqlite3
from pathlib import Path

from loguru import logger
from tqdm import tqdm

_caption_cache = None


def _is_image(mime_type: str | None) -> bool:
    return mime_type is not None and mime_type.startswith("image/")


_IMAGE_TOKEN_INDEX = -200


def _load_caption_model():
    """Lazy-load FastVLM-0.5B for image captioning. Cached after first call."""
    global _caption_cache
    if _caption_cache is not None:
        return _caption_cache

    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer

    logger.info("Loading FastVLM-0.5B caption model...")

    if torch.backends.mps.is_available():
        device = torch.device("mps")
    elif torch.cuda.is_available():
        device = torch.device("cuda")
    else:
        device = torch.device("cpu")

    dtype = torch.float16 if device.type == "cuda" else torch.float32

    model = AutoModelForCausalLM.from_pretrained(
        "apple/FastVLM-0.5B",
        torch_dtype=dtype,
        trust_remote_code=True,
    ).to(device)
    model.eval()
    tokenizer = AutoTokenizer.from_pretrained(
        "apple/FastVLM-0.5B", trust_remote_code=True,
    )

    _caption_cache = (model, tokenizer, device)
    logger.info(f"FastVLM loaded on {device} ({dtype})")
    return _caption_cache


def _caption_image(stored_path: str, project_dir: Path, model, tokenizer, device) -> str:
    """Generate a text caption for an image using FastVLM."""
    import torch
    from PIL import Image

    full_path = project_dir / "artifacts" / stored_path
    try:
        img = Image.open(full_path).convert("RGB")

        messages = [
            {"role": "user", "content": "<image>\nDescribe this image in one sentence."},
        ]
        rendered = tokenizer.apply_chat_template(
            messages, add_generation_prompt=True, tokenize=False,
        )
        pre, post = rendered.split("<image>", 1)
        pre_ids = tokenizer(pre, return_tensors="pt", add_special_tokens=False).input_ids
        post_ids = tokenizer(post, return_tensors="pt", add_special_tokens=False).input_ids
        img_tok = torch.tensor([[_IMAGE_TOKEN_INDEX]], dtype=pre_ids.dtype)
        input_ids = torch.cat([pre_ids, img_tok, post_ids], dim=1).to(device)
        attention_mask = torch.ones_like(input_ids, device=device)

        px = model.get_vision_tower().image_processor(
            images=img, return_tensors="pt",
        )["pixel_values"].to(device, dtype=model.dtype)

        with torch.no_grad():
            output = model.generate(
                inputs=input_ids,
                attention_mask=attention_mask,
                images=px,
                max_new_tokens=100,
            )
        return tokenizer.decode(output[0], skip_special_tokens=True).strip()
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
    caption_images: bool = True,
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
    batch_size = 50
    uncommitted = 0

    for item in tqdm(items, desc="Materializing", unit="item", disable=len(items) == 0):
        item_id = item["item_id"]
        row_name = item["row_name"]
        metadata = json.loads(item["row_metadata_json"])

        # Get all artifacts for this item.
        artifacts = conn.execute(
            "SELECT a.stored_path, a.original_path, a.mime_type FROM item_artifacts ia "
            "JOIN artifacts a ON ia.artifact_id = a.artifact_id "
            "WHERE ia.item_id = ?",
            (item_id,),
        ).fetchall()

        # Extract text (or caption) from all artifacts.
        extracted_parts = []
        for artifact in artifacts:
            if _is_image(artifact["mime_type"]):
                if caption_images:
                    model, tokenizer, device = _load_caption_model()
                    text = _caption_image(artifact["stored_path"], project_dir, model, tokenizer, device)
                else:
                    text = ""
            else:
                text = _extract_text(artifact["stored_path"], project_dir)
            if text.strip():
                extracted_parts.append(text.strip())

        extracted_text = "\n\n".join(extracted_parts)

        embedding_text = _build_embedding_text(row_name, metadata, extracted_text)

        if not embedding_text.strip():
            # Use artifact filename as fallback so item appears in scatter plot
            if artifacts:
                embedding_text = Path(artifacts[0]["original_path"]).stem
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
        uncommitted += 1

        if uncommitted >= batch_size:
            conn.commit()
            uncommitted = 0

    if uncommitted:
        conn.commit()

    return {
        "materialized": materialized,
        "skipped": skipped,
    }
