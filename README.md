# `fluster`

**fluster clusters for the confused.**

`fluster` is a local-first, reproducible clustering engine for real-world messes. Give it a CSV, and it will:

- Materialize rows into structured items
- Extract and normalize text
- Generate embeddings
- Reduce dimensions (PCA + UMAP)
- Cluster (HDBSCAN or agglomerative)
- Select representative exemplars per cluster
- Label clusters with an LLM (BYO)
- Critique the clustering run
- Keep an auditable trail of every step

Everything is stored locally, and every stage is versioned. You and your data won't be a Flustered Frederick but instead a Clustered Cathy.

---

## Why `fluster`?

Real-world datasets are often a mixture of:

- Texts (up to ~8k tokens)
- PDFs (not supported yet)
- Random images (supported)
- Metadata columns (supported)

`fluster` turns that confusion into structure--reproducibly, and with a complete provenance trail.

It is:
- **Local-first** — runs entirely on your machine if you want it to.
- **Append-only** — no destructive pipeline steps.
- **Reproducible** — seed-fixed (42, because IFKYK).
- **LLM-assisted, not LLM-dependent** — BYO model, or don't bring anything at all.
- **Auditable** — we've got jobs, logs, and cluster runs all persisted

---

## Status

v0 (CLI + pipeline) and v1 (SvelteKit visualization) are complete.

---

## Core Philosophy

> Structure and cluster locally first, with receipts.

Every embedding, reduction, cluster run, label, and critique is stored in the project database. Nothing is overwritten. You can always trace how a cluster came to be.

---

## Quick Start

```bash
uv tool install -e .
npm --prefix client install

fluster init my-project
fluster ingest-rows data.csv
fluster run
fluster chill
```

---

## Demo

Run an end-to-end demo using the `demo/` folder dataset generator.

```bash
# from the repo root
uv sync
uv tool install -e .
npm --prefix client install

# create the demo CSV (not committed)
uv run python demo/fetch_newsgroups.py

# create + select a project
fluster init demo-20ng

# set provider credentials if needed (OpenAI by default)
fluster config

# ingest + run full pipeline
fluster ingest-rows demo/twenty_newsgroups_1k.csv
fluster run

# open the UI
fluster chill
```

Then open `http://localhost:3000`.

---

## Workflow

```bash
# Create a project — this becomes the active project automatically
fluster init my-project

# Set up your API key (stored in ~/.fluster/secrets.yaml)
fluster config

# Tweak the plan if you want (embedding model, LLM, clustering params)
fluster plan

# Ingest a CSV — file_path column is optional
fluster ingest-rows data.csv

# Run the full pipeline (materialize → embed → reduce → cluster → exemplars → label → critique)
fluster run

# Check on jobs and logs
fluster jobs
fluster job 1
fluster logs

# Export results
fluster export --cluster-run 1 -o results.csv

# Launch the visualization UI
fluster chill

# Cancel a stuck job (--force for orphaned jobs)
fluster cancel 1 --force

# Switch between projects
fluster list
fluster use other-project
```

---

## CSV Format

Your CSV needs at least one column of text content. Two column names are special:

| Column | Required? | What it does |
|--------|-----------|--------------|
| `name` | No | A human-readable label for the row (shown in the UI) |
| `file_path` | No | Absolute path to a file to attach as an artifact |

Everything else becomes searchable metadata.

### Images

If `file_path` points to an image (`.jpg`, `.png`, etc.), fluster will:

1. **Caption it** locally with [Florence-2](https://huggingface.co/florence-community/Florence-2-base) — the caption becomes the row's searchable text
2. **Embed it** with [nomic-embed-vision-v1.5](https://huggingface.co/nomic-ai/nomic-embed-vision-v1.5) — same 768D space as text, so images and text cluster together naturally
3. **Display it** in the UI — thumbnails on hover, full preview in the detail drawer

No external services needed. Both models run locally via `transformers`.

Example:

```csv
name,file_path,source
cat lounging,/data/photos/cat.jpg,flickr
meeting notes,/data/docs/notes.txt,internal
dog at park,/data/photos/dog.png,flickr
```

---

## Development

`fluster` uses [uv](https://docs.astral.sh/uv/) for package management. No virtualenv dance required.

```bash
# Install dependencies (including dev)
uv sync

# Run the full test suite
uv run pytest

# Run a specific test file
uv run pytest tests/test_server.py -v

# Run tests matching a keyword
uv run pytest -k "cluster" -v
```

### Project layout

```
fluster/
├── cli.py              # Typer CLI (init, use, list, config, plan, ingest-rows, run, ...)
├── server.py           # FastAPI server (create_app factory)
├── config/             # Plan YAML schema, project layout, settings
├── db/                 # SQLite connection + schema
├── jobs/               # Job lifecycle management
├── llm/                # LLM interface (OpenAI, Ollama)
└── pipeline/           # The pipeline stages (ingest → critique)
```

### Starting the dev server

```bash
fluster serve --port 8000
```

### Database

Everything lives in `~/.fluster/projects/<name>/project.db` — a single SQLite file with WAL mode, JSON1, and sqlite-vec loaded. You can inspect it directly:

```bash
sqlite3 ~/.fluster/projects/my-project/project.db ".tables"
```
