# `fluster`

**fluster clusters for the confused.**

`fluster` is a local-first, reproducible clustering engine for real-world messes. Give it a CSV, and it will:

- Materialize rows into structured items
- Extract and normalize text
- Generate embeddings
- Reduce dimensions (PCA + UMAP)
- Cluster (HDBSCAN by default)
- Label clusters with an LLM (BYO)
- Critique the clustering run
- Keep an auditable trail of every step

Everything is stored locally, and every stage is versioned. You and your data won't be a Flustered Frederick but instead a Clustered Cathy.

---

## Why `fluster`?

Real-world datasets are often a mixture of:

- Short texts
- PDFs
- Random images
- Metadata columns

`fluster` turns that confusion into structure--reproducibly, and with a complete provenance trail.

It is:
- **Local-first** — runs entirely on your machine if you want it to.
- **Append-only** — no destructive pipeline steps.
- **Reproducible** — seed-fixed (42, because IFKYK).
- **LLM-assisted, not LLM-dependent** — BYO model, or don't bring anything at all.
- **Auditable** — we've got jobs, logs, and cluster runs all persisted

---

## Status

v0 (CLI + pipeline) is complete. v1 (SvelteKit visualization) is in progress.

---

## Core Philosophy

> Structure and cluster locally first, with receipts.

Every embedding, reduction, cluster run, label, and critique is stored in the project database. Nothing is overwritten. You can always trace how a cluster came to be.

---

## Quick Start

```bash
uv tool install -e .

fluster init my-project
fluster ingest-rows data.csv
fluster run
fluster chill
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
├── cli.py              # Typer CLI (init, ingest-rows, run, export, jobs, serve)
├── server.py           # FastAPI server (create_app factory)
├── config/             # Plan YAML schema, project layout, settings
├── db/                 # SQLite connection + schema
├── jobs/               # Job lifecycle management
├── llm/                # LLM interface (OpenAI, Ollama)
├── pipeline/           # The pipeline stages (ingest → critique)
└── util/               # Shared utilities
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