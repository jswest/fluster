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

v0: CLI + FastAPI backend (`uv`, baybee!)
v1: SvelteKit visualization layer (planned)

### What's working (v0)

| Phase | What | Status |
|-------|------|--------|
| 0 | Repo skeleton + dependencies | done |
| 1 | Workspace + project layout | done |
| 2 | SQLite bootstrap (WAL, JSON1, sqlite-vec) | done |
| 3 | Core tables (rows, items, artifacts, representations) | done |
| 4 | Jobs + job_logs | done |
| 5 | Plan YAML schema (Pydantic) | done |
| 6 | `ingest_rows` — CSV to rows + items + artifacts | done |
| 7 | `materialize_items` — extract text, build embedding_text | done |
| 8 | `embed_items` — sentence-transformers + vec0 storage | done |
| 9 | `reduce_items` — PCA + UMAP (2D & 8D) | done |
| 10 | `cluster_items` — HDBSCAN on UMAP-8D | done |
| 11 | Exemplars — faux-medoid selection per cluster | done |
| 12 | LLM interface — `generate_json` with OpenAI/Ollama | done |
| 13 | `label_clusters` | done |
| 14 | `critique_clusters` | done |
| 15 | FastAPI server | done |
| 16 | CLI orchestration (`fluster run`) | done |
| 17 | Export | done |

175 tests. All passing.

---

## Core Philosophy

> Structure and cluster locally first, with receipts.

Every embedding, reduction, cluster run, label, and critique is stored in the project database. Nothing is overwritten. You can always trace how a cluster came to be.

---

## Quick Start (v0)

```bash
fluster init project-foo
fluster ingest-rows project-foo data.csv
fluster run project-foo
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
fluster serve my-project --port 8000
```

### Database

Everything lives in `~/.fluster/projects/<name>/project.db` — a single SQLite file with WAL mode, JSON1, and sqlite-vec loaded. You can inspect it directly:

```bash
sqlite3 ~/.fluster/projects/my-project/project.db ".tables"
```