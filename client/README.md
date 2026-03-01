# fluster client

SvelteKit visualization layer for fluster. Read-only access to `project.db`.

## Environment

The client expects two env vars, set automatically by `fluster show` or `fluster chill`:

| Variable | Purpose |
|----------|---------|
| `FLUSTER_DB_PATH` | Absolute path to `project.db` |
| `FLUSTER_PROJECT_NAME` | Project name for the UI banner |

The dev server will refuse to start if either is missing.

## Development

```bash
npm install
npm run dev   # needs FLUSTER_DB_PATH and FLUSTER_PROJECT_NAME
npm run check # TypeScript
npm run build # production build (no env vars needed)
```

## Regenerating the database schema

The Drizzle schema (`src/lib/server/db/schema.ts`) is generated from a real SQLite database via `drizzle-kit pull`. If the backend schema changes, regenerate it:

```bash
# 1. Create a fresh database with the current backend schema
cd /path/to/fluster
uv run python -c "
from pathlib import Path
from fluster.db.connection import connect
p = Path('/tmp/fluster-pull')
p.mkdir(exist_ok=True)
(p / 'artifacts').mkdir(exist_ok=True)
conn = connect(p)
conn.close()
"

# 2. Pull the schema into the client
cd client
FLUSTER_DB_PATH=/tmp/fluster-pull/project.db npm run db:pull

# 3. Clean up migration artifacts (we only want schema.ts and relations.ts)
rm -rf src/lib/server/db/meta src/lib/server/db/*.sql

# 4. Review the generated schema — drizzle-kit may produce broken CHECK
#    constraints. Since the client is read-only, strip them if needed.
```

The generated `schema.ts` and `relations.ts` are committed to the repo.
