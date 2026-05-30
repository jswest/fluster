"""fluster CLI — powered by Typer."""

import json
import os
import subprocess
from contextlib import contextmanager
from pathlib import Path

import typer
from loguru import logger
from rich.console import Console
from rich.table import Table

from fluster import __version__
from fluster.config import settings
from fluster.config.plan import load_plan, save_plan, Plan, LLMProvider, UMAPReduction
from fluster.config.project import (
    create_project,
    delete_project,
    get_active_project,
    list_projects,
    project_dir,
    project_exists,
    set_active_project,
)
from fluster.db.connection import connect
from fluster.jobs.manager import (
    create_job,
    fail_job,
    get_active_job,
    get_job,
    get_job_logs,
    get_recent_logs,
    list_jobs,
    mark_canceled,
    request_cancel,
    start_job,
    succeed_job,
)
from fluster.pipeline.export import export_cluster_run
from fluster.pipeline.ingest import ingest_rows
from fluster.pipeline.merge import merge_clusters
from fluster.pipeline.run import PipelineCancelled, run_pipeline

console = Console()


def _resolve_project() -> str:
    """Return the active project name or exit with a helpful error."""
    name = get_active_project()
    if not name:
        logger.error("No active project. Run 'fluster use <name>' first.")
        raise typer.Exit(code=1)
    return name


@contextmanager
def _open_project():
    """Resolve the active project, validate it exists, yield (project_path, connection).

    Closes the connection on exit.
    """
    project_name = _resolve_project()
    if not project_exists(project_name):
        logger.error(f"Project '{project_name}' does not exist.")
        raise typer.Exit(code=1)
    project_path = project_dir(project_name)
    conn = connect(project_path)
    try:
        yield project_path, conn
    finally:
        conn.close()


app = typer.Typer(
    name="fluster",
    help="Clustering for confused data. Structure, with receipts.",
    no_args_is_help=True,
)


def _version_callback(value: bool):
    if value:
        print(f"fluster {__version__}")
        raise typer.Exit()


@app.callback()
def _main(
    version: bool | None = typer.Option(
        None, "--version", "-v", callback=_version_callback, is_eager=True,
        help="Show version and exit.",
    ),
):
    """Clustering for confused data. Structure, with receipts."""


@app.command()
def init(project_name: str = typer.Argument(help="Name for the new project.")):
    """Create a new fluster project."""
    try:
        project_path = create_project(project_name)
    except FileExistsError:
        logger.error(f"Project '{project_name}' already exists.")
        raise typer.Exit(code=1)
    set_active_project(project_name)
    logger.info(f"Created and now using project '{project_name}' at {project_path}")


@app.command()
def use(project_name: str = typer.Argument(help="Project to switch to.")):
    """Set the active project."""
    if not project_exists(project_name):
        logger.error(f"Project '{project_name}' does not exist.")
        raise typer.Exit(code=1)
    set_active_project(project_name)
    logger.info(f"Now using project '{project_name}'")


@app.command(name="list")
def list_cmd():
    """List all projects."""
    projects = list_projects()
    if not projects:
        console.print("[dim]No projects found.[/dim]")
        return
    active = get_active_project()
    for name in projects:
        marker = " *" if name == active else ""
        console.print(f"  {name}{marker}")


@app.command()
def delete(project_name: str = typer.Argument(help="Project to delete.")):
    """Permanently delete a project and all its data."""
    if not project_exists(project_name):
        logger.error(f"Project '{project_name}' does not exist.")
        raise typer.Exit(code=1)
    typer.confirm(f"Delete project '{project_name}'? This cannot be undone", abort=True)
    delete_project(project_name)
    logger.info(f"Deleted project '{project_name}'")


@app.command()
def config():
    """Set up API keys for LLM providers."""
    import yaml

    secrets = {}
    if settings.SECRETS_FILE.is_file():
        secrets = yaml.safe_load(settings.SECRETS_FILE.read_text()) or {}

    provider = typer.prompt(
        "LLM provider", default="openai", type=str,
    ).strip().lower()

    if provider == "openai":
        current = secrets.get("openai_api_key", "")
        hint = f" (current: ...{current[-4:]})" if current else ""
        key = typer.prompt(
            f"OpenAI API key{hint}", default=current, hide_input=True,
        ).strip()
        if key:
            secrets["openai_api_key"] = key
    elif provider == "ollama":
        console.print("Ollama runs locally — no API key needed.")
        return
    else:
        console.print(f"Unknown provider '{provider}'. No key saved.")
        return

    settings.FLUSTER_HOME.mkdir(parents=True, exist_ok=True)
    settings.SECRETS_FILE.write_text(
        yaml.dump(secrets, default_flow_style=False)
    )
    console.print(f"Saved to {settings.SECRETS_FILE}")


@app.command(name="plan")
def plan_cmd():
    """Interactively edit the clustering plan for the active project."""
    with _open_project() as (project_path, _conn):
        plan_path = project_path / settings.PLAN_YAML
        plan = load_plan(plan_path)

        provider_str = typer.prompt(
            "LLM provider (openai/ollama)", default=plan.llm.provider.value,
        ).strip().lower()
        plan.llm.provider = LLMProvider(provider_str)

        plan.llm.model = typer.prompt(
            "LLM model", default=plan.llm.model,
        )

        cluster = plan.clustering[0]

        method_choice = typer.prompt(
            "Clustering method (hdbscan/agglomerative)", default=cluster.method,
        ).strip().lower()
        if method_choice not in ("hdbscan", "agglomerative"):
            console.print(f"[red]Invalid method '{method_choice}'.[/red]")
            raise typer.Exit(code=1)
        if method_choice == "hdbscan":
            # Clear any previously set agglomerative parameters (only clear old non-HDBSCAN params)
            old_params = cluster.params
            cluster.params = {}
            
            min_cluster = old_params.get("min_cluster_size", 5)
            min_cluster = typer.prompt(
                "HDBSCAN min_cluster_size", default=min_cluster, type=int,
            )
            cluster.params["min_cluster_size"] = min_cluster

            min_samples = old_params.get("min_samples", min_cluster)
            min_samples = typer.prompt(
                "HDBSCAN min_samples", default=min_samples, type=int,
            )
            if min_samples < 1:
                console.print("[red]min_samples must be >= 1.[/red]")
                raise typer.Exit(code=1)
            cluster.params["min_samples"] = min_samples

            csm = old_params.get("cluster_selection_method", "eom")
            csm = typer.prompt(
                "HDBSCAN cluster_selection_method (eom/leaf)", default=csm,
            ).strip().lower()
            if csm not in ("eom", "leaf"):
                console.print(f"[red]Invalid method '{csm}'. Must be 'eom' or 'leaf'.[/red]")
                raise typer.Exit(code=1)
            cluster.params["cluster_selection_method"] = csm

            epsilon = old_params.get("cluster_selection_epsilon", 0.0)
            epsilon = typer.prompt(
                "HDBSCAN cluster_selection_epsilon", default=epsilon, type=float,
            )
            if epsilon < 0.0:
                console.print("[red]cluster_selection_epsilon must be >= 0.0.[/red]")
                raise typer.Exit(code=1)
            cluster.params["cluster_selection_epsilon"] = epsilon

        elif method_choice == "agglomerative":
            # Clear any previously set hdbscan parameters (only clear old non-agglomerative params)
            old_params = cluster.params
            cluster.params = {}
            
            n_clusters = old_params.get("n_clusters", 8)
            n_clusters = typer.prompt("Number of clusters", default=n_clusters, type=int)
            if n_clusters < 2:
                console.print("[red]n_clusters must be >= 2.[/red]")
                raise typer.Exit(code=1)

            linkage = old_params.get("linkage", "ward")
            linkage = typer.prompt(
                "Linkage (ward/complete/average/single)", default=linkage,
            ).strip().lower()
            if linkage not in ("ward", "complete", "average", "single"):
                console.print(f"[red]Invalid linkage '{linkage}'.[/red]")
                raise typer.Exit(code=1)

            cluster.params = {"n_clusters": n_clusters, "linkage": linkage}

        cluster.method = method_choice

        # UMAP reduction options. n_neighbors and min_dist are shared across all
        # UMAP reductions; target_dimensions is prompted per reduction since the
        # 2D (visualization) and higher-D (clustering) reductions differ.
        umap_reductions = [r for r in plan.reductions if isinstance(r, UMAPReduction)]
        if umap_reductions:
            n_neighbors = typer.prompt(
                "UMAP n_neighbors", default=umap_reductions[0].n_neighbors, type=int,
            )
            if n_neighbors < 2:
                console.print("[red]n_neighbors must be >= 2.[/red]")
                raise typer.Exit(code=1)

            min_dist = typer.prompt(
                "UMAP min_dist", default=umap_reductions[0].min_dist, type=float,
            )
            if not 0.0 <= min_dist < 1.0:
                console.print("[red]min_dist must be in [0.0, 1.0).[/red]")
                raise typer.Exit(code=1)

            for reduction in umap_reductions:
                reduction.n_neighbors = n_neighbors
                reduction.min_dist = min_dist
                reduction.target_dimensions = typer.prompt(
                    f"UMAP target_dimensions for the {reduction.target_dimensions}D reduction",
                    default=reduction.target_dimensions, type=int,
                )
                if reduction.target_dimensions < 2:
                    console.print("[red]target_dimensions must be >= 2.[/red]")
                    raise typer.Exit(code=1)

        caption = plan.images.caption
        caption = typer.confirm("Caption images with FastVLM?", default=caption)
        plan.images.caption = caption

        save_plan(plan, plan_path)
        console.print(f"\nPlan saved to {plan_path}")
        console.print(f"  LLM:        {plan.llm.provider.value} / {plan.llm.model}")
        params_str = ", ".join(f"{k}={v}" for k, v in cluster.params.items())
        console.print(f"  Clustering:  method={cluster.method}, {params_str}")
        if umap_reductions:
            dims = ", ".join(f"{r.target_dimensions}D" for r in umap_reductions)
            u = umap_reductions[0]
            console.print(
                f"  UMAP:        n_neighbors={u.n_neighbors}, "
                f"min_dist={u.min_dist}, dimensions=[{dims}]"
            )
        console.print(f"  Images:      caption={'on' if caption else 'off'}")


@app.command(name="ingest-rows")
def ingest_rows_cmd(
    csv_path: Path = typer.Argument(help="Path to the CSV file.", exists=True),
):
    """Ingest rows from a CSV file into the active project."""
    with _open_project() as (project_path, conn):
        summary = ingest_rows(conn, csv_path, project_path)
        logger.info(
            f"Done: {summary['rows_created']} rows, "
            f"{summary['artifacts_linked']} artifacts linked"
        )


@app.command()
def run():
    """Run the full clustering pipeline on the active project."""
    with _open_project() as (project_path, conn):
        active = get_active_job(conn)
        if active:
            if active["cancel_requested_at"]:
                mark_canceled(conn, active["job_id"])
                logger.warning(
                    f"Auto-recovered orphaned job {active['job_id']} "
                    "(cancel was already requested)."
                )
            else:
                logger.error(
                    f"Job {active['job_id']} is already active. "
                    "Only one job can run at a time."
                )
                raise typer.Exit(code=1)

        plan = load_plan(project_path / settings.PLAN_YAML)
        job_id = create_job(conn, "full_run")
        start_job(conn, job_id)
        logger.info(f"Started job {job_id}")

        def _on_step(name, completed, total):
            total_str = str(total) if total else "?"
            console.print(f"  [{completed}/{total_str}] {name} done")

        try:
            summary = run_pipeline(conn, project_path, plan, job_id, on_step=_on_step)
            succeed_job(conn, job_id)
            logger.info(f"Job {job_id} succeeded: {summary}")
        except PipelineCancelled:
            logger.warning(f"Job {job_id} was cancelled.")
            raise typer.Exit(code=1)
        except Exception as exc:
            fail_job(conn, job_id, str(exc))
            logger.error(f"Job {job_id} failed: {exc}")
            raise typer.Exit(code=1)


@app.command()
def jobs():
    """List recent jobs for the active project."""
    with _open_project() as (_project_path, conn):
        rows = list_jobs(conn)
        if not rows:
            console.print("[dim]No jobs found.[/dim]")
            return

        table = Table(title=f"Jobs for '{_project_path.name}'")
        table.add_column("ID", style="bold")
        table.add_column("Type")
        table.add_column("Status")
        table.add_column("Created")
        table.add_column("Finished")
        table.add_column("Error")

        for row in rows:
            status_style = {
                "queued": "dim",
                "running": "yellow",
                "succeeded": "green",
                "failed": "red",
                "canceled": "orange3",
            }.get(row["status"], "")
            table.add_row(
                str(row["job_id"]),
                row["job_type"],
                f"[{status_style}]{row['status']}[/{status_style}]",
                row["created_at"] or "",
                row["finished_at"] or "",
                (row["error_message"] or "")[:60],
            )
        console.print(table)


@app.command()
def job(
    job_id: int = typer.Argument(help="Job ID to inspect."),
):
    """Show details for a single job."""
    with _open_project() as (_project_path, conn):
        row = get_job(conn, job_id)
        if row is None:
            logger.error(f"Job {job_id} not found.")
            raise typer.Exit(code=1)

        console.print(f"[bold]Job {row['job_id']}[/bold]")
        console.print(f"  Type:     {row['job_type']}")
        console.print(f"  Status:   {row['status']}")
        console.print(f"  Created:  {row['created_at']}")
        console.print(f"  Started:  {row['started_at'] or '-'}")
        console.print(f"  Finished: {row['finished_at'] or '-'}")
        if row["error_message"]:
            console.print(f"  Error:    [red]{row['error_message']}[/red]")

        progress = json.loads(row["progress_json"])
        if progress:
            console.print(f"  Progress: {json.dumps(progress, indent=2)}")

        logs = get_job_logs(conn, job_id)
        if logs:
            console.print(f"\n[bold]Logs ({len(logs)} entries):[/bold]")
            for log_entry in logs:
                level_style = {"error": "red", "warning": "yellow"}.get(log_entry["level"], "")
                message = log_entry["message"]
                timestamp = log_entry["created_at"]
                if level_style:
                    console.print(f"  [{level_style}]{timestamp} [{log_entry['level']}] {message}[/{level_style}]")
                else:
                    console.print(f"  {timestamp} [{log_entry['level']}] {message}")


@app.command()
def cancel(
    job_id: int = typer.Argument(help="Job ID to cancel."),
    force: bool = typer.Option(False, "--force", help="Immediately mark the job as canceled."),
):
    """Request cancellation of a running job."""
    with _open_project() as (_project_path, conn):
        row = get_job(conn, job_id)
        if row is None:
            logger.error(f"Job {job_id} not found.")
            raise typer.Exit(code=1)

        if row["status"] not in ("queued", "running"):
            logger.warning(f"Job {job_id} is already {row['status']}. Cannot cancel.")
            return

        if force:
            mark_canceled(conn, job_id)
            logger.info(f"Job {job_id} force-canceled.")
        else:
            request_cancel(conn, job_id)
            logger.info(
                f"Cancellation requested for job {job_id}. "
                "If the pipeline process is no longer running, use --force."
            )


@app.command()
def reset():
    """Clear pipeline outputs (embeddings, clusters, etc.) so the next run starts fresh."""
    with _open_project() as (project_path, conn):
        tables = [
            "cluster_run_critiques",
            "cluster_summaries",
            "cluster_exemplars",
            "cluster_assignments",
            "cluster_runs",
            "reduction_coordinates",
            "reductions",
            "vec_embeddings",
            "embeddings",
            "representations",
        ]
        for table in tables:
            conn.execute(f"DELETE FROM {table}")
        conn.commit()
        typer.echo("Pipeline outputs cleared. Run 'fluster run' to re-process.")


@app.command()
def logs(
    job_id: int | None = typer.Argument(default=None, help="Job ID to show logs for (optional)."),
):
    """Browse job logs. Shows recent logs, or detailed logs for a specific job."""
    with _open_project() as (_project_path, conn):
        if job_id is not None:
            entries = get_job_logs(conn, job_id)
            if not entries:
                console.print(f"[dim]No logs for job {job_id}.[/dim]")
                return
            for entry in entries:
                level_style = {"error": "red", "warning": "yellow", "debug": "dim"}.get(entry["level"], "")
                ts = entry["created_at"]
                msg = entry["message"]
                line = f"{ts}  job={entry['job_id']}  [{entry['level']}]  {msg}"
                if entry["payload_json"]:
                    line += f"\n    {entry['payload_json']}"
                if level_style:
                    console.print(f"[{level_style}]{line}[/{level_style}]")
                else:
                    console.print(line)
        else:
            entries = get_recent_logs(conn)
            if not entries:
                console.print("[dim]No logs found.[/dim]")
                return
            table = Table(title="Recent Logs")
            table.add_column("Time")
            table.add_column("Job", style="bold")
            table.add_column("Level")
            table.add_column("Message")
            for entry in entries:
                level_style = {"error": "red", "warning": "yellow", "debug": "dim"}.get(entry["level"], "")
                level_text = f"[{level_style}]{entry['level']}[/{level_style}]" if level_style else entry["level"]
                table.add_row(
                    entry["created_at"] or "",
                    str(entry["job_id"]),
                    level_text,
                    (entry["message"] or "")[:80],
                )
            console.print(table)


@app.command()
def export(
    cluster_run: int = typer.Option(..., "--cluster-run", help="Cluster run ID to export."),
    output: Path | None = typer.Option(None, "--output", "-o", help="Output file path. Defaults to stdout."),
):
    """Export a cluster run's results to CSV."""
    with _open_project() as (_project_path, conn):
        try:
            csv_text = export_cluster_run(conn, cluster_run)
            if output:
                output.write_text(csv_text)
                logger.info(f"Exported to {output}")
            else:
                typer.echo(csv_text, nl=False)
        except ValueError as exc:
            logger.error(str(exc))
            raise typer.Exit(code=1)


@app.command()
def merge(
    cluster_run: int = typer.Option(..., "--cluster-run", help="Source cluster run ID to merge."),
    force: bool = typer.Option(
        False, "--force", help="Create a new merged run even if one already exists.",
    ),
):
    """Auto-merge a labeled cluster run's redundant clusters into a new run."""
    with _open_project() as (project_path, conn):
        plan = load_plan(project_path / settings.PLAN_YAML)
        try:
            summary = merge_clusters(conn, cluster_run, plan.llm, force=force)
        except ValueError as exc:
            logger.error(str(exc))
            raise typer.Exit(code=1)

        if summary["skipped"]:
            if "cluster_run_id" in summary:
                console.print(
                    f"Cluster run {cluster_run} already has merged run "
                    f"{summary['cluster_run_id']}. Use --force to create another."
                )
            else:
                console.print(f"No clusters were merged ({summary.get('reason', 'nothing to do')}).")
            return

        console.print(
            f"Created merged cluster run [bold]{summary['cluster_run_id']}[/bold] "
            f"({summary['n_clusters_before']} → {summary['n_clusters_after']} clusters)."
        )
        for group in summary["merges"]:
            sources = ", ".join(str(cid) for cid in group["source_cluster_ids"])
            console.print(f"  merged [{sources}] → cluster {group['new_cluster_id']}: {group['rationale']}")


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", "--host", help="Bind address."),
    port: int = typer.Option(8000, "--port", help="Bind port."),
):
    """Start the fluster API server for the active project."""
    project_name = _resolve_project()
    if not project_exists(project_name):
        logger.error(f"Project '{project_name}' does not exist.")
        raise typer.Exit(code=1)

    import uvicorn

    from fluster.server import create_app

    fastapi_app = create_app(project_name)
    logger.info(f"Serving project '{project_name}' on {host}:{port}")
    uvicorn.run(fastapi_app, host=host, port=port)


# Config files (alongside client/src/) whose timestamps mark the build as stale.
_CLIENT_BUILD_CONFIG_FILES = ("package.json", "svelte.config.js", "vite.config.ts", "tsconfig.json")


def _client_build_is_stale(client_dir: Path) -> bool:
    """True if the built client bundle is missing or older than any source file."""
    build_file = client_dir / "build" / "index.js"
    if not build_file.is_file():
        return True
    build_mtime = build_file.stat().st_mtime

    sources = list((client_dir / "src").rglob("*"))
    sources += [client_dir / name for name in _CLIENT_BUILD_CONFIG_FILES]
    for path in sources:
        try:
            if path.is_file() and path.stat().st_mtime > build_mtime:
                return True
        except OSError:
            continue
    return False


@app.command()
def chill(
    port: int = typer.Option(3000, "--port", help="Port to serve on."),
    host: str = typer.Option("127.0.0.1", "--host", help="Bind address."),
    dev: bool = typer.Option(False, "--dev", help="Run SvelteKit dev server instead of production build."),
    no_rebuild: bool = typer.Option(
        False,
        "--no-rebuild",
        help="Don't auto-rebuild a stale client; serve the existing build (error if missing).",
    ),
):
    """Launch the fluster visualization UI for the active project."""
    project_name = _resolve_project()
    if not project_exists(project_name):
        logger.error(f"Project '{project_name}' does not exist.")
        raise typer.Exit(code=1)

    client_dir = settings.CLIENT_DIR
    if not client_dir.is_dir():
        logger.error(f"Client directory not found at {client_dir}")
        raise typer.Exit(code=1)

    db_path = project_dir(project_name) / settings.PROJECT_DB
    env = {
        **os.environ,
        "FLUSTER_DB_PATH": str(db_path),
        "FLUSTER_PROJECT_NAME": project_name,
        "FLUSTER_PROJECT_DIR": str(project_dir(project_name)),
        "PORT": str(port),
        "HOST": host,
    }

    if dev:
        cmd = ["npm", "run", "dev", "--", "--port", str(port), "--host", host]
    else:
        build_file = client_dir / "build" / "index.js"
        stale = _client_build_is_stale(client_dir)
        if stale and no_rebuild:
            if not build_file.is_file():
                logger.error(
                    "Client not built. Run 'npm run build' in the client/ directory, "
                    "or use --dev for the dev server."
                )
                raise typer.Exit(code=1)
            logger.warning(
                "Client build is stale (source newer than build/). Serving anyway; "
                "run 'npm run build' in client/ to pick up the latest changes."
            )
        elif stale:
            if build_file.is_file():
                logger.warning("Client build is stale (source newer than build/).")
            else:
                logger.info("Client not built yet.")
            logger.info("Rebuilding client (npm run build)...")
            try:
                build = subprocess.run(["npm", "run", "build"], cwd=client_dir)
            except FileNotFoundError:
                logger.error("npm not found. Install Node.js/npm, or use --dev / --no-rebuild.")
                raise typer.Exit(code=1)
            if build.returncode != 0:
                logger.error("Client build failed. See the output above, or use --dev.")
                raise typer.Exit(code=build.returncode)
            logger.info("Rebuild complete.")
        cmd = ["node", "build/index.js"]

    url = f"http://{host}:{port}"
    logger.info(f"Launching fluster UI for '{project_name}' at {url}")
    result = subprocess.run(cmd, cwd=client_dir, env=env)
    raise typer.Exit(code=result.returncode)


def main():
    app()


if __name__ == "__main__":
    main()
