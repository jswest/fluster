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
from fluster.config.plan import load_plan, save_plan, Plan, LLMProvider
from fluster.config.project import (
    create_project,
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

        plan.embedding.model_name = typer.prompt(
            "Embedding model", default=plan.embedding.model_name,
        )
        plan.embedding.max_tokens = typer.prompt(
            "Embedding max tokens", default=plan.embedding.max_tokens, type=int,
        )

        provider_str = typer.prompt(
            "LLM provider (openai/ollama)", default=plan.llm.provider.value,
        ).strip().lower()
        plan.llm.provider = LLMProvider(provider_str)

        plan.llm.model = typer.prompt(
            "LLM model", default=plan.llm.model,
        )

        min_cluster = plan.clustering[0].params.get("min_cluster_size", 5)
        min_cluster = typer.prompt(
            "HDBSCAN min_cluster_size", default=min_cluster, type=int,
        )
        plan.clustering[0].params["min_cluster_size"] = min_cluster

        save_plan(plan, plan_path)
        console.print(f"\nPlan saved to {plan_path}")
        console.print(f"  Embedding:  {plan.embedding.model_name} (max {plan.embedding.max_tokens} tokens)")
        console.print(f"  LLM:        {plan.llm.provider.value} / {plan.llm.model}")
        console.print(f"  Clustering:  min_cluster_size={min_cluster}")


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


@app.command()
def chill(
    port: int = typer.Option(3000, "--port", help="Port to serve on."),
    host: str = typer.Option("127.0.0.1", "--host", help="Bind address."),
    dev: bool = typer.Option(False, "--dev", help="Run SvelteKit dev server instead of production build."),
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
        "PORT": str(port),
        "HOST": host,
    }

    if dev:
        cmd = ["npm", "run", "dev", "--", "--port", str(port), "--host", host]
    else:
        if not (client_dir / "build" / "index.js").is_file():
            logger.error(
                "Client not built. Run 'npm run build' in the client/ directory, "
                "or use --dev for the dev server."
            )
            raise typer.Exit(code=1)
        cmd = ["node", "build/index.js"]

    url = f"http://{host}:{port}"
    logger.info(f"Launching fluster UI for '{project_name}' at {url}")
    result = subprocess.run(cmd, cwd=client_dir, env=env)
    raise typer.Exit(code=result.returncode)


def main():
    app()


if __name__ == "__main__":
    main()
