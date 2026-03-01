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
from fluster.config.plan import load_plan
from fluster.config.project import create_project, project_dir, project_exists
from fluster.db.connection import connect
from fluster.jobs.manager import (
    create_job,
    fail_job,
    get_active_job,
    get_job,
    get_job_logs,
    list_jobs,
    request_cancel,
    start_job,
    succeed_job,
)
from fluster.pipeline.export import export_cluster_run
from fluster.pipeline.ingest import ingest_rows
from fluster.pipeline.run import PipelineCancelled, run_pipeline

console = Console()


@contextmanager
def _open_project(project_name: str):
    """Validate a project exists, then yield (project_path, connection).

    Closes the connection on exit.
    """
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
    logger.info(f"Created project '{project_name}' at {project_path}")


@app.command(name="ingest-rows")
def ingest_rows_cmd(
    project_name: str = typer.Argument(help="Project to ingest into."),
    csv_path: Path = typer.Argument(help="Path to the CSV file.", exists=True),
):
    """Ingest rows from a CSV file into a project."""
    with _open_project(project_name) as (project_path, conn):
        summary = ingest_rows(conn, csv_path, project_path)
        logger.info(
            f"Done: {summary['rows_created']} rows, "
            f"{summary['artifacts_linked']} artifacts linked"
        )


@app.command()
def run(project_name: str = typer.Argument(help="Project to run the full pipeline on.")):
    """Run the full clustering pipeline."""
    with _open_project(project_name) as (project_path, conn):
        active = get_active_job(conn)
        if active:
            logger.error(
                f"Job {active['job_id']} is already active. "
                "Only one job can run at a time."
            )
            raise typer.Exit(code=1)

        plan = load_plan(project_path / settings.PLAN_YAML)
        job_id = create_job(conn, "full_run")
        start_job(conn, job_id)
        logger.info(f"Started job {job_id} for project '{project_name}'")

        try:
            summary = run_pipeline(conn, project_path, plan, job_id)
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
def jobs(project_name: str = typer.Argument(help="Project to list jobs for.")):
    """List recent jobs for a project."""
    with _open_project(project_name) as (_project_path, conn):
        rows = list_jobs(conn)
        if not rows:
            console.print("[dim]No jobs found.[/dim]")
            return

        table = Table(title=f"Jobs for '{project_name}'")
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
    project_name: str = typer.Argument(help="Project name."),
    job_id: int = typer.Argument(help="Job ID to inspect."),
):
    """Show details for a single job."""
    with _open_project(project_name) as (_project_path, conn):
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
    project_name: str = typer.Argument(help="Project name."),
    job_id: int = typer.Argument(help="Job ID to cancel."),
):
    """Request cancellation of a running job."""
    with _open_project(project_name) as (_project_path, conn):
        row = get_job(conn, job_id)
        if row is None:
            logger.error(f"Job {job_id} not found.")
            raise typer.Exit(code=1)

        if row["status"] not in ("queued", "running"):
            logger.warning(f"Job {job_id} is already {row['status']}. Cannot cancel.")
            return

        request_cancel(conn, job_id)
        logger.info(f"Cancellation requested for job {job_id}.")


@app.command()
def export(
    project_name: str = typer.Argument(help="Project to export from."),
    cluster_run: int = typer.Option(..., "--cluster-run", help="Cluster run ID to export."),
    output: Path | None = typer.Option(None, "--output", "-o", help="Output file path. Defaults to stdout."),
):
    """Export a cluster run's results to CSV."""
    with _open_project(project_name) as (_project_path, conn):
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
    project_name: str = typer.Argument(help="Project to serve."),
    host: str = typer.Option("127.0.0.1", "--host", help="Bind address."),
    port: int = typer.Option(8000, "--port", help="Bind port."),
):
    """Start the fluster API server for a project."""
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
    project_name: str = typer.Argument(help="Project to visualize."),
    port: int = typer.Option(3000, "--port", help="Port to serve on."),
    host: str = typer.Option("127.0.0.1", "--host", help="Bind address."),
    dev: bool = typer.Option(False, "--dev", help="Run SvelteKit dev server instead of production build."),
):
    """Launch the fluster visualization UI for a project."""
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
