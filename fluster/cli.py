"""fluster CLI — powered by Typer."""

import json
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
from fluster.pipeline.ingest import ingest_rows
from fluster.pipeline.run import PipelineCancelled, run_pipeline

console = Console()

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
        pdir = create_project(project_name)
    except FileExistsError:
        logger.error(f"Project '{project_name}' already exists.")
        raise typer.Exit(code=1)
    logger.info(f"Created project '{project_name}' at {pdir}")


@app.command(name="ingest-rows")
def ingest_rows_cmd(
    project_name: str = typer.Argument(help="Project to ingest into."),
    csv_path: Path = typer.Argument(help="Path to the CSV file.", exists=True),
):
    """Ingest rows from a CSV file into a project."""
    if not project_exists(project_name):
        logger.error(f"Project '{project_name}' does not exist.")
        raise typer.Exit(code=1)

    pdir = project_dir(project_name)
    conn = connect(pdir)
    try:
        summary = ingest_rows(conn, csv_path, pdir)
        logger.info(
            f"Done: {summary['rows_created']} rows, "
            f"{summary['artifacts_linked']} artifacts linked"
        )
    finally:
        conn.close()


@app.command()
def run(project_name: str = typer.Argument(help="Project to run the full pipeline on.")):
    """Run the full clustering pipeline."""
    if not project_exists(project_name):
        logger.error(f"Project '{project_name}' does not exist.")
        raise typer.Exit(code=1)

    pdir = project_dir(project_name)
    conn = connect(pdir)
    try:
        active = get_active_job(conn)
        if active:
            logger.error(
                f"Job {active['job_id']} is already active. "
                "Only one job can run at a time."
            )
            raise typer.Exit(code=1)

        plan = load_plan(pdir / settings.PLAN_YAML)
        job_id = create_job(conn, "full_run")
        start_job(conn, job_id)
        logger.info(f"Started job {job_id} for project '{project_name}'")

        try:
            summary = run_pipeline(conn, pdir, plan, job_id)
            succeed_job(conn, job_id)
            logger.info(f"Job {job_id} succeeded: {summary}")
        except PipelineCancelled:
            logger.warning(f"Job {job_id} was cancelled.")
            raise typer.Exit(code=1)
        except Exception as exc:
            fail_job(conn, job_id, str(exc))
            logger.error(f"Job {job_id} failed: {exc}")
            raise typer.Exit(code=1)
    finally:
        conn.close()


@app.command()
def jobs(project_name: str = typer.Argument(help="Project to list jobs for.")):
    """List recent jobs for a project."""
    if not project_exists(project_name):
        logger.error(f"Project '{project_name}' does not exist.")
        raise typer.Exit(code=1)

    pdir = project_dir(project_name)
    conn = connect(pdir)
    try:
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
    finally:
        conn.close()


@app.command()
def job(
    project_name: str = typer.Argument(help="Project name."),
    job_id: int = typer.Argument(help="Job ID to inspect."),
):
    """Show details for a single job."""
    if not project_exists(project_name):
        logger.error(f"Project '{project_name}' does not exist.")
        raise typer.Exit(code=1)

    pdir = project_dir(project_name)
    conn = connect(pdir)
    try:
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
            for log in logs:
                level_style = {"error": "red", "warning": "yellow"}.get(log["level"], "")
                msg = log["message"]
                ts = log["created_at"]
                if level_style:
                    console.print(f"  [{level_style}]{ts} [{log['level']}] {msg}[/{level_style}]")
                else:
                    console.print(f"  {ts} [{log['level']}] {msg}")
    finally:
        conn.close()


@app.command()
def cancel(
    project_name: str = typer.Argument(help="Project name."),
    job_id: int = typer.Argument(help="Job ID to cancel."),
):
    """Request cancellation of a running job."""
    if not project_exists(project_name):
        logger.error(f"Project '{project_name}' does not exist.")
        raise typer.Exit(code=1)

    pdir = project_dir(project_name)
    conn = connect(pdir)
    try:
        row = get_job(conn, job_id)
        if row is None:
            logger.error(f"Job {job_id} not found.")
            raise typer.Exit(code=1)

        if row["status"] not in ("queued", "running"):
            logger.warning(f"Job {job_id} is already {row['status']}. Cannot cancel.")
            return

        request_cancel(conn, job_id)
        logger.info(f"Cancellation requested for job {job_id}.")
    finally:
        conn.close()


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


def main():
    app()


if __name__ == "__main__":
    main()
