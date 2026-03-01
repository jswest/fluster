"""fluster CLI — powered by Typer."""

from pathlib import Path

import typer
from loguru import logger

from fluster import __version__
from fluster.config.project import create_project, project_dir, project_exists
from fluster.db.connection import connect
from fluster.pipeline.ingest import ingest_rows

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
