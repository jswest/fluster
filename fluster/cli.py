"""fluster CLI — powered by Typer."""

import typer
from loguru import logger

from fluster import __version__
from fluster.config.project import create_project

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


def main():
    app()


if __name__ == "__main__":
    main()
