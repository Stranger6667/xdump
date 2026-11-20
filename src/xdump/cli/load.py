from typing import Any

import click

from .base import COMMON_DECORATORS, PG_DECORATORS
from .utils import Decorator, apply_decorators, init_backend


@click.group(name="xload")
@click.version_option()
def load() -> None:
    pass


DEFAULT_PARAMETERS: list[Decorator] = [
    load.command(),
    click.option("-i", "--input", required=True, help="input file name"),
    click.option(
        "-m",
        "--cleanup-method",
        help="method of DB cleaning up",
        type=click.Choice(("recreate", "truncate")),
    ),
] + COMMON_DECORATORS


def base_load(backend_path: str, input: str, cleanup_method: str | None, **kwargs: Any) -> None:
    click.echo("Loading ...")
    click.echo(f"Input file: {input}")

    backend = init_backend(backend_path, **kwargs)
    try:
        if cleanup_method == "truncate":
            backend.truncate()
        elif cleanup_method == "recreate":
            backend.recreate_database()
        backend.load(input)
    finally:
        backend.cache_clear()
    click.echo("Done!")


@apply_decorators(DEFAULT_PARAMETERS + PG_DECORATORS)
def postgres(
    user: str,
    password: str | None,
    host: str,
    port: str,
    dbname: str,
    verbosity: int,
    input: str,
    cleanup_method: str | None,
) -> None:
    base_load(
        "xdump.postgresql.PostgreSQLBackend",
        input,
        cleanup_method,
        user=user,
        password=password,
        host=host,
        port=port,
        dbname=dbname,
        verbosity=verbosity,
    )


@apply_decorators(DEFAULT_PARAMETERS)
def sqlite(dbname: str, verbosity: int, input: str, cleanup_method: str | None) -> None:
    base_load(
        "xdump.sqlite.SQLiteBackend",
        input,
        cleanup_method,
        dbname=dbname,
        verbosity=verbosity,
    )
