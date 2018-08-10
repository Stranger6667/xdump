import click

from .base import COMMON_DECORATORS, PG_DECORATORS
from .utils import apply_decorators, init_backend


@click.group(name='xload')
@click.version_option()
def load():
    pass


DEFAULT_PARAMETERS = [
    load.command(),
    click.option('-i', '--input', required=True, help='input file name'),
] + COMMON_DECORATORS


def base_load(backend_path, user, password, host, port, dbname, verbosity, input):
    click.echo('Loading ...')
    click.echo('Input file: {0}'.format(input))

    backend = init_backend(
        backend_path, dbname=dbname, host=host, port=port, user=user, password=password, verbosity=verbosity
    )

    backend.load(input)
    click.echo('Done!')


@apply_decorators(DEFAULT_PARAMETERS + PG_DECORATORS)
def postgres(user, password, host, port, dbname, verbosity, input):
    base_load('xdump.postgresql.PostgreSQLBackend', user, password, host, port, dbname, verbosity, input)


@apply_decorators(DEFAULT_PARAMETERS)
def sqlite(dbname, verbosity, input):
    base_load('xdump.sqlite.SQLiteBackend', None, None, None, None, dbname, verbosity, input)
