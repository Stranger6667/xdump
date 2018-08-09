import click

from .utils import apply_decorators, import_backend


@click.group(name='xload')
@click.version_option()
def load():
    pass


DEFAULT_PARAMETERS = [
    load.command(),
    click.option('-D', '--dbname', required=True, help='database to dump'),
    click.option('-v', '--verbosity', help='verbosity level', default=0, count=True, type=click.IntRange(0, 2)),
    click.option('-i', '--input', required=True, help='input file name'),
]


def command(func):
    return apply_decorators(DEFAULT_PARAMETERS)(func)


def base_load(backend_path, user, password, host, port, dbname, verbosity, input):
    click.echo('Loading ...')
    click.echo('Input file: {0}'.format(input))

    backend_class = import_backend(backend_path)

    backend = backend_class(dbname=dbname, host=host, port=port, user=user, password=password, verbosity=verbosity)
    backend.load(input)
    click.echo('Done!')


@command
@click.option('-U', '--user', required=True, help='connect as specified database user')
@click.option('-W', '--password', help='password for the DB connection')
@click.option('-H', '--host', default='127.0.0.1', help='database server host or socket directory')
@click.option('-P', '--port', default='5432', help='database server port number')
def postgres(user, password, host, port, dbname, verbosity, input):
    base_load('xdump.postgresql.PostgreSQLBackend', user, password, host, port, dbname, verbosity, input)


@command
def sqlite(dbname, verbosity, input):
    base_load('xdump.sqlite.SQLiteBackend', None, None, None, None, dbname, verbosity, input)
