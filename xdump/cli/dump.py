import sys
import zipfile

import click

from .base import COMMON_DECORATORS, PG_DECORATORS
from .utils import apply_decorators, init_backend


@click.group(name='xdump')
@click.version_option()
def dump():
    pass


def parse_partial(ctx, param, value):
    """Parse values for `partial` option. They should be in the format `table:select SQL`."""

    def parse_value(value):
        try:
            table_name, sql = value.split(':', 1)
            return table_name.strip(), sql.strip()
        except ValueError:
            raise click.exceptions.BadParameter(
                'partial table specification should be in the following format: "table:select SQL"',
                param=param,
            )

    return dict(parse_value(partial) for partial in value)


COMPRESSION_MAPPING = {
    'deflated': zipfile.ZIP_DEFLATED,
    'stored': zipfile.ZIP_STORED,
}
if sys.version_info[0] == 3:
    # BZIP2 & LZMA are not available on Python 2
    COMPRESSION_MAPPING.update(bzip2=zipfile.ZIP_BZIP2, lzma=zipfile.ZIP_LZMA)


DEFAULT_PARAMETERS = [
    dump.command(),
    click.option('-o', '--output', required=True, help='output file name'),
    click.option('-f', '--full', help='table name to be fully dumped. Could be used multiple times', multiple=True),
    click.option(
        '-p', '--partial',
        help='partial tables specification in a form "table_name:select SQL". Could be used multiple times',
        callback=parse_partial,
        multiple=True
    ),
    click.option(
        '-c', '--compression',
        help='dump compression level',
        default='deflated',
        type=click.Choice(list(COMPRESSION_MAPPING.keys()))
    ),
    click.option('--schema/--no-schema', help='include / exclude the schema from the dump', default=True),
    click.option('--data/--no-data', help='include / exclude the data from the dump', default=True),
] + COMMON_DECORATORS


def base_dump(backend_path, output, full, partial, compression, schema, data, **kwargs):
    """Common implementation of dump command. Writes a few logs, imports a backend and makes a dump."""
    compression = COMPRESSION_MAPPING[compression]

    click.echo('Dumping ...')
    click.echo('Output file: {0}'.format(output))

    backend = init_backend(backend_path, **kwargs)
    backend.dump(
        output, full_tables=full, partial_tables=partial, compression=compression, dump_schema=schema,
        dump_data=data
    )
    click.echo('Done!')


@apply_decorators(DEFAULT_PARAMETERS + PG_DECORATORS)
def postgres(user, password, host, port, dbname, verbosity, output, full, partial, compression, schema, data):
    base_dump('xdump.postgresql.PostgreSQLBackend', output, full, partial, compression, schema, data, user=user,
              password=password, host=host, port=port, dbname=dbname, verbosity=verbosity)


@apply_decorators(DEFAULT_PARAMETERS)
def sqlite(dbname, verbosity, output, full, partial, compression, schema, data):
    base_dump('xdump.sqlite.SQLiteBackend', output, full, partial, compression, schema, data, dbname=dbname,
              verbosity=verbosity)
