import zipfile

import click


@click.group()
@click.version_option()
def xdump():
    pass


def parse_partial(*partials):

    def parse_value(value):
        try:
            table_name, sql = value.split(':', 1)
            return table_name.strip(), sql.strip()
        except ValueError:
            raise click.exceptions.BadParameter(
                'partial table specification should be in the following format: "table:select SQL"',
            )

    return dict(parse_value(partial) for partial in partials)


COMPRESSION_MAPPING = {
    'deflated': zipfile.ZIP_DEFLATED,
    'stored': zipfile.ZIP_STORED,
    'bzip2': zipfile.ZIP_BZIP2,
    'lzma': zipfile.ZIP_LZMA,
}


@xdump.command()
@click.option('-U', '--user', required=True, help='connect as specified database user')
@click.option('-W', '--password', help='password for the DB connection')
@click.option('-H', '--host', default='127.0.0.1', help='database server host or socket directory')
@click.option('-P', '--port', default='5432', help='database server port number')
@click.option('-D', '--dbname', required=True, help='database to dump')
@click.option('-o', '--output', required=True, help='output file name')
@click.option('-f', '--full', help='table name to be fully dumped. Could be used multiple times', multiple=True)
@click.option(
    '-p', '--partial',
    help='partial tables specification in a form "table_name:select SQL". Could be used multiple times',
    multiple=True
)
@click.option(
    '-c', '--compression',
    help='dump compression level',
    default='deflated',
    type=click.Choice(['deflated', 'stored', 'bzip2', 'lzma'])
)
@click.option('--dump-schema', help='include / exclude the schema from the dump', default=True, type=bool)
@click.option('--dump-data', help='include / exclude the data from the dump', default=True, type=bool)
def postgres(user, password, host, port, dbname, output, full, partial, compression, dump_schema, dump_data):
    if partial:
        partial = parse_partial(*partial)
    compression = COMPRESSION_MAPPING[compression]

    click.echo('Dumping ...')
    click.echo('Output file: {0}'.format(output))

    from .postgresql import PostgreSQLBackend

    backend = PostgreSQLBackend(dbname=dbname, host=host, port=port, user=user, password=password)
    backend.dump(
        output, full_tables=full, partial_tables=partial, compression=compression, dump_schema=dump_schema,
        dump_data=dump_data
    )
    click.echo('Done!')


@xdump.command()
@click.option('-D', '--dbname', required=True, help='database to dump')
@click.option('-o', '--output', required=True, help='output file name')
@click.option('-f', '--full', help='table name to be fully dumped. Could be used multiple times', multiple=True)
@click.option(
    '-p', '--partial',
    help='partial tables specification in a form "table_name:select SQL". Could be used multiple times',
    multiple=True
)
@click.option(
    '-c', '--compression',
    help='dump compression level',
    default='deflated',
    type=click.Choice(['deflated', 'stored', 'bzip2', 'lzma'])
)
@click.option('--dump-schema', help='include / exclude the schema from the dump', default=True, type=bool)
@click.option('--dump-data', help='include / exclude the data from the dump', default=True, type=bool)
def sqlite(dbname, output, full, partial, compression, dump_schema, dump_data):
    if partial:
        partial = parse_partial(*partial)
    compression = COMPRESSION_MAPPING[compression]

    click.echo('Dumping ...')
    click.echo('Output file: {0}'.format(output))

    from .sqlite import SQLiteBackend

    backend = SQLiteBackend(dbname=dbname, host=None, port=None, user=None, password=None)
    backend.dump(
        output, full_tables=full, partial_tables=partial, compression=compression, dump_schema=dump_schema,
        dump_data=dump_data
    )
    click.echo('Done!')
