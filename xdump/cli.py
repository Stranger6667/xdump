import sys
import zipfile

import click


@click.group()
@click.version_option()
def xdump():
    pass


def parse_partial(ctx, param, value):

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
    COMPRESSION_MAPPING.update(bzip2=zipfile.ZIP_BZIP2, lzma=zipfile.ZIP_LZMA)


def command(func):
    decorators = [
        xdump.command(),
        click.option('-D', '--dbname', required=True, help='database to dump'),
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
        click.option('--dump-schema', help='include / exclude the schema from the dump', default=True, type=bool),
        click.option('--dump-data', help='include / exclude the data from the dump', default=True, type=bool),
    ]
    for decorator in reversed(decorators):
        func = decorator(func)
    return func


def import_backend(path):
    module_name, class_name = path.rsplit('.', 1)
    module = __import__(module_name, fromlist=[class_name])
    return getattr(module, class_name)


def base_dump(backend_path, user, password, host, port, dbname, output, full, partial, compression, dump_schema,
              dump_data):
    compression = COMPRESSION_MAPPING[compression]

    click.echo('Dumping ...')
    click.echo('Output file: {0}'.format(output))

    backend_class = import_backend(backend_path)

    backend = backend_class(dbname=dbname, host=host, port=port, user=user, password=password)
    backend.dump(
        output, full_tables=full, partial_tables=partial, compression=compression, dump_schema=dump_schema,
        dump_data=dump_data
    )
    click.echo('Done!')


@command
@click.option('-U', '--user', required=True, help='connect as specified database user')
@click.option('-W', '--password', help='password for the DB connection')
@click.option('-H', '--host', default='127.0.0.1', help='database server host or socket directory')
@click.option('-P', '--port', default='5432', help='database server port number')
def postgres(user, password, host, port, dbname, output, full, partial, compression, dump_schema, dump_data):
    base_dump('xdump.postgresql.PostgreSQLBackend', user, password, host, port, dbname, output, full, partial,
              compression, dump_schema, dump_data)


@command
def sqlite(dbname, output, full, partial, compression, dump_schema, dump_data):
    base_dump('xdump.sqlite.SQLiteBackend', None, None, None, None, dbname, output, full, partial, compression,
              dump_schema, dump_data)
