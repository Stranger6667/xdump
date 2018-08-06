import click


@click.group()
@click.version_option()
def xdump():
    pass


@xdump.command()
@click.option('-U', '--user', required=True, help='connect as specified database user')
@click.option('-W', '--password', help='password for the DB connection')
@click.option('-H', '--host', default='127.0.0.1', help='database server host or socket directory')
@click.option('-P', '--port', default='5432', help='database server port number')
@click.option('-D', '--dbname', required=True, help='database to dump')
@click.option('-o', '--output', required=True, help='output file name')
@click.option('-f', '--full', help='table name to be fully dumped. Could be used multiple times', multiple=True)
def postgres(user, password, host, port, dbname, output, full):
    click.echo('Dumping ...')
    click.echo('Output file: {0}'.format(output))

    from .postgresql import PostgreSQLBackend

    backend = PostgreSQLBackend(dbname=dbname, host=host, port=port, user=user, password=password)
    backend.dump(output, full_tables=full)
    click.echo('Done!')
