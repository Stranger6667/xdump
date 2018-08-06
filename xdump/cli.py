import click


@click.group()
@click.version_option()
def xdump():
    pass


@xdump.command()
@click.option('-U', '--user', help='connect as specified database user')
@click.option('-W', '--password', help='password for the DB connection')
@click.option('-h', '--host', default='127.0.0.1', help='database server host or socket directory')
@click.option('-p', '--port', default='5432', help='database server port number')
@click.option('-d', '--dbname', help='database to dump')
def postgres(user, password, host, port, dbname):
    from .postgresql import PostgreSQLBackend

    backend = PostgreSQLBackend(dbname=dbname, host=host, port=port, user=user, password=password)
