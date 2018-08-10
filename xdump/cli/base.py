import click


COMMON_DECORATORS = [
    click.option('-D', '--dbname', required=True, help='database to work with'),
    click.option('-v', '--verbosity', help='verbosity level', default=0, count=True, type=click.IntRange(0, 2)),
]


PG_DECORATORS = [
    click.option('-U', '--user', required=True, help='connect as specified database user'),
    click.option('-W', '--password', help='password for the DB connection'),
    click.option('-H', '--host', default='127.0.0.1', help='database server host or socket directory'),
    click.option('-P', '--port', default='5432', help='database server port number'),
]
