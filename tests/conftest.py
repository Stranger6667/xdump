import zipfile
from pathlib import Path

import pytest
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from xdump.core import DatabaseWrapper


CURRENT_DIR = Path(__file__).parent.absolute()


@pytest.fixture
def cursor(postgresql):
    postgresql.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return postgresql.cursor()


def execute_file(cursor, filename):
    with (CURRENT_DIR / filename).open('r') as fd:
        sql = fd.read()
    cursor.execute(sql)


@pytest.fixture
def schema(cursor):
    execute_file(cursor, 'schema.sql')


@pytest.fixture
def data(cursor):
    execute_file(cursor, 'data.sql')


@pytest.fixture
def db_wrapper(postgresql):
    parameters = postgresql.get_dsn_parameters()
    return DatabaseWrapper(
        backend='postgres',
        dbname=parameters['dbname'],
        user=parameters['user'],
        password=None,
        host=parameters['host'],
        port=parameters['port'],
    )


@pytest.fixture
def archive_filename(tmpdir):
    return str(tmpdir.join('dump.zip'))


@pytest.fixture
def archive(archive_filename):
    with zipfile.ZipFile(archive_filename, 'w', zipfile.ZIP_DEFLATED) as file:
        yield file


def assert_schema(schema, postgres_backend):
    assert b'Dumped from database version 10.1' in schema
    assert b'CREATE TABLE groups' in schema
    selectable_tables = postgres_backend.get_selectable_tables()
    for table in selectable_tables:
        assert f'COPY {table}'.encode() not in schema
