import zipfile
from pathlib import Path

import pytest
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


CURRENT_DIR = Path(__file__).parent.absolute()
EMPLOYEES_SQL = '''
WITH RECURSIVE employees_cte AS (
  SELECT * 
  FROM recent_employees
  UNION
  SELECT E.*
  FROM employees E
  INNER JOIN employees_cte ON (employees_cte.manager_id = E.id)
), recent_employees AS (
  SELECT * 
  FROM employees
  ORDER BY id DESC
  LIMIT 2
)
SELECT * FROM employees_cte
'''


@pytest.fixture
def cursor(postgresql):
    postgresql.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return postgresql.cursor()


@pytest.fixture
def postgres_backend(postgresql):
    from xdump.postgresql import PostgreSQLBackend

    parameters = postgresql.get_dsn_parameters()
    return PostgreSQLBackend(
        dbname=parameters['dbname'],
        user=parameters['user'],
        password=None,
        host=parameters['host'],
        port=parameters['port'],
    )


def execute_file(cursor, filename):
    with (CURRENT_DIR / filename).open('r') as fd:
        sql = fd.read()
    cursor.execute(sql)


@pytest.fixture
def schema(cursor):
    execute_file(cursor, 'sql/schema.sql')


@pytest.fixture
def data(cursor):
    execute_file(cursor, 'sql/postgres_data.sql')


@pytest.fixture
def archive_filename(tmpdir):
    return str(tmpdir.join('dump.zip'))


@pytest.fixture
def archive(archive_filename):
    with zipfile.ZipFile(archive_filename, 'w', zipfile.ZIP_DEFLATED) as file:
        yield file


def assert_schema(schema, with_data=False):
    assert b'Dumped from database version 10.1' in schema
    assert b'CREATE TABLE groups' in schema
    for table in ('groups', 'employees', 'tickets'):
        assert (f'COPY {table}'.encode() in schema) is with_data


def assert_unused_sequences(archive):
    assert "SELECT pg_catalog.setval('groups_id_seq', 1, false);".encode() in archive.read('dump/sequences.sql')
