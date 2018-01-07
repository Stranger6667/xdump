import os
import sqlite3
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

ALL = {'postgres', 'sqlite'}
DATABASE = os.environ['DB']
IS_POSTGRES = DATABASE == 'postgres'
IS_SQLITE = DATABASE == 'sqlite'

# Travis has only PostgreSQL 9.6
if IS_POSTGRES and 'TRAVIS' in os.environ:
    from pytest_postgresql import factories

    postgresql_proc = factories.postgresql_proc(executable=f'/usr/lib/postgresql/9.6/bin/pg_ctl')
    postgresql = factories.postgresql('postgresql_proc')


@pytest.fixture
def dbname(tmpdir):
    return str(tmpdir.join('test.db'))


@pytest.fixture
def backend(request):
    if IS_POSTGRES:
        from xdump.postgresql import PostgreSQLBackend

        postgresql = request.getfixturevalue('postgresql')
        parameters = postgresql.get_dsn_parameters()
        return PostgreSQLBackend(
            dbname=parameters['dbname'],
            user=parameters['user'],
            password=None,
            host=parameters['host'],
            port=parameters['port'],
        )
    elif IS_SQLITE:
        from xdump.sqlite import SQLiteBackend

        dbname = request.getfixturevalue('dbname')
        return SQLiteBackend(
            dbname=dbname,
            user=None,
            password=None,
            host=None,
            port=None,
        )


@pytest.fixture
def cursor(request):
    if IS_POSTGRES:
        postgresql = request.getfixturevalue('postgresql')
        postgresql.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return postgresql.cursor()
    elif IS_SQLITE:
        dbname = request.getfixturevalue('dbname')
        return sqlite3.connect(dbname, isolation_level=None).cursor()


def read_sql_file(filename):
    with (CURRENT_DIR / filename).open('r') as fd:
        return fd.read()


@pytest.fixture
def execute_file(cursor):

    def executor(filename):
        sql = read_sql_file(filename)
        if IS_POSTGRES:
            cursor.execute(sql)
        elif IS_SQLITE:
            cursor.executescript(sql)

    return executor


@pytest.fixture
def schema(execute_file):
    execute_file('sql/schema.sql')


@pytest.fixture
def data(execute_file):
    if IS_POSTGRES:
        execute_file('sql/postgres_data.sql')
    elif IS_SQLITE:
        execute_file('sql/sqlite_data.sql')


def pytest_runtest_setup(item):
    if isinstance(item, item.Function) and not item.get_marker(DATABASE) and ALL.intersection(item.keywords):
        pytest.skip(f'Cannot run on {DATABASE}')


@pytest.fixture
def archive_filename(tmpdir):
    return str(tmpdir.join('dump.zip'))


@pytest.fixture
def archive(archive_filename):
    with zipfile.ZipFile(archive_filename, 'w', zipfile.ZIP_DEFLATED) as file:
        yield file


def assert_schema(schema, with_data=False):
    assert b'CREATE TABLE groups' in schema
    for table in ('groups', 'employees', 'tickets'):
        assert (f'COPY {table}'.encode() in schema) is with_data


def assert_unused_sequences(archive):
    assert "SELECT pg_catalog.setval('groups_id_seq', 1, false);".encode() in archive.read('dump/sequences.sql')
