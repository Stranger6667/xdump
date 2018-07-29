import os
import platform
import sqlite3
import sys
import zipfile
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import attr
import pytest


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

    postgresql_proc = factories.postgresql_proc(executable='/usr/lib/postgresql/9.6/bin/pg_ctl')
    postgresql = factories.postgresql('postgresql_proc')


def is_search_path_fixed(connection):
    """
    Check if a security issue with `search_path` is fixed in the current PG version. CVE-2018-1058
    """
    version = connection.server_version
    return version >= 100003 or \
        90608 <= version < 100000 or \
        90512 <= version < 90600 or \
        90417 <= version < 90500 or \
        90322 <= version < 90400


@pytest.fixture
def dbname(tmpdir):
    return str(tmpdir.join('test.db'))


@attr.s(cmp=False)
class BackendWrapper:
    """
    Runs special commands for specific DB backend, that are useful for testing.
    """
    backend = attr.ib()
    request = attr.ib()

    def is_database_exists(self, dbname):
        raise NotImplementedError

    def assert_namelist(self, archive):
        raise NotImplementedError

    def get_tables_count(self):
        raise NotImplementedError

    def get_tickets_count(self):
        return self.backend.run('SELECT COUNT(*) AS count FROM tickets')[0]['count']

    def get_new_database_name(self):
        raise NotImplementedError

    def _get_concurrent_insert_class(self, query):
        cursor = self.request.getfixturevalue('cursor')

        class ConcurrentInsertEmulator:
            is_loaded = False

            def insert(self, *args, **kwargs):
                if not self.is_loaded:
                    self._insert()
                    self.is_loaded = True

            def _insert(self):
                cursor.execute(query)

        return ConcurrentInsertEmulator

    @contextmanager
    def concurrent_insert(self, query):
        concurrent_insert = self._get_concurrent_insert_class(query)()
        with patch.object(self.backend, 'export_to_csv', wraps=self.backend.export_to_csv,
                          side_effect=concurrent_insert.insert):
            yield

    def assert_schema(self, schema):
        for table in ('groups', 'employees', 'tickets'):
            assert 'CREATE TABLE {0}'.format(table).encode() in schema

    def assert_dump(self, archive_filename):
        archive = zipfile.ZipFile(archive_filename)
        self.assert_namelist(archive)
        self.assert_groups(archive)
        self.assert_employees(archive)
        schema = archive.read('dump/schema.sql')
        self.assert_schema(schema)

    def assert_content(self, archive, table, expected):
        rows = set(archive.read('dump/data/{}.csv'.format(table)).split(b'\n'))
        rows.remove(b'')
        assert rows == expected

    def assert_employees(self, archive):
        self.assert_content(
            archive,
            'employees',
            {
                b'id,first_name,last_name,manager_id,referrer_id,group_id',
                b'5,John,Snow,3,4,2',
                b'4,John,Brown,3,,2',
                b'3,John,Smith,1,,1',
                b'1,John,Doe,,,1',
            }
        )

    def assert_groups(self, archive):
        self.assert_content(archive, 'groups', {b'id,name', b'1,Admin', b'2,User'})


class PostgreSQLWrapper(BackendWrapper):

    @property
    def is_search_path_fixed(self):
        if not hasattr(self, '_is_search_path_fixed'):
            connection = self.backend.get_connection()
            self._is_search_path_fixed = is_search_path_fixed(connection)
        return self._is_search_path_fixed

    def assert_schema(self, schema):
        if self.is_search_path_fixed:
            template = 'CREATE TABLE public.{0}'
        else:
            template = 'CREATE TABLE {0}'
        for table in ('groups', 'employees', 'tickets'):
            assert template.format(table).encode() in schema

    def is_database_exists(self, dbname):
        return self.backend.run(
            'SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = %s)', [dbname]
        )[0]['exists']

    def assert_namelist(self, archive):
        assert archive.namelist() == [
            'dump/schema.sql', 'dump/sequences.sql', 'dump/data/groups.csv', 'dump/data/employees.csv',
        ]

    def assert_unused_sequences(self, archive):
        if self.is_search_path_fixed:
            string = "SELECT pg_catalog.setval('public.groups_id_seq', 1, false);"
        else:
            string = "SELECT pg_catalog.setval('groups_id_seq', 1, false);"
        assert string.encode() in archive.read('dump/sequences.sql')

    def get_tables_count(self):
        return self.backend.run(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename IN ('groups', 'employees', 'tickets')"
        )[0]['count']

    def get_new_database_name(self):
        return 'test_xxx'


class SQLiteWrapper(BackendWrapper):

    def is_database_exists(self, dbname):
        return Path(dbname).exists()

    def assert_namelist(self, archive):
        assert archive.namelist() == ['dump/schema.sql', 'dump/data/groups.csv', 'dump/data/employees.csv']

    def get_tables_count(self):
        return self.backend.run(
            "SELECT COUNT(*) AS 'count' "
            "FROM sqlite_master WHERE type='table' AND name IN ('groups', 'employees', 'tickets')"
        )[0]['count']

    def get_new_database_name(self):
        tmpdir = self.request.getfixturevalue('tmpdir')
        return str(tmpdir.join('test_xxx.db'))

    def _get_concurrent_insert_class(self, query):
        base_class = super()._get_concurrent_insert_class(query)

        class ConcurrentInsertEmulator(base_class):

            def _insert(self):
                with pytest.raises(sqlite3.OperationalError, message='database is locked'):
                    return super()._insert()

        return ConcurrentInsertEmulator


@pytest.fixture
def backend(request):
    if IS_POSTGRES:
        from xdump.postgresql import PostgreSQLBackend

        postgresql = request.getfixturevalue('postgresql')
        if platform.python_implementation() == 'Pypy' and sys.version_info[0] == 3:
            parameters = dict(item.split('=') for item in postgresql.dsn.split())
        else:
            parameters = postgresql.get_dsn_parameters()
        return PostgreSQLBackend(
            dbname=parameters['dbname'],
            user=parameters['user'],
            password=None,
            host=parameters['host'],
            port=parameters['port'],
        )
    elif IS_SQLITE:
        if sqlite3.sqlite_version_info < (3, 8, 3):
            pytest.skip('Unsupported SQLite version')
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
def db_helper(request, backend):
    return {
        'postgres': PostgreSQLWrapper,
        'sqlite': SQLiteWrapper,
    }[DATABASE](backend=backend, request=request)


@pytest.fixture
def cursor(request):
    if IS_POSTGRES:
        from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

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
        pytest.skip('Cannot run on {0}'.format(DATABASE))


@pytest.fixture
def archive_filename(tmpdir):
    return str(tmpdir.join('dump.zip'))


@pytest.fixture
def archive(archive_filename):
    with zipfile.ZipFile(archive_filename, 'w', zipfile.ZIP_DEFLATED) as file:
        yield file
