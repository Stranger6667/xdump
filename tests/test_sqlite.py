# coding: utf-8
import sqlite3
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest

from xdump.sqlite import SQLiteBackend

from .conftest import EMPLOYEES_SQL


CURRENT_DIR = Path(__file__).parent.absolute()


@pytest.fixture
def dbname(tmpdir):
    return str(tmpdir.join('test.db'))


@pytest.fixture
def cursor(dbname):
    connection = sqlite3.connect(dbname, isolation_level=None)
    return connection.cursor()


def execute_file(cursor, filename):
    with (CURRENT_DIR / filename).open('r') as fd:
        sql = fd.read()
    cursor.executescript(sql)
    return sql


@pytest.fixture
def schema(cursor):
    execute_file(cursor, 'sql/schema.sql')


@pytest.fixture
def data(cursor):
    execute_file(cursor, 'sql/sqlite_data.sql')


@pytest.fixture
def sqlite_backend(dbname):
    return SQLiteBackend(
        dbname=dbname,
        user=None,
        password=None,
        host=None,
        port=None,
    )


pytestmark = pytest.mark.usefixtures('schema')


def assert_schema(schema):
    for table in ('groups', 'employees', 'tickets'):
        assert f'CREATE TABLE {table}'.encode() in schema


def test_dump_schema(sqlite_backend):
    schema = sqlite_backend.dump_schema()
    assert_schema(schema)


def test_write_schema(sqlite_backend, archive):
    sqlite_backend.write_schema(archive)
    schema = archive.read('dump/schema.sql')
    assert_schema(schema)


@pytest.mark.parametrize('sql, expected', (
    ('', b'id,name\n1,Admin\n2,User\n'),
    ('INSERT INTO groups (id, name) VALUES (3, \'test\')', b'id,name\n1,Admin\n2,User\n3,test\n'),
))
@pytest.mark.usefixtures('schema', 'data')
def test_export_to_csv(sqlite_backend, cursor, sql, expected):
    if sql:
        cursor.execute(sql)
    assert sqlite_backend.export_to_csv('SELECT * FROM groups') == expected


@pytest.mark.usefixtures('schema', 'data')
def test_write_partial_tables(sqlite_backend, archive):
    """
    Here we need to select two latest employees with all related managers.
    In that case - John Black will not be in the output.
    """
    sqlite_backend.write_partial_tables(archive, {'employees': EMPLOYEES_SQL})
    assert archive.read('dump/data/employees.csv') == b'id,first_name,last_name,manager_id,group_id\n' \
                                                      b'5,John,Snow,3,2\n' \
                                                      b'4,John,Brown,3,2\n' \
                                                      b'3,John,Smith,1,1\n' \
                                                      b'1,John,Doe,,1\n'


@pytest.mark.usefixtures('schema', 'data')
def test_write_full_tables(sqlite_backend, archive):
    sqlite_backend.write_full_tables(archive, ['groups'])
    assert archive.read('dump/data/groups.csv') == b'id,name\n1,Admin\n2,User\n'
    assert archive.namelist() == ['dump/data/groups.csv']


class TestRecreating:

    def is_database_exists(self, dbname):
        return Path(dbname).exists()

    def test_drop_database(self, sqlite_backend, dbname):
        sqlite_backend.drop_database(dbname)
        assert not self.is_database_exists(dbname)

    def test_non_existent_db(self, sqlite_backend):
        assert sqlite_backend.drop_database('not_exists') is None
        assert not self.is_database_exists('not_exists')

    def test_create_database(self, tmpdir, sqlite_backend):
        dbname = str(tmpdir.join('test_xxx.db'))
        sqlite_backend.create_database(dbname)
        assert self.is_database_exists(dbname)

    @pytest.mark.usefixtures('schema', 'data')
    def test_recreate_database(self, sqlite_backend, dbname):
        sqlite_backend.recreate_database()
        assert self.is_database_exists(dbname)


class TestHighLevelInterface:
    """
    Creating a dump and loading it.
    """

    @pytest.fixture
    def dump(self, sqlite_backend, archive_filename):
        sqlite_backend.dump(archive_filename, ['groups'], {'employees': EMPLOYEES_SQL})

    @pytest.mark.usefixtures('schema', 'data', 'dump')
    def test_dump(self, archive_filename):
        archive = zipfile.ZipFile(archive_filename)
        assert archive.namelist() == ['dump/schema.sql', 'dump/data/groups.csv', 'dump/data/employees.csv']
        assert archive.read('dump/data/groups.csv') == b'id,name\n1,Admin\n2,User\n'
        assert archive.read('dump/data/employees.csv') == b'id,first_name,last_name,manager_id,group_id\n' \
                                                          b'5,John,Snow,3,2\n' \
                                                          b'4,John,Brown,3,2\n' \
                                                          b'3,John,Smith,1,1\n' \
                                                          b'1,John,Doe,,1\n'
        schema = archive.read('dump/schema.sql')
        assert_schema(schema)

    @pytest.mark.usefixtures('schema', 'data')
    def test_transaction(self, sqlite_backend, cursor, archive_filename):
        """
        We add extra values to the second table after first table was dumped.
        This data should not appear in the result.
        """
        insert = 'INSERT INTO groups (id, name) VALUES (3,\'test\')'

        class Data:
            is_loaded = False

            def insert(self, *args, **kwargs):
                if not self.is_loaded:
                    with pytest.raises(sqlite3.OperationalError, message='database is locked'):
                        cursor.execute(insert)
                    self.is_loaded = True

        with patch.object(sqlite_backend, 'export_to_csv', wraps=sqlite_backend.export_to_csv,
                          side_effect=Data().insert):
            sqlite_backend.dump(archive_filename, ['employees', 'groups'], {})
            archive = zipfile.ZipFile(archive_filename)
            assert archive.read('dump/data/groups.csv') == b'id,name\n1,Admin\n2,User\n'
        sqlite_backend.run(insert)
        assert sqlite_backend.run('SELECT COUNT(*) AS "count" FROM groups')[0]['count'] == 3

    @pytest.mark.usefixtures('schema', 'data', 'dump')
    def test_load(self, sqlite_backend, archive_filename):
        sqlite_backend.recreate_database()
        sqlite_backend.load(archive_filename)
        result = sqlite_backend.run(
            "SELECT COUNT(*) AS 'count' "
            "FROM sqlite_master WHERE type='table' AND name IN ('groups', 'employees', 'tickets')"
        )
        assert result[0]['count'] == 3
        assert sqlite_backend.run('SELECT name FROM groups') == [{'name': 'Admin'}, {'name': 'User'}]
