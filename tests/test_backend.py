# coding: utf-8
import sqlite3
import zipfile
from pathlib import Path
from unittest.mock import patch

import psycopg2
import pytest

from .conftest import EMPLOYEES_SQL, IS_POSTGRES, IS_SQLITE, assert_schema, assert_unused_sequences


pytestmark = pytest.mark.usefixtures('schema')


class TestRunDump:

    @pytest.mark.postgres
    def test_run_dump(self, backend):
        schema = backend.run_dump()
        assert_schema(schema, True)

    @pytest.mark.postgres
    def test_run_dump_environment(self, backend):
        backend.password = 'PASSW'
        assert backend.run_dump_environment['PGPASSWORD'] == backend.password

    @pytest.mark.postgres
    def test_run_dump_environment_empty_password(self, backend):
        assert 'PGPASSWORD' not in backend.run_dump_environment

    def test_dump_schema(self, backend):
        """
        Schema should not include any COPY statements.
        """
        output = backend.dump_schema()
        assert_schema(output)


@pytest.mark.postgres
def test_get_sequences(backend):
    assert backend.get_sequences() == ['groups_id_seq', 'employees_id_seq', 'tickets_id_seq']


@pytest.mark.parametrize('sql, expected', (
    ('INSERT INTO groups (name) VALUES (\'test\')', 1),
    ('INSERT INTO groups (name) VALUES (\'test\'), (\'test2\')', 2),
))
@pytest.mark.postgres
def test_dump_sequences(backend, cursor, sql, expected):
    cursor.execute(sql)
    assert f"SELECT pg_catalog.setval('groups_id_seq', {expected}, true);".encode() in backend.dump_sequences()


@pytest.mark.postgres
def test_handling_error(backend):
    with patch('psycopg2.extras.DictCursorBase.fetchall', side_effect=psycopg2.ProgrammingError), \
            pytest.raises(psycopg2.ProgrammingError):
        backend.run('BEGIN')


@pytest.mark.parametrize('sql, expected', (
    ('', b'id,name\n1,Admin\n2,User\n'),
    ('INSERT INTO groups (id, name) VALUES (3, \'test\')', b'id,name\n1,Admin\n2,User\n3,test\n'),
))
@pytest.mark.usefixtures('schema', 'data')
def test_export_to_csv(backend, cursor, sql, expected):
    if sql:
        cursor.execute(sql)
    assert backend.export_to_csv('SELECT * FROM groups') == expected


class TestRecreating:

    def is_database_exists(self, backend, dbname):
        if IS_POSTGRES:
            return backend.run(
                'SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = %s)', [dbname]
            )[0]['exists']
        elif IS_SQLITE:
            return Path(dbname).exists()

    def test_drop_database(self, backend):
        original_dbname = backend.dbname
        backend.dbname = 'postgres'
        if IS_POSTGRES:
            backend.drop_connections(original_dbname)
        backend.drop_database(original_dbname)
        assert not self.is_database_exists(backend, original_dbname)

    def test_create_database(self, backend, tmpdir):
        if IS_POSTGRES:
            dbname = 'test_xxx'
        elif IS_SQLITE:
            dbname = str(tmpdir.join('test_xxx.db'))
        backend.create_database(dbname, backend.user)
        assert self.is_database_exists(backend, dbname)

    @pytest.mark.usefixtures('schema', 'data')
    def test_recreate_database(self, backend):
        backend.recreate_database()
        assert self.is_database_exists(backend, backend.dbname)

    def test_non_existent_db(self, backend):
        assert backend.drop_database('not_exists') is None
        assert not self.is_database_exists(backend, 'not_exists')


class TestHighLevelInterface:
    """
    Creating a dump and loading it.
    """

    @pytest.fixture
    def dump(self, backend, archive_filename):
        backend.dump(archive_filename, ['groups'], {'employees': EMPLOYEES_SQL})

    @pytest.mark.usefixtures('schema', 'dump')
    def test_dump(self, archive_filename):
        archive = zipfile.ZipFile(archive_filename)
        namelist = archive.namelist()
        if IS_POSTGRES:
            assert namelist == [
                'dump/schema.sql', 'dump/sequences.sql', 'dump/data/groups.csv', 'dump/data/employees.csv',
            ]
        elif IS_SQLITE:
            assert namelist == ['dump/schema.sql', 'dump/data/groups.csv', 'dump/data/employees.csv']
        assert archive.read('dump/data/groups.csv') == b'id,name\n'
        assert archive.read('dump/data/employees.csv') == b'id,first_name,last_name,manager_id,group_id\n'
        if IS_POSTGRES:
            assert_unused_sequences(archive)
        schema = archive.read('dump/schema.sql')
        assert_schema(schema)

    @pytest.mark.usefixtures('schema', 'data')
    def test_transaction(self, backend, cursor, archive_filename):
        """
        We add extra values to the second table after first table was dumped.
        This data should not appear in the result.
        """
        insert = 'INSERT INTO groups (id, name) VALUES (3,\'test\')'

        class Data:
            is_loaded = False

            def insert(self, *args, **kwargs):
                if not self.is_loaded:
                    if IS_POSTGRES:
                        cursor.execute(insert)
                    elif IS_SQLITE:
                        with pytest.raises(sqlite3.OperationalError, message='database is locked'):
                            cursor.execute(insert)
                    self.is_loaded = True

        with patch.object(backend, 'export_to_csv', wraps=backend.export_to_csv,
                          side_effect=Data().insert):
            backend.dump(archive_filename, ['employees', 'groups'], {})
            archive = zipfile.ZipFile(archive_filename)
            assert archive.read('dump/data/groups.csv') == b'id,name\n1,Admin\n2,User\n'
        backend.get_cursor.cache_clear()
        backend.get_connection.cache_clear()
        if IS_SQLITE:
            backend.run(insert)
        assert backend.run('SELECT COUNT(*) AS "count" FROM groups')[0]['count'] == 3

    @pytest.mark.usefixtures('schema', 'data', 'dump')
    def test_load(self, backend, archive_filename):
        backend.recreate_database()
        backend.load(archive_filename)
        if IS_POSTGRES:
            result = backend.run(
                "SELECT COUNT(*) FROM pg_tables WHERE tablename IN ('groups', 'employees', 'tickets')"
            )
        elif IS_SQLITE:
            result = backend.run(
                "SELECT COUNT(*) AS 'count' "
                "FROM sqlite_master WHERE type='table' AND name IN ('groups', 'employees', 'tickets')"
            )
        assert result[0]['count'] == 3
        assert backend.run('SELECT name FROM groups') == [{'name': 'Admin'}, {'name': 'User'}]
        if IS_POSTGRES:
            result = backend.run("SELECT currval('groups_id_seq')")
            assert result[0]['currval'] == 2


@pytest.mark.postgres
def test_write_sequences(backend, archive):
    backend.write_sequences(archive)
    assert_unused_sequences(archive)


def test_write_schema(backend, archive):
    backend.write_schema(archive)
    schema = archive.read('dump/schema.sql')
    assert_schema(schema)


@pytest.mark.usefixtures('schema', 'data')
def test_write_partial_tables(backend, archive):
    """
    Here we need to select two latest employees with all related managers.
    In that case - John Black will not be in the output.
    """
    backend.write_partial_tables(archive, {'employees': EMPLOYEES_SQL})
    assert archive.read('dump/data/employees.csv') == b'id,first_name,last_name,manager_id,group_id\n' \
                                                      b'5,John,Snow,3,2\n' \
                                                      b'4,John,Brown,3,2\n' \
                                                      b'3,John,Smith,1,1\n' \
                                                      b'1,John,Doe,,1\n'


@pytest.mark.usefixtures('schema', 'data')
def test_write_full_tables(backend, archive):
    backend.write_full_tables(archive, ['groups'])
    assert archive.read('dump/data/groups.csv') == b'id,name\n1,Admin\n2,User\n'
    assert archive.namelist() == ['dump/data/groups.csv']
