# coding: utf-8
import zipfile
from unittest.mock import patch

import psycopg2
import pytest

from .conftest import EMPLOYEES_SQL, assert_schema, assert_unused_sequences


pytestmark = pytest.mark.usefixtures('schema')


class TestRunDump:

    def test_run_dump(self, postgres_backend):
        schema = postgres_backend.run_dump()
        assert_schema(schema, True)

    def test_run_dump_environment(self, postgres_backend):
        postgres_backend.password = 'PASSW'
        assert postgres_backend.run_dump_environment['PGPASSWORD'] == postgres_backend.password

    def test_run_dump_environment_empty_password(self, postgres_backend):
        assert 'PGPASSWORD' not in postgres_backend.run_dump_environment

    def test_dump_schema(self, postgres_backend):
        """
        Schema should not include any COPY statements.
        """
        output = postgres_backend.dump_schema()
        assert_schema(output)


def test_get_sequences(postgres_backend):
    assert postgres_backend.get_sequences() == ['groups_id_seq', 'employees_id_seq', 'tickets_id_seq']


@pytest.mark.parametrize('sql, expected', (
    ('INSERT INTO groups (name) VALUES (\'test\')', 1),
    ('INSERT INTO groups (name) VALUES (\'test\'), (\'test2\')', 2),
))
def test_dump_sequences(postgres_backend, cursor, sql, expected):
    cursor.execute(sql)
    assert f"SELECT pg_catalog.setval('groups_id_seq', {expected}, true);".encode() in postgres_backend.dump_sequences()


def test_handling_error(postgres_backend):
    with patch('psycopg2.extras.DictCursorBase.fetchall', side_effect=psycopg2.ProgrammingError), \
            pytest.raises(psycopg2.ProgrammingError):
        postgres_backend.run('BEGIN')


@pytest.mark.parametrize('sql, expected', (
    ('', b'id,name\n'),
    ('INSERT INTO groups (name) VALUES (\'test\')', b'id,name\n1,test\n'),
))
def test_export_to_csv(postgres_backend, cursor, sql, expected):
    if sql:
        cursor.execute(sql)
    assert postgres_backend.export_to_csv('SELECT * FROM groups') == expected


class TestRecreating:

    def is_database_exists(self, postgres_backend, dbname):
        return postgres_backend.run(
            'SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = %s)', [dbname]
        )[0]['exists']

    def test_drop_database(self, postgres_backend):
        original_dbname = postgres_backend.dbname
        postgres_backend.dbname = 'postgres'
        postgres_backend.drop_connections(original_dbname)
        postgres_backend.drop_database(original_dbname)
        assert not self.is_database_exists(postgres_backend, original_dbname)

    def test_create_database(self, postgres_backend):
        dbname = 'test_xxx'
        postgres_backend.create_database(dbname, postgres_backend.user)
        assert self.is_database_exists(postgres_backend, dbname)

    @pytest.mark.usefixtures('schema', 'data')
    def test_recreate_database(self, postgres_backend):
        postgres_backend.recreate_database(postgres_backend.user)
        assert self.is_database_exists(postgres_backend, postgres_backend.dbname)


class TestHighLevelInterface:
    """
    Creating a dump and loading it.
    """

    @pytest.fixture
    def dump(self, postgres_backend, archive_filename):
        postgres_backend.dump(archive_filename, ['groups'], {'employees': EMPLOYEES_SQL})

    @pytest.mark.usefixtures('schema', 'dump')
    def test_dump(self, archive_filename):
        archive = zipfile.ZipFile(archive_filename)
        assert archive.namelist() == [
            'dump/schema.sql', 'dump/sequences.sql', 'dump/data/groups.csv', 'dump/data/employees.csv',
        ]
        assert archive.read('dump/data/groups.csv') == b'id,name\n'
        assert archive.read('dump/data/employees.csv') == b'id,first_name,last_name,manager_id,group_id\n'
        assert_unused_sequences(archive)
        schema = archive.read('dump/schema.sql')
        assert_schema(schema)

    def test_transaction(self, postgres_backend, cursor, archive_filename):
        """
        We add extra values to the second table after first table was dumped.
        This data should not appear in the result.
        """

        class Data:
            is_loaded = False

            def insert(self, *args, **kwargs):
                if not self.is_loaded:
                    cursor.execute('INSERT INTO groups (name) VALUES (\'test\')')
                    self.is_loaded = True

        with patch.object(postgres_backend, 'export_to_csv', wraps=postgres_backend.export_to_csv,
                          side_effect=Data().insert):
            postgres_backend.dump(archive_filename, ['employees', 'groups'], {})
            archive = zipfile.ZipFile(archive_filename)
            assert archive.read('dump/data/groups.csv') == b'id,name\n'

    @pytest.mark.usefixtures('schema', 'data', 'dump')
    def test_load(self, postgres_backend, archive_filename):
        postgres_backend.recreate_database(postgres_backend.user)
        postgres_backend.load(archive_filename)
        result = postgres_backend.run(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename IN ('groups', 'employees', 'tickets')"
        )
        assert result[0]['count'] == 3
        result = postgres_backend.run("SELECT last_value FROM pg_sequences WHERE sequencename = 'groups_id_seq'")
        assert result[0]['last_value'] == 2
        assert postgres_backend.run('SELECT name FROM groups') == [{'name': 'Admin'}, {'name': 'User'}]


def test_write_sequences(postgres_backend, archive):
    postgres_backend.write_sequences(archive)
    assert_unused_sequences(archive)


def test_write_schema(postgres_backend, archive):
    postgres_backend.write_schema(archive)
    schema = archive.read('dump/schema.sql')
    assert_schema(schema)


@pytest.mark.usefixtures('schema', 'data')
def test_write_partial_tables(postgres_backend, archive):
    """
    Here we need to select two latest employees with all related managers.
    In that case - John Black will not be in the output.
    """
    postgres_backend.write_partial_tables(archive, {'employees': EMPLOYEES_SQL})
    assert archive.read('dump/data/employees.csv') == b'id,first_name,last_name,manager_id,group_id\n' \
                                                      b'5,John,Snow,3,2\n' \
                                                      b'4,John,Brown,3,2\n' \
                                                      b'3,John,Smith,1,1\n' \
                                                      b'1,John,Doe,,1\n'


@pytest.mark.usefixtures('schema', 'data')
def test_write_full_tables(postgres_backend, archive):
    postgres_backend.write_full_tables(archive, ['groups'])
    assert archive.read('dump/data/groups.csv') == b'id,name\n1,Admin\n2,User\n'
    assert archive.namelist() == ['dump/data/groups.csv']
