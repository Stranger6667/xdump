import zipfile
from unittest.mock import patch

import pytest


pytestmark = pytest.mark.usefixtures('schema')


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


def assert_schema(schema, dumper):
    assert b'Dumped from database version 10.1' in schema
    assert b'CREATE TABLE groups' in schema
    selectable_tables = dumper.get_selectable_tables()
    for table in selectable_tables:
        assert f'COPY {table}'.encode() not in schema


def test_pg_dump(dumper):
    output = dumper.pg_dump()
    assert b'Dumped from database version 10.1' in output
    assert b'CREATE TABLE groups' in output
    selectable_tables = dumper.get_selectable_tables()
    for table in selectable_tables:
        assert f'COPY {table}'.encode() in output


def test_get_selectable_tables(dumper):
    assert dumper.get_selectable_tables() == ['groups', 'employees', 'tickets']


def test_dump_schema(dumper):
    """
    Schema should not include any COPY statements.
    """
    output = dumper.dump_schema()
    assert_schema(output, dumper)


def test_get_sequences(dumper):
    assert dumper.get_sequences() == ['groups_id_seq', 'employees_id_seq', 'tickets_id_seq']


@pytest.mark.parametrize('sql, expected', (
    ('INSERT INTO groups (name) VALUES (\'test\')', 1),
    ('INSERT INTO groups (name) VALUES (\'test\'), (\'test2\')', 2),
))
def test_dump_sequences(dumper, cursor, sql, expected):
    cursor.execute(sql)
    assert f"SELECT pg_catalog.setval('groups_id_seq', {expected}, true);".encode() in dumper.dump_sequences()


@pytest.mark.parametrize('sql, expected', (
    ('', b'id,name\n'),
    ('INSERT INTO groups (name) VALUES (\'test\')', b'id,name\n1,test\n'),
))
def test_export_to_csv(dumper, cursor, sql, expected):
    if sql:
        cursor.execute(sql)
    assert dumper.export_to_csv('SELECT * FROM groups') == expected


@pytest.fixture
def archive_filename(tmpdir):
    return str(tmpdir.join('dump.zip'))


class TestDump:

    @pytest.fixture
    def archive(self, archive_filename):
        with zipfile.ZipFile(archive_filename, 'w', zipfile.ZIP_DEFLATED) as file:
            yield file

    def test_write_schema(self, dumper, archive):
        dumper.write_schema(archive)
        schema = archive.read('dump/schema.sql')
        assert_schema(schema, dumper)

    def test_write_sequences(self, dumper, archive):
        dumper.write_sequences(archive)
        assert "SELECT pg_catalog.setval('groups_id_seq', 1, false);".encode() in archive.read('dump/sequences.sql')

    def test_write_full_tables(self, dumper, archive):
        dumper.write_full_tables(archive, ['groups'])
        assert archive.read('dump/data/groups.csv') == b'id,name\n'
        assert archive.namelist() == ['dump/data/groups.csv']

    def test_pg_dump_environment(self, dumper):
        dumper.password = 'PASSW'
        assert dumper.pg_dump_environment['PGPASSWORD'] == dumper.password

    def test_pg_dump_environment_empty_password(self, dumper):
        assert 'PGPASSWORD' not in dumper.pg_dump_environment

    def test_dump(self, dumper, archive_filename):
        dumper.dump(archive_filename, ['groups'], {'employees': EMPLOYEES_SQL})
        archive = zipfile.ZipFile(archive_filename)
        assert archive.namelist() == [
            'dump/schema.sql', 'dump/sequences.sql', 'dump/data/groups.csv', 'dump/data/employees.csv',
        ]
        assert archive.read('dump/data/groups.csv') == b'id,name\n'
        assert archive.read('dump/data/employees.csv') == b'id,first_name,last_name,manager_id,group_id\n'
        assert "SELECT pg_catalog.setval('groups_id_seq', 1, false);".encode() in archive.read('dump/sequences.sql')
        schema = archive.read('dump/schema.sql')
        assert_schema(schema, dumper)

    def test_transaction(self, dumper, cursor, archive_filename):
        is_added = False

        def add_group(*args, **kwargs):
            nonlocal is_added
            if not is_added:
                cursor.execute('INSERT INTO groups (name) VALUES (\'test\')')
                is_added = True

        with patch.object(dumper, 'export_to_csv', wraps=dumper.export_to_csv, side_effect=add_group):
            dumper.dump(archive_filename, ['employees', 'groups'])
            archive = zipfile.ZipFile(archive_filename)
            assert archive.read('dump/data/groups.csv') == b'id,name\n'

    @pytest.mark.usefixtures('schema', 'data')
    def test_write_partial_tables(self, dumper, archive):
        """
        Here we need to select two latest employees with all related managers.
        In that case - John Black will not be in the output.
        """

        dumper.write_partial_tables(archive, {'employees': EMPLOYEES_SQL})
        assert archive.read('dump/data/employees.csv') == b'id,first_name,last_name,manager_id,group_id\n' \
                                                          b'5,John,Snow,3,2\n' \
                                                          b'4,John,Brown,3,2\n' \
                                                          b'3,John,Smith,1,1\n' \
                                                          b'1,John,Doe,,1\n'


class TestRecreating:

    @pytest.fixture
    def archive(self, dumper, archive_filename):
        dumper.dump(archive_filename, ['groups'], {'employees': EMPLOYEES_SQL})
        dumper.recreate_database()
        return zipfile.ZipFile(archive_filename)

    def is_database_exists(self, dumper, dbname):
        return dumper.run(
            'SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = %s)', [dbname]
        )[0]['exists']

    def test_drop_database(self, dumper):
        original_dbname = dumper.dbname
        dumper.dbname = 'postgres'
        dumper.drop_connections(original_dbname)
        dumper.drop_database(original_dbname)
        assert not self.is_database_exists(dumper, original_dbname)

    def test_create_database(self, dumper):
        dbname = 'test_xxx'
        dumper.create_database(dbname, dumper.user)
        assert self.is_database_exists(dumper, dbname)

    @pytest.mark.usefixtures('schema', 'data')
    def test_recreate_database(self, dumper):
        dumper.recreate_database()
        assert self.is_database_exists(dumper, dumper.dbname)

    @pytest.mark.usefixtures('schema', 'data')
    def test_populate_database(self, dumper, archive):
        dumper.populate_database(archive.filename)
        result = dumper.run("SELECT COUNT(*) FROM pg_tables WHERE tablename IN ('groups', 'employees', 'tickets')")
        assert result[0]['count'] == 3
        result = dumper.run("SELECT last_value FROM pg_sequences WHERE sequencename = 'groups_id_seq'")
        assert result[0]['last_value'] == 2
        assert dumper.run('SELECT name FROM groups') == [{'name': 'Admin'}, {'name': 'User'}]
