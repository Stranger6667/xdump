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


def assert_schema(schema, db_wrapper):
    assert b'Dumped from database version 10.1' in schema
    assert b'CREATE TABLE groups' in schema
    selectable_tables = db_wrapper.get_selectable_tables()
    for table in selectable_tables:
        assert f'COPY {table}'.encode() not in schema


def test_pg_dump(db_wrapper):
    output = db_wrapper.pg_dump()
    assert b'Dumped from database version 10.1' in output
    assert b'CREATE TABLE groups' in output
    selectable_tables = db_wrapper.get_selectable_tables()
    for table in selectable_tables:
        assert f'COPY {table}'.encode() in output


def test_get_selectable_tables(db_wrapper):
    assert db_wrapper.get_selectable_tables() == ['groups', 'employees', 'tickets']


def test_dump_schema(db_wrapper):
    """
    Schema should not include any COPY statements.
    """
    output = db_wrapper.dump_schema()
    assert_schema(output, db_wrapper)


def test_get_sequences(db_wrapper):
    assert db_wrapper.get_sequences() == ['groups_id_seq', 'employees_id_seq', 'tickets_id_seq']


@pytest.mark.parametrize('sql, expected', (
    ('INSERT INTO groups (name) VALUES (\'test\')', 1),
    ('INSERT INTO groups (name) VALUES (\'test\'), (\'test2\')', 2),
))
def test_dump_sequences(db_wrapper, cursor, sql, expected):
    cursor.execute(sql)
    assert f"SELECT pg_catalog.setval('groups_id_seq', {expected}, true);".encode() in db_wrapper.dump_sequences()


@pytest.mark.parametrize('sql, expected', (
    ('', b'id,name\n'),
    ('INSERT INTO groups (name) VALUES (\'test\')', b'id,name\n1,test\n'),
))
def test_export_to_csv(db_wrapper, cursor, sql, expected):
    if sql:
        cursor.execute(sql)
    assert db_wrapper.export_to_csv('SELECT * FROM groups') == expected


@pytest.fixture
def archive_filename(tmpdir):
    return str(tmpdir.join('dump.zip'))


class TestDump:

    @pytest.fixture
    def archive(self, archive_filename):
        with zipfile.ZipFile(archive_filename, 'w', zipfile.ZIP_DEFLATED) as file:
            yield file

    def test_write_schema(self, db_wrapper, archive):
        db_wrapper.write_schema(archive)
        schema = archive.read('dump/schema.sql')
        assert_schema(schema, db_wrapper)

    def test_write_sequences(self, db_wrapper, archive):
        db_wrapper.write_sequences(archive)
        assert "SELECT pg_catalog.setval('groups_id_seq', 1, false);".encode() in archive.read('dump/sequences.sql')

    def test_write_full_tables(self, db_wrapper, archive):
        db_wrapper.write_full_tables(archive, ['groups'])
        assert archive.read('dump/data/groups.csv') == b'id,name\n'
        assert archive.namelist() == ['dump/data/groups.csv']

    def test_pg_dump_environment(self, db_wrapper):
        db_wrapper.password = 'PASSW'
        assert db_wrapper.pg_dump_environment['PGPASSWORD'] == db_wrapper.password

    def test_pg_dump_environment_empty_password(self, db_wrapper):
        assert 'PGPASSWORD' not in db_wrapper.pg_dump_environment

    def test_dump(self, db_wrapper, archive_filename):
        db_wrapper.dump(archive_filename, ['groups'], {'employees': EMPLOYEES_SQL})
        archive = zipfile.ZipFile(archive_filename)
        assert archive.namelist() == [
            'dump/schema.sql', 'dump/sequences.sql', 'dump/data/groups.csv', 'dump/data/employees.csv',
        ]
        assert archive.read('dump/data/groups.csv') == b'id,name\n'
        assert archive.read('dump/data/employees.csv') == b'id,first_name,last_name,manager_id,group_id\n'
        assert "SELECT pg_catalog.setval('groups_id_seq', 1, false);".encode() in archive.read('dump/sequences.sql')
        schema = archive.read('dump/schema.sql')
        assert_schema(schema, db_wrapper)

    def test_transaction(self, db_wrapper, cursor, archive_filename):
        is_added = False

        def add_group(*args, **kwargs):
            nonlocal is_added
            if not is_added:
                cursor.execute('INSERT INTO groups (name) VALUES (\'test\')')
                is_added = True

        with patch.object(db_wrapper, 'export_to_csv', wraps=db_wrapper.export_to_csv, side_effect=add_group):
            db_wrapper.dump(archive_filename, ['employees', 'groups'])
            archive = zipfile.ZipFile(archive_filename)
            assert archive.read('dump/data/groups.csv') == b'id,name\n'

    @pytest.mark.usefixtures('schema', 'data')
    def test_write_partial_tables(self, db_wrapper, archive):
        """
        Here we need to select two latest employees with all related managers.
        In that case - John Black will not be in the output.
        """

        db_wrapper.write_partial_tables(archive, {'employees': EMPLOYEES_SQL})
        assert archive.read('dump/data/employees.csv') == b'id,first_name,last_name,manager_id,group_id\n' \
                                                          b'5,John,Snow,3,2\n' \
                                                          b'4,John,Brown,3,2\n' \
                                                          b'3,John,Smith,1,1\n' \
                                                          b'1,John,Doe,,1\n'


class TestRecreating:

    @pytest.fixture
    def archive(self, db_wrapper, archive_filename):
        db_wrapper.dump(archive_filename, ['groups'], {'employees': EMPLOYEES_SQL})
        db_wrapper.recreate_database()
        return zipfile.ZipFile(archive_filename)

    def is_database_exists(self, db_wrapper, dbname):
        return db_wrapper.run(
            'SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = %s)', [dbname]
        )[0]['exists']

    def test_drop_database(self, db_wrapper):
        original_dbname = db_wrapper.dbname
        db_wrapper.dbname = 'postgres'
        db_wrapper.drop_connections(original_dbname)
        db_wrapper.drop_database(original_dbname)
        assert not self.is_database_exists(db_wrapper, original_dbname)

    def test_create_database(self, db_wrapper):
        dbname = 'test_xxx'
        db_wrapper.create_database(dbname, db_wrapper.user)
        assert self.is_database_exists(db_wrapper, dbname)

    @pytest.mark.usefixtures('schema', 'data')
    def test_recreate_database(self, db_wrapper):
        db_wrapper.recreate_database()
        assert self.is_database_exists(db_wrapper, db_wrapper.dbname)

    @pytest.mark.usefixtures('schema', 'data')
    def test_populate_database(self, db_wrapper, archive):
        db_wrapper.populate_database(archive.filename)
        result = db_wrapper.run("SELECT COUNT(*) FROM pg_tables WHERE tablename IN ('groups', 'employees', 'tickets')")
        assert result[0]['count'] == 3
        result = db_wrapper.run("SELECT last_value FROM pg_sequences WHERE sequencename = 'groups_id_seq'")
        assert result[0]['last_value'] == 2
        assert db_wrapper.run('SELECT name FROM groups') == [{'name': 'Admin'}, {'name': 'User'}]
