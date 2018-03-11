# coding: utf-8
import zipfile

import pytest

from .conftest import DATABASE, EMPLOYEES_SQL


pytestmark = pytest.mark.usefixtures('schema')


def test_dump_schema(backend, db_helper):
    """
    Schema should not include any COPY statements.
    """
    output = backend.dump_schema()
    db_helper.assert_schema(output)


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

    def test_drop_database(self, backend, db_helper):
        original_dbname = backend.dbname
        backend.dbname = 'postgres'
        if DATABASE == 'postgres':
            backend.drop_connections(original_dbname)
        backend.drop_database(original_dbname)
        assert not db_helper.is_database_exists(original_dbname)

    def test_create_database(self, backend, db_helper):
        dbname = db_helper.get_new_database_name()
        backend.create_database(dbname, backend.user)
        assert db_helper.is_database_exists(dbname)

    @pytest.mark.usefixtures('schema', 'data')
    def test_recreate_database(self, backend, db_helper):
        backend.recreate_database()
        assert db_helper.is_database_exists(backend.dbname)

    def test_non_existent_db(self, backend, db_helper):
        assert backend.drop_database('not_exists') is None
        assert not db_helper.is_database_exists('not_exists')


class TestHighLevelInterface:
    """
    Creating a dump and loading it.
    """

    @pytest.fixture
    def dump(self, backend, archive_filename, data):
        backend.dump(archive_filename, ['groups'], {'employees': EMPLOYEES_SQL})

    @pytest.mark.usefixtures('schema', 'dump')
    def test_dump(self, db_helper, archive_filename):
        db_helper.assert_dump(archive_filename)

    @pytest.mark.usefixtures('schema', 'data')
    def test_transaction(self, backend, archive_filename, db_helper):
        """
        We add extra values to the second table after first table was dumped.
        This data should not appear in the result.
        """
        insert = 'INSERT INTO groups (id, name) VALUES (3,\'test\')'

        with db_helper.concurrent_insert(insert):
            backend.dump(archive_filename, ['employees', 'groups'], {})
            archive = zipfile.ZipFile(archive_filename)
            db_helper.assert_groups(archive)
        if DATABASE == 'sqlite':
            backend.run(insert)
        else:
            backend.cache_clear()
        assert backend.run('SELECT COUNT(*) AS "count" FROM groups')[0]['count'] == 3

    @pytest.mark.usefixtures('schema', 'data', 'dump')
    def test_load(self, backend, archive_filename, db_helper):
        backend.recreate_database()
        backend.load(archive_filename)
        assert db_helper.get_tables_count() == 3
        assert backend.run('SELECT name FROM groups') == [{'name': 'Admin'}, {'name': 'User'}]
        if DATABASE == 'postgres':
            result = backend.run("SELECT currval('groups_id_seq')")
            assert result[0]['currval'] == 2


def test_write_schema(backend, db_helper, archive):
    backend.write_schema(archive)
    schema = archive.read('dump/schema.sql')
    db_helper.assert_schema(schema)


@pytest.mark.usefixtures('schema', 'data')
def test_write_partial_tables(backend, archive, db_helper):
    """
    Here we need to select two latest employees with all related managers.
    In that case - John Black will not be in the output.
    """
    backend.write_partial_tables(archive, {'employees': EMPLOYEES_SQL})
    db_helper.assert_employees(archive)


@pytest.mark.usefixtures('schema', 'data')
def test_write_full_tables(backend, archive, db_helper):
    backend.write_full_tables(archive, ['groups'])
    db_helper.assert_groups(archive)
    assert archive.namelist() == ['dump/data/groups.csv']


@pytest.mark.usefixtures('schema', 'data')
class TestAutoSelect:

    def assert_employee(self, archive):
        assert archive.read(
            'dump/data/employees.csv'
        ) == b'id,first_name,last_name,manager_id,group_id\n1,John,Doe,,1\n'

    def assert_groups(self, archive):
        assert archive.read('dump/data/groups.csv') == b'id,name\n1,Admin\n'

    def test_related_table(self, backend, archive_filename):
        backend.dump(archive_filename, [], {'employees': 'SELECT * FROM employees WHERE id = 1'})
        archive = zipfile.ZipFile(archive_filename)
        self.assert_employee(archive)
        self.assert_groups(archive)

    def test_full_tables_handling(self, backend, db_helper, archive_filename):
        backend.dump(archive_filename, ['groups'], {'employees': 'SELECT * FROM employees WHERE id = 1'})
        archive = zipfile.ZipFile(archive_filename)
        self.assert_employee(archive)
        db_helper.assert_groups(archive)

    def test_long_relation(self, backend, archive_filename):
        backend.dump(archive_filename, [], {'tickets': 'SELECT * FROM tickets WHERE id = 1'})
        archive = zipfile.ZipFile(archive_filename)
        assert archive.read('dump/data/tickets.csv') == b'id,author_id,subject,message\n1,1,Sub 1,Message 1\n'
        self.assert_employee(archive)
        self.assert_groups(archive)

    def test_related_to_full(self, backend, archive_filename, db_helper):
        backend.dump(archive_filename, ['employees'], {})
        archive = zipfile.ZipFile(archive_filename)
        db_helper.assert_groups(archive)

    def test_recursive_relation(self, backend, archive_filename):
        backend.dump(archive_filename, [], {'employees': 'SELECT * FROM employees WHERE id = 2'})
        archive = zipfile.ZipFile(archive_filename)
        assert archive.read(
            'dump/data/employees.csv'
        ) == b'id,first_name,last_name,manager_id,group_id\n2,John,Black,1,1\n1,John,Doe,,1\n'
        self.assert_groups(archive)

    def test_long_recursive_relation(self, backend, archive_filename):
        backend.dump(archive_filename, [], {'tickets': 'SELECT * FROM tickets WHERE id = 2'})
        archive = zipfile.ZipFile(archive_filename)
        assert archive.read('dump/data/tickets.csv') == b'id,author_id,subject,message\n2,2,Sub 2,Message 2\n'
        assert archive.read(
            'dump/data/employees.csv'
        ) == b'id,first_name,last_name,manager_id,group_id\n2,John,Black,1,1\n1,John,Doe,,1\n'
        self.assert_groups(archive)
        self.assert_groups(archive)

    # TODO. Test multiple recursive relations
