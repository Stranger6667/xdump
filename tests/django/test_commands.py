# coding: utf-8
import zipfile

import pytest
from django.core.management import call_command

from xdump.sqlite import SQLiteBackend

from ..conftest import EMPLOYEES_SQL, IS_POSTGRES, IS_SQLITE


pytestmark = pytest.mark.usefixtures('schema', 'data')


@pytest.fixture(autouse=True)
def setup(settings, backend):
    if IS_POSTGRES:
        settings.DATABASES['default']['ENGINE'] = 'django.db.backends.postgresql'
    elif IS_SQLITE:
        settings.DATABASES['default']['ENGINE'] = 'django.db.backends.sqlite'
    for source, target in (
            ('dbname', 'NAME'),
            ('user', 'USER'),
            ('password', 'PASSWORD'),
            ('host', 'HOST'),
            ('port', 'PORT')
    ):
        settings.DATABASES['default'][target] = getattr(backend, source)
    settings.XDUMP = {
        'FULL_TABLES': ('groups', ),
        'PARTIAL_TABLES': {'employees': EMPLOYEES_SQL}
    }


def test_xdump(db_helper, archive_filename):
    call_command('xdump', archive_filename)
    db_helper.assert_dump(archive_filename)


if IS_POSTGRES:
    from xdump.postgresql import PostgreSQLBackend

    class CustomBackend(PostgreSQLBackend):
        pass
elif IS_SQLITE:
    class CustomBackend(SQLiteBackend):
        pass


def test_custom_backend_via_cli(archive_filename, db_helper, capsys):
    call_command(
        'xdump', archive_filename, backend=CustomBackend.__module__ + '.' + CustomBackend.__name__, verbosity=2
    )
    db_helper.assert_dump(archive_filename)
    out = capsys.readouterr()[0]
    assert "Parameters: " in out


def test_custom_backend_via_config(settings, db_helper, archive_filename):
    settings.XDUMP['BACKEND'] = CustomBackend.__module__ + '.' + CustomBackend.__name__
    call_command('xdump', archive_filename)
    db_helper.assert_dump(archive_filename)


def test_xload(archive_filename, db_helper):
    call_command('xdump', archive_filename)
    assert db_helper.get_tickets_count() == 5
    call_command('xload', archive_filename)
    assert db_helper.get_tickets_count() == 0


def test_dump_schema(archive_filename, db_helper):
    call_command('xdump', archive_filename, dump_data=False)
    archive = zipfile.ZipFile(archive_filename)
    schema = archive.read('dump/schema.sql')
    db_helper.assert_schema(schema)
    if IS_POSTGRES:
        assert archive.namelist() == ['dump/schema.sql', 'dump/sequences.sql']
    else:
        assert archive.namelist() == ['dump/schema.sql']


def test_dump_data(archive_filename):
    call_command('xdump', archive_filename, dump_schema=False)
    archive = zipfile.ZipFile(archive_filename)
    assert archive.namelist() == ['dump/data/groups.csv', 'dump/data/employees.csv']


def test_skip_recreate(backend, execute_file, archive_filename, db_helper):
    call_command('xdump', archive_filename, dump_schema=False)

    backend.recreate_database()
    execute_file('sql/schema.sql', backend.get_cursor())
    if IS_POSTGRES:
        backend.run('COMMIT')

    call_command('xload', archive_filename)
    assert db_helper.get_tickets_count() == 0


def test_truncate_load(backend, archive_filename, db_helper):
    backend.dump(
        archive_filename,
        ['groups'],
        {'employees': 'SELECT * FROM employees WHERE id = 1'},
        dump_schema=False
    )

    backend.run('COMMIT')

    call_command('xload', archive_filename, truncate=True)

    assert db_helper.get_tables_count() == 3
    assert backend.run('SELECT name FROM groups') == [{'name': 'Admin'}, {'name': 'User'}]
    assert backend.run('SELECT id, first_name, last_name FROM employees') == [
        {'id': 1, 'first_name': 'John', 'last_name': 'Doe'}
    ]
