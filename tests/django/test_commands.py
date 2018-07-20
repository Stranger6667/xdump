# coding: utf-8
import pytest
from django.core.management import call_command

from xdump.postgresql import PostgreSQLBackend
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
    assert "Parameters: {'table_name': 'tickets', 'full_tables': ['groups']}" in out


def test_custom_backend_via_config(settings, db_helper, archive_filename):
    settings.XDUMP['BACKEND'] = CustomBackend.__module__ + '.' + CustomBackend.__name__
    call_command('xdump', archive_filename)
    db_helper.assert_dump(archive_filename)


def test_xload(archive_filename, db_helper):
    call_command('xdump', archive_filename)
    assert db_helper.get_tickets_count() == 5
    call_command('xload', archive_filename)
    assert db_helper.get_tickets_count() == 0
