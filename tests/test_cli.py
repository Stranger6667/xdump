import zipfile

import pytest

from xdump import __version__
from xdump.cli import postgres, xdump


@pytest.fixture
def xdump_pg(isolated_cli_runner, dsn_parameters, archive_filename):

    def runner(*args):
        return isolated_cli_runner.invoke(
            postgres,
            (
                '-U', dsn_parameters['user'],
                '-H', dsn_parameters['host'],
                '-P', dsn_parameters['port'],
                '-D', dsn_parameters['dbname'],
                '-o', archive_filename,
            ) + args,
            catch_exceptions=False
        )

    return runner


def test_xdump_run(isolated_cli_runner):
    """Smoke test for a click group."""
    result = isolated_cli_runner.invoke(xdump, ('--version', ))
    assert not result.exception
    assert result.output == 'xdump, version {0}\n'.format(__version__)


@pytest.mark.postgres
@pytest.mark.usefixtures('schema', 'data')
def test_xdump_postgres(xdump_pg, archive_filename, db_helper):
    result = xdump_pg('-f', 'groups')
    assert not result.exception
    assert result.output == 'Dumping ...\nOutput file: {0}\nDone!\n'.format(archive_filename)
    archive = zipfile.ZipFile(archive_filename)
    db_helper.assert_groups(archive)


@pytest.mark.postgres
@pytest.mark.usefixtures('schema', 'data')
def test_xdump_postgres_multiple_full_tables(xdump_pg, archive_filename, db_helper):
    result = xdump_pg('-f', 'groups', '-f' 'tickets')
    assert not result.exception
    archive = zipfile.ZipFile(archive_filename)
    db_helper.assert_groups(archive)
    db_helper.assert_content(
        archive,
        'tickets',
        {
            b'id,author_id,subject,message',
            b'1,1,Sub 1,Message 1',
            b'2,2,Sub 2,Message 2',
            b'3,2,Sub 3,Message 3',
            b'4,2,Sub 4,Message 4',
            b'5,3,Sub 5,Message 5',
        }
    )


@pytest.mark.postgres
@pytest.mark.usefixtures('schema', 'data')
def test_xdump_postgres_partial_tables(xdump_pg, archive_filename, db_helper):
    result = xdump_pg('-p', 'employees:SELECT * FROM employees WHERE id = 1')
    assert not result.exception
    archive = zipfile.ZipFile(archive_filename)
    db_helper.assert_content(archive, 'groups', {b'id,name', b'1,Admin'})
    db_helper.assert_content(
        archive,
        'employees',
        {
            b'id,first_name,last_name,manager_id,referrer_id,group_id',
            b'1,John,Doe,,,1',
        }
    )


@pytest.mark.postgres
@pytest.mark.usefixtures('schema', 'data')
def test_xdump_postgres_partial_tables_invalid(xdump_pg):
    result = xdump_pg('-p', 'shit')
    assert result.exception
    assert result.output == 'Usage: postgres [OPTIONS]\n\nError: ' \
                            'Invalid value: partial table specification should be in the ' \
                            'following format: "table:select SQL"\n'
