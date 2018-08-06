import zipfile

import pytest

from xdump.cli import postgres


@pytest.fixture
def xdump_pg(isolated_cli_runner, dsn_parameters, archive_filename):

    def runner(*args):
        result = isolated_cli_runner.invoke(
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
        assert not result.exception
        return result

    return runner


@pytest.mark.postgres
@pytest.mark.usefixtures('schema', 'data')
def test_xdump_postgres(xdump_pg, archive_filename, db_helper):
    result = xdump_pg('-f', 'groups')
    assert result.output == 'Dumping ...\nOutput file: {0}\nDone!\n'.format(archive_filename)
    archive = zipfile.ZipFile(archive_filename)
    db_helper.assert_groups(archive)


@pytest.mark.postgres
@pytest.mark.usefixtures('schema', 'data')
def test_xdump_postgres_multiple_full_tables(xdump_pg, archive_filename, db_helper):
    xdump_pg('-f', 'groups', '-f' 'tickets')
    archive = zipfile.ZipFile(archive_filename)
    db_helper.assert_groups(archive)
    assert db_helper.get_tickets_count() == 5
