import zipfile

import pytest

from xdump.cli import postgres


@pytest.mark.postgres
@pytest.mark.usefixtures('schema', 'data')
def test_xdump_postgres(request, isolated_cli_runner, archive_filename, db_helper):
    postgresql = request.getfixturevalue('postgresql')
    parameters = postgresql.get_dsn_parameters()

    result = isolated_cli_runner.invoke(
        postgres,
        (
            '-U', parameters['user'],
            '-H', parameters['host'],
            '-P', parameters['port'],
            '-D', parameters['dbname'],
            '-o', archive_filename,
            '-f', 'groups'
        ),
        catch_exceptions=False
    )
    assert not result.exception
    assert result.output == 'Dumping ...\nOutput file: {0}\nDone!\n'.format(archive_filename)
    archive = zipfile.ZipFile(archive_filename)
    db_helper.assert_groups(archive)
