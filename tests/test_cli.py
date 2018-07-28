import pytest
import yaml

from tests.conftest import EMPLOYEES_SQL
from xdump.cli import xdump


pytestmark = [pytest.mark.postgres]


@pytest.mark.usefixtures('schema', 'data')
def test_dump(request, isolated_cli_runner, tmpdir, archive_filename, db_helper):
    postgresql = request.getfixturevalue('postgresql')
    parameters = postgresql.get_dsn_parameters()
    config_file = tmpdir.join('test.yml')
    config_file.write(yaml.safe_dump({
        'dump': {
            'backend': 'xdump.postgresql.PostgreSQLBackend',
            'dbname': parameters['dbname'],
            'user': parameters['user'],
            'password': None,
            'host': parameters['host'],
            'port': parameters['port'],
            'output_file': archive_filename,
            'compression': '',
            'full_tables': ['groups'],
            'partial_tables': {
                'employees': EMPLOYEES_SQL
            },
        }
    }))
    result = isolated_cli_runner.invoke(
        xdump,
        (
            '-c', config_file.strpath,
        ),
        catch_exceptions=False
    )
    assert not result.exception
    assert result.output == ''
    db_helper.assert_dump(archive_filename)
