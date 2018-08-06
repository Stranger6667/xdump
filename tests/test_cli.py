import pytest

from xdump.cli import postgres
from ._compat import patch


@pytest.mark.postgres
@pytest.mark.usefixtures('schema', 'data')
def test_pg_connect(isolated_cli_runner):
    from xdump.postgresql import PostgreSQLBackend

    with patch('xdump.postgresql.PostgreSQLBackend', wraps=PostgreSQLBackend) as backend:
        result = isolated_cli_runner.invoke(
            postgres,
            (
                '-U', 'test',
                '-W', 'passw',
                '-h', '127.0.0.1',
                '-p', '5432',
                '-d', 'tests'
            ),
            catch_exceptions=False
        )
        backend.assert_called_with(dbname='tests', host='127.0.0.1', port='5432', user='test', password='passw')
    assert not result.exception
    assert result.output == ''
