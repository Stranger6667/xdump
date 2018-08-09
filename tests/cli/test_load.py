import pytest

from ..conftest import EMPLOYEES_SQL, IS_POSTGRES


@pytest.fixture
def dump(backend, archive_filename, data):
    backend.dump(archive_filename, ['groups'], {'employees': EMPLOYEES_SQL})


@pytest.mark.usefixtures('schema', 'data', 'dump')
def test_load(backend, cli, archive_filename, db_helper):
    backend.recreate_database()
    if IS_POSTGRES:
        backend.run('COMMIT')
    assert db_helper.get_tables_count() == 0
    result = cli.load('-i', archive_filename)
    assert not result.exception
    assert db_helper.get_tables_count() == 3
