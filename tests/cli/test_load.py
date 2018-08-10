import pytest

from ..conftest import EMPLOYEES_SQL, IS_POSTGRES


@pytest.mark.usefixtures('schema', 'data')
def test_load(backend, cli, archive_filename, db_helper):
    backend.dump(archive_filename, ['groups'], {'employees': EMPLOYEES_SQL})
    backend.recreate_database()
    if IS_POSTGRES:
        backend.run('COMMIT')
    assert db_helper.get_tables_count() == 0
    result = cli.load('-i', archive_filename)
    assert not result.exception
    assert db_helper.get_tables_count() == 3


@pytest.mark.parametrize('cleanup_method, dump_kwargs', (
    ('truncate', {'dump_schema': False}),
    ('recreate', {}),
))
@pytest.mark.usefixtures('schema', 'data')
def test_cleanup_methods(cli, archive_filename, backend, cleanup_method, dump_kwargs):
    backend.dump(archive_filename, ['groups'], {'employees': EMPLOYEES_SQL}, **dump_kwargs)
    if IS_POSTGRES:
        backend.run('COMMIT')
    result = cli.load('-i', archive_filename, '-m', cleanup_method)
    assert not result.exception
    assert backend.run('SELECT name FROM groups') == [{'name': 'Admin'}, {'name': 'User'}]
    assert backend.run('SELECT id, first_name, last_name FROM employees') == [
        {'id': 5, 'last_name': 'Snow', 'first_name': 'John'},
        {'id': 4, 'first_name': 'John', 'last_name': 'Brown'},
        {'id': 3, 'first_name': 'John', 'last_name': 'Smith'},
        {'id': 1, 'first_name': 'John', 'last_name': 'Doe'},
    ]
