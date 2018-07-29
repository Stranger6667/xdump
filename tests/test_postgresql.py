# coding: utf-8
from unittest.mock import Mock, patch

import pytest

from .conftest import is_search_path_fixed


pytestmark = [pytest.mark.postgres]


@pytest.mark.usefixtures('schema')
def test_write_sequences(backend, archive, db_helper):
    backend.write_sequences(archive)
    db_helper.assert_unused_sequences(archive)


def test_handling_error(backend):
    import psycopg2

    with patch('psycopg2.extras.DictCursorBase.fetchall', side_effect=psycopg2.ProgrammingError), \
            pytest.raises(psycopg2.ProgrammingError):
        backend.run('BEGIN')


@pytest.mark.parametrize('sql, expected', (
    ('INSERT INTO groups (name) VALUES (\'test\')', 1),
    ('INSERT INTO groups (name) VALUES (\'test\'), (\'test2\')', 2),
))
@pytest.mark.usefixtures('schema')
def test_dump_sequences(backend, db_helper, cursor, sql, expected):
    cursor.execute(sql)
    if db_helper.is_search_path_fixed:
        template = "SELECT pg_catalog.setval('public.groups_id_seq', {0}, true);"
    else:
        template = "SELECT pg_catalog.setval('groups_id_seq', {0}, true);"
    assert template.format(expected).encode() in backend.dump_sequences()


@pytest.mark.usefixtures('schema')
def test_get_sequences(backend):
    assert backend.get_sequences() == ['groups_id_seq', 'employees_id_seq', 'tickets_id_seq']


@pytest.mark.usefixtures('schema')
def test_run_dump(backend, db_helper):
    schema = backend.run_dump()
    db_helper.assert_schema(schema)
    if db_helper.is_search_path_fixed:
        template = 'COPY public.{0}'
    else:
        template = 'COPY {0}'
    for table in ('groups', 'employees', 'tickets'):
        assert template.format(table).encode() in schema


def test_run_dump_environment(backend):
    backend.password = 'PASSW'
    assert backend.run_dump_environment['PGPASSWORD'] == backend.password


def test_run_dump_environment_empty_password(backend):
    assert 'PGPASSWORD' not in backend.run_dump_environment


@pytest.mark.parametrize('version, is_fixed', (
    (100004, True),
    (100003, True),
    (100002, False),
    (90609, True),
    (90608, True),
    (90607, False),
))
def test_postgres_version(version, is_fixed):
    mocked_connection = Mock(server_version=version)
    assert is_search_path_fixed(mocked_connection) == is_fixed
