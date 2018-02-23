# coding: utf-8
from unittest.mock import patch

import psycopg2
import pytest


pytestmark = [pytest.mark.postgres, pytest.mark.usefixtures('schema')]


def test_write_sequences(backend, archive, db_helper):
    backend.write_sequences(archive)
    db_helper.assert_unused_sequences(archive)


def test_handling_error(backend):
    with patch('psycopg2.extras.DictCursorBase.fetchall', side_effect=psycopg2.ProgrammingError), \
            pytest.raises(psycopg2.ProgrammingError):
        backend.run('BEGIN')


@pytest.mark.parametrize('sql, expected', (
    ('INSERT INTO groups (name) VALUES (\'test\')', 1),
    ('INSERT INTO groups (name) VALUES (\'test\'), (\'test2\')', 2),
))
def test_dump_sequences(backend, cursor, sql, expected):
    cursor.execute(sql)
    assert "SELECT pg_catalog.setval('groups_id_seq', {0}, true);".format(expected).encode() in backend.dump_sequences()


def test_get_sequences(backend):
    assert backend.get_sequences() == ['groups_id_seq', 'employees_id_seq', 'tickets_id_seq']


def test_run_dump(backend, db_helper):
    schema = backend.run_dump()
    db_helper.assert_schema(schema)
    for table in ('groups', 'employees', 'tickets'):
        assert 'COPY {0}'.format(table).encode() in schema


def test_run_dump_environment(backend):
    backend.password = 'PASSW'
    assert backend.run_dump_environment['PGPASSWORD'] == backend.password


def test_run_dump_environment_empty_password(backend):
    assert 'PGPASSWORD' not in backend.run_dump_environment
