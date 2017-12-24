# coding: utf-8
import zipfile

import pytest

from .conftest import assert_schema


pytestmark = pytest.mark.usefixtures('schema')


def test_dump(db_wrapper, archive_filename):
    db_wrapper.dump(archive_filename, ['groups'])
    archive = zipfile.ZipFile(archive_filename)
    assert archive.namelist() == ['dump/schema.sql', 'dump/sequences.sql', 'dump/data/groups.csv']
    assert archive.read('dump/data/groups.csv') == b'id,name\n'
    assert "SELECT pg_catalog.setval('groups_id_seq', 1, false);".encode() in archive.read('dump/sequences.sql')
    schema = archive.read('dump/schema.sql')
    assert_schema(schema, db_wrapper.get_backend())


@pytest.mark.usefixtures('schema', 'data')
def test_populate_database(db_wrapper, archive_filename):
    db_wrapper.dump(archive_filename, ['groups'])
    backend = db_wrapper.get_backend()
    backend.recreate_database()
    db_wrapper.populate_database(archive_filename)
    result = backend.run(
        "SELECT COUNT(*) FROM pg_tables WHERE tablename IN ('groups', 'employees', 'tickets')"
    )
    assert result[0]['count'] == 3
    result = backend.run("SELECT last_value FROM pg_sequences WHERE sequencename = 'groups_id_seq'")
    assert result[0]['last_value'] == 2
    assert backend.run('SELECT name FROM groups') == [{'name': 'Admin'}, {'name': 'User'}]
