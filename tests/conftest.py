import zipfile
from pathlib import Path

import pytest
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


CURRENT_DIR = Path(__file__).parent.absolute()


@pytest.fixture
def cursor(postgresql):
    postgresql.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return postgresql.cursor()


def execute_file(cursor, filename):
    with (CURRENT_DIR / filename).open('r') as fd:
        sql = fd.read()
    cursor.execute(sql)


@pytest.fixture
def schema(cursor):
    execute_file(cursor, 'schema.sql')


@pytest.fixture
def data(cursor):
    execute_file(cursor, 'data.sql')


@pytest.fixture
def archive_filename(tmpdir):
    return str(tmpdir.join('dump.zip'))


@pytest.fixture
def archive(archive_filename):
    with zipfile.ZipFile(archive_filename, 'w', zipfile.ZIP_DEFLATED) as file:
        yield file
