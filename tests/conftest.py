from pathlib import Path

import pytest
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from concopy.dump import Dumper


CURRENT_DIR = Path(__file__).parent.absolute()


@pytest.fixture
def cursor(postgresql):
    postgresql.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return postgresql.cursor()


@pytest.fixture
def setup_database(cursor):
    with (CURRENT_DIR / 'schema.sql').open('r') as fd:
        sql = fd.read()
    cursor.execute(sql)


@pytest.fixture
def dumper(postgresql):
    parameters = postgresql.get_dsn_parameters()
    return Dumper(
        dbname=parameters['dbname'],
        user=parameters['user'],
        host=parameters['host'],
        port=parameters['port']
    )
