# coding: utf-8
import zipfile

import pytest
from django.core.management import call_command

from ..conftest import EMPLOYEES_SQL, assert_schema


pytestmark = pytest.mark.usefixtures('schema', 'data')


@pytest.fixture(autouse=True)
def setup(settings, postgresql):
    parameters = postgresql.get_dsn_parameters()
    for source, target in (
            ('dbname', 'NAME'),
            ('user', 'USER'),
            ('password', 'PASSWORD'),
            ('host', 'HOST'),
            ('port', 'PORT')
    ):
        settings.DATABASES['default'][target] = parameters.get(source)
    settings.XDUMP = {
        'FULL_TABLES': ('groups', ),
        'PARTIAL_TABLES': {'employees': EMPLOYEES_SQL}
    }


def test_xdump(archive_filename):
    call_command('xdump', archive_filename)
    archive = zipfile.ZipFile(archive_filename)
    assert archive.namelist() == [
        'dump/schema.sql', 'dump/sequences.sql', 'dump/data/groups.csv', 'dump/data/employees.csv',
    ]
    assert archive.read('dump/data/groups.csv') == b'id,name\n1,Admin\n2,User\n'
    assert archive.read('dump/data/employees.csv') == b'id,first_name,last_name,manager_id,group_id\n' \
                                                      b'5,John,Snow,3,2\n' \
                                                      b'4,John,Brown,3,2\n' \
                                                      b'3,John,Smith,1,1\n' \
                                                      b'1,John,Doe,,1\n'
    schema = archive.read('dump/schema.sql')
    assert_schema(schema)
