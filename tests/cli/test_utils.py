import pytest

from xdump.cli.utils import apply_decorators, import_backend
from xdump.postgresql import PostgreSQLBackend
from xdump.sqlite import SQLiteBackend


@pytest.mark.parametrize('path, expected', (
    ('xdump.postgresql.PostgreSQLBackend', PostgreSQLBackend),
    ('xdump.sqlite.SQLiteBackend', SQLiteBackend),
))
def test_import_backend(path, expected):
    assert import_backend(path) is expected


def test_apply_decorators():

    def dec1(func):
        func.foo = 1
        return func

    def dec2(func):
        func.bar = 2
        return func

    @apply_decorators([dec1, dec2])
    def func():
        pass

    assert func.foo == 1
    assert func.bar == 2
