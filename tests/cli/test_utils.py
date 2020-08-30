from xdump.base import BaseBackend
from xdump.cli.utils import apply_decorators, import_backend


def test_import_backend():
    backend_class = import_backend("xdump.sqlite.SQLiteBackend")
    assert issubclass(backend_class, BaseBackend)


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
