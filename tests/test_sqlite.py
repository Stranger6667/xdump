import sqlite3

import pytest

from xdump.sqlite import SQLiteBackend

pytestmark = [pytest.mark.sqlite]


@pytest.mark.skipif(
    sqlite3.sqlite_version_info >= (3, 8, 3),
    reason="Not relevant for newer SQLite version",
)
def test_old_version_info():
    with pytest.raises(RuntimeError):
        SQLiteBackend(dbname="tests")
