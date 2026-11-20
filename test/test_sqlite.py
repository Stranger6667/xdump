from unittest.mock import patch

import pytest

from xdump.sqlite import SQLiteBackend

pytestmark = [pytest.mark.sqlite]


def test_old_version_info():
    with patch("sqlite3.sqlite_version_info", (3, 8, 2)), pytest.raises(RuntimeError):
        SQLiteBackend(dbname="tests")
