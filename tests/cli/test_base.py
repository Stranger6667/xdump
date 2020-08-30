import pytest

from xdump import __version__
from xdump.cli import dump, load


@pytest.mark.parametrize("command, name", ((dump.dump, "xdump"), (load.load, "xload")))
def test_xdump_run(isolated_cli_runner, command, name):
    """Smoke test for a click group."""
    result = isolated_cli_runner.invoke(command, ("--version",))
    assert not result.exception
    assert result.output == "{0}, version {1}\n".format(name, __version__)
