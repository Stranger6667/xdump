import pytest

from xdump.cli import dump, load

from ..conftest import DATABASE, IS_POSTGRES, IS_SQLITE


@pytest.fixture
def cli(request, archive_filename, cli_runner):
    commands = {
        "sqlite": {
            "dump": dump.sqlite,
            "load": load.sqlite,
        },
        "postgres": {
            "dump": dump.postgres,
            "load": load.postgres,
        },
    }[DATABASE]

    class CLI:
        def call(self, command, *args):
            default_args = ()
            if IS_SQLITE:
                dbname = request.getfixturevalue("dbname")
                default_args = (
                    "-D",
                    dbname,
                )
            elif IS_POSTGRES:
                dsn_parameters = request.getfixturevalue("dsn_parameters")
                default_args = (
                    "-U",
                    dsn_parameters["user"],
                    "-H",
                    dsn_parameters["host"],
                    "-P",
                    dsn_parameters["port"],
                    "-D",
                    dsn_parameters["dbname"],
                )
            return cli_runner.invoke(command, default_args + args, catch_exceptions=False)

        def dump(self, *args):
            return self.call(commands["dump"], "-o", archive_filename, *args)

        def load(self, *args):
            return self.call(commands["load"], "-i", archive_filename, *args)

    return CLI()
