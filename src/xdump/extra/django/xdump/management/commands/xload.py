from typing import Any

from django.core.management.base import CommandParser

from xdump.base import BaseBackend

from ..core import XDumpCommand


class Command(XDumpCommand):
    help = "Loads an SQL dump."

    def add_arguments(self, parser: CommandParser) -> None:
        super().add_arguments(parser)
        parser.add_argument(
            "-m",
            "--cleanup-method",
            action="store",
            nargs="?",
            choices=["recreate", "truncate"],
            dest="cleanup_method",
            help="Method of DB cleaning up",
            required=False,
        )

    def _handle(self, filename: str, backend: BaseBackend, **options: Any) -> None:
        if options["cleanup_method"] == "truncate":
            backend.truncate()
        elif options["cleanup_method"] == "recreate":
            backend.recreate_database()
        backend.load(filename)
