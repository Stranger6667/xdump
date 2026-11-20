from typing import Any

from django.core.management.base import CommandParser

from xdump.base import BaseBackend

from ..core import XDumpCommand


class Command(XDumpCommand):
    help = "Creates an SQL dump with latest data."

    def add_arguments(self, parser: CommandParser) -> None:
        super().add_arguments(parser)
        parser.add_argument(
            "-d",
            "--dump-data",
            action="store",
            dest="dump_data",
            help="Control if the data should be dumped.",
            required=False,
            default=True,
        )
        parser.add_argument(
            "-s",
            "--dump-schema",
            action="store",
            dest="dump_schema",
            help="Control if the schema should be dumped.",
            required=False,
            default=True,
        )

    def _handle(self, filename: str, backend: BaseBackend, **options: Any) -> None:
        backend.dump(
            filename, dump_data=options["dump_data"], dump_schema=options["dump_schema"], **self.get_dump_kwargs()
        )
