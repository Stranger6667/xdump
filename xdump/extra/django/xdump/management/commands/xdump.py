# coding: utf-8
from ..core import XDumpCommand


class Command(XDumpCommand):
    help = 'Creates an SQL dump with latest data.'

    def _handle(self, filename, backend):
        backend.dump(filename, **self.get_dump_kwargs())
