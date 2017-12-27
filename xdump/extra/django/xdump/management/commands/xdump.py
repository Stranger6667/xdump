# coding: utf-8
from ..core import XDumpCommand


class Command(XDumpCommand):
    help = 'Creates an SQL dump with latest data.'

    def handle(self, filename, **options):
        backend = self.get_xdump_backend(options['alias'], options['backend'])
        backend.dump(filename, **self.get_dump_kwargs())
