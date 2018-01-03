# coding: utf-8
from ..core import XDumpCommand


class Command(XDumpCommand):
    help = 'Loads an SQL dump.'

    def handle(self, filename, **options):
        backend = self.get_xdump_backend(options['alias'], options['backend'])
        backend.recreate_database()
        backend.load(filename)
