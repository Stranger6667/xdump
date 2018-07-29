# coding: utf-8
from ..core import XDumpCommand


class Command(XDumpCommand):
    help = 'Loads an SQL dump.'

    def _handle(self, filename, backend, **options):
        if not backend.is_dump_without_schema(filename):
            print('XXX')
            backend.recreate_database()
        backend.load(filename)
